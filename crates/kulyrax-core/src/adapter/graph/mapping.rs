use super::adapter::GraphError;
use crate::adapter::{OnExisting, Operation, is_identity_scalar, json_scalar_to_string};
use crate::event::Event;
use crate::runtime::config::AdapterCapability;
use serde::Deserialize;
use serde_json::{Map, Value};

/// A graph mapping (Phase 16) — the fourth storage kind. A tagged enum on
/// `kind`: a mapping emits **exactly one** record, a node or an edge, keyed
/// and sequence-gated identically to every prior adapter. `operation` /
/// `on_existing` / `permanent` / `version` / `facet` keep their Phase 11–15
/// meanings; the only graph-specific vocabulary is `kind`, `label` /
/// `edge_type`, and `from` / `to` / `discriminator`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum GraphMapping {
    Node(GraphNodeMapping),
    Edge(GraphEdgeMapping),
}

/// `kind: node` — a node *is* an entity, so facets (Phase 15) apply verbatim.
#[derive(Debug, Clone, Deserialize)]
pub struct GraphNodeMapping {
    pub event: String,
    pub label: String,
    pub version: u32,
    /// Path expression resolving this event's node id (must be a JSON scalar).
    pub key: String,
    /// JSON template, leaves are payload/metadata paths. Empty for `operation: delete`.
    #[serde(default)]
    pub properties: Value,
    #[serde(default)]
    pub on_existing: OnExisting,
    #[serde(default)]
    pub operation: Operation,
    #[serde(default)]
    pub permanent: bool,
    /// Phase 15.1: the named facet this mapping owns (nodes only).
    #[serde(default)]
    pub facet: Option<String>,
    #[serde(default)]
    pub requires_capabilities: Vec<AdapterCapability>,
}

/// `kind: edge` — identity is the canonical `[edge_type, from, to]`, plus an
/// optional `discriminator` for parallel edges (a multigraph). Edges are not
/// faceted.
#[derive(Debug, Clone, Deserialize)]
pub struct GraphEdgeMapping {
    pub event: String,
    pub edge_type: String,
    pub version: u32,
    /// Path expression resolving the `from` endpoint node id (scalar).
    pub from: String,
    /// Path expression resolving the `to` endpoint node id (scalar).
    pub to: String,
    /// Optional 4th key component for parallel edges between one ordered pair.
    #[serde(default)]
    pub discriminator: Option<String>,
    #[serde(default)]
    pub properties: Value,
    #[serde(default)]
    pub on_existing: OnExisting,
    #[serde(default)]
    pub operation: Operation,
    #[serde(default)]
    pub permanent: bool,
    #[serde(default)]
    pub requires_capabilities: Vec<AdapterCapability>,
}

/// A resolved graph record — the single node or edge a `GraphMapping` emits
/// for one event, plus its projected properties.
#[derive(Debug, Clone)]
pub enum ResolvedRecord {
    Node {
        label: String,
        node_id: String,
        properties: Value,
    },
    Edge {
        edge_type: String,
        from: String,
        to: String,
        discriminator: Option<String>,
        properties: Value,
    },
}

impl GraphMapping {
    pub fn event(&self) -> &str {
        match self {
            GraphMapping::Node(m) => &m.event,
            GraphMapping::Edge(m) => &m.event,
        }
    }

    pub fn version(&self) -> u32 {
        match self {
            GraphMapping::Node(m) => m.version,
            GraphMapping::Edge(m) => m.version,
        }
    }

    pub fn operation(&self) -> Operation {
        match self {
            GraphMapping::Node(m) => m.operation,
            GraphMapping::Edge(m) => m.operation,
        }
    }

    pub fn on_existing(&self) -> OnExisting {
        match self {
            GraphMapping::Node(m) => m.on_existing,
            GraphMapping::Edge(m) => m.on_existing,
        }
    }

    pub fn permanent(&self) -> bool {
        match self {
            GraphMapping::Node(m) => m.permanent,
            GraphMapping::Edge(m) => m.permanent,
        }
    }

    pub fn requires_capabilities(&self) -> &[AdapterCapability] {
        match self {
            GraphMapping::Node(m) => &m.requires_capabilities,
            GraphMapping::Edge(m) => &m.requires_capabilities,
        }
    }

    /// Normalized facet key — `""` for the default facet and for every edge
    /// (edges are not faceted).
    pub fn facet_key(&self) -> &str {
        match self {
            GraphMapping::Node(m) => m.facet.as_deref().unwrap_or(""),
            GraphMapping::Edge(_) => "",
        }
    }

    /// The projected-properties template (empty for a `delete` mapping).
    pub fn properties(&self) -> &Value {
        match self {
            GraphMapping::Node(m) => &m.properties,
            GraphMapping::Edge(m) => &m.properties,
        }
    }

    /// Resolve the single record this mapping emits for `event`.
    pub fn resolve(&self, event: &Event) -> Result<ResolvedRecord, GraphError> {
        let payload: Value = serde_json::from_str(&event.payload)
            .map_err(|e| GraphError::BuildFailed(format!("invalid payload JSON: {}", e)))?;
        let payload_obj = payload
            .as_object()
            .ok_or_else(|| GraphError::BuildFailed("payload must be a JSON object".into()))?;

        match self {
            GraphMapping::Node(m) => {
                let properties = expand_value(&m.properties, payload_obj, &event.metadata)?;
                let node_id = scalar_key("key", &m.key, payload_obj, &event.metadata)?;
                Ok(ResolvedRecord::Node {
                    label: m.label.clone(),
                    node_id,
                    properties,
                })
            }
            GraphMapping::Edge(m) => {
                let properties = expand_value(&m.properties, payload_obj, &event.metadata)?;
                let from = scalar_key("from", &m.from, payload_obj, &event.metadata)?;
                let to = scalar_key("to", &m.to, payload_obj, &event.metadata)?;
                let discriminator = match &m.discriminator {
                    Some(path) => Some(scalar_key(
                        "discriminator",
                        path,
                        payload_obj,
                        &event.metadata,
                    )?),
                    None => None,
                };
                Ok(ResolvedRecord::Edge {
                    edge_type: m.edge_type.clone(),
                    from,
                    to,
                    discriminator,
                    properties,
                })
            }
        }
    }
}

fn scalar_key(
    field: &str,
    path: &str,
    payload: &Map<String, Value>,
    metadata: &std::collections::HashMap<String, String>,
) -> Result<String, GraphError> {
    let value = resolve_path(path, payload, metadata)?;
    if !is_identity_scalar(&value) {
        return Err(GraphError::BuildFailed(format!(
            "{field} path '{path}' resolved to a non-scalar value; \
             a graph key must be a string, number, or bool"
        )));
    }
    Ok(json_scalar_to_string(&value))
}

fn expand_value(
    template: &Value,
    payload: &Map<String, Value>,
    metadata: &std::collections::HashMap<String, String>,
) -> Result<Value, GraphError> {
    match template {
        Value::Object(map) => {
            let mut out = Map::new();
            for (k, v) in map {
                out.insert(k.clone(), expand_value(v, payload, metadata)?);
            }
            Ok(Value::Object(out))
        }
        Value::Array(arr) => {
            let mut out = Vec::with_capacity(arr.len());
            for v in arr {
                out.push(expand_value(v, payload, metadata)?);
            }
            Ok(Value::Array(out))
        }
        Value::String(path) => resolve_path(path, payload, metadata),
        other => Ok(other.clone()),
    }
}

fn resolve_path(
    path: &str,
    payload: &Map<String, Value>,
    metadata: &std::collections::HashMap<String, String>,
) -> Result<Value, GraphError> {
    if let Some(rest) = path.strip_prefix("payload.") {
        payload
            .get(rest)
            .cloned()
            .ok_or_else(|| GraphError::BuildFailed(format!("payload field '{}' not found", rest)))
    } else if let Some(rest) = path.strip_prefix("metadata.") {
        metadata
            .get(rest)
            .map(|v| Value::String(v.clone()))
            .ok_or_else(|| GraphError::BuildFailed(format!("metadata field '{}' not found", rest)))
    } else {
        Err(GraphError::BuildFailed(format!(
            "invalid path '{}' - must start with 'payload.' or 'metadata.'",
            path
        )))
    }
}

/// Canonical edge identity (Phase 11.1 composite-key encoding): an ordered
/// JSON array `[edge_type, from, to]`, or `[edge_type, from, to, discriminator]`
/// for a multigraph. One encoding path, so a 3-component key never collides
/// with a differently-split 4-component one.
pub fn canonical_edge_key(
    edge_type: &str,
    from: &str,
    to: &str,
    discriminator: Option<&str>,
) -> String {
    let parts: Vec<&str> = match discriminator {
        Some(d) => vec![edge_type, from, to, d],
        None => vec![edge_type, from, to],
    };
    serde_json::to_string(&parts).unwrap_or_else(|_| format!("{edge_type}/{from}/{to}"))
}

/// Filesystem-safe encoding of a canonical edge key — lowercase hex of its
/// UTF-8 bytes. Deterministic and collision-free (the canonical key is
/// already collision-free); not meant to be human-read.
pub fn encode_edge_key(canonical: &str) -> String {
    let mut s = String::with_capacity(canonical.len() * 2);
    for b in canonical.as_bytes() {
        s.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
        s.push(char::from_digit((b & 0xf) as u32, 16).unwrap_or('0'));
    }
    s
}
