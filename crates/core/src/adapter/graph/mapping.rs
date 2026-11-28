use super::adapter::GraphError;
use crate::adapter::{OnExisting, Operation, is_identity_scalar, json_scalar_to_string};
use crate::event::Event;
use crate::runtime::config::AdapterCapability;
use serde::Deserialize;
use serde_json::{Map, Value};

/// A graph mapping (Phase 16) — the fourth storage kind. A tagged enum on
/// `kind`: a node or an edge mapping emits **exactly one** record, keyed and
/// sequence-gated identically to every prior adapter; a subgraph mapping
/// emits several, applied together. `operation` / `on_existing` /
/// `permanent` / `version` / `facet` keep their Phase 11–15 meanings; the
/// only graph-specific vocabulary is `kind`, `label` / `edge_type`, `from` /
/// `to` / `discriminator`, and `records`.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
#[non_exhaustive]
pub enum GraphMapping {
    /// `kind: node`: one node per event.
    Node(GraphNodeMapping),
    /// `kind: edge`: one edge per event.
    Edge(GraphEdgeMapping),
    /// `kind: subgraph`: several nodes and edges per event, applied together.
    Subgraph(GraphSubgraphMapping),
}

/// `kind: subgraph` — several records from one event: a node and its edges,
/// say. Each entry of `records` is a `kind: node` or `kind: edge` mapping
/// without `event` and `version`, which it takes from the subgraph. Every
/// record keeps its own guard lane and is decided on its own, at the event's
/// sequence; the adapter applies all of them in one transaction, so a record
/// that fails leaves nothing of the event behind.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct GraphSubgraphMapping {
    /// The event type this mapping projects.
    pub event: String,
    /// The payload version this mapping is written for; older events are upcast
    /// to it.
    pub version: u32,
    /// Node and edge mappings, applied in this order.
    pub records: Vec<GraphMapping>,
}

impl<'de> Deserialize<'de> for GraphSubgraphMapping {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Raw {
            event: String,
            version: u32,
            records: Vec<Value>,
        }
        let raw = Raw::deserialize(d)?;
        let mut records = Vec::with_capacity(raw.records.len());
        for (i, mut record) in raw.records.into_iter().enumerate() {
            let fields = record
                .as_object_mut()
                .ok_or_else(|| D::Error::custom(format!("records[{i}] must be a mapping")))?;
            for field in ["event", "version"] {
                if fields.contains_key(field) {
                    return Err(D::Error::custom(format!(
                        "records[{i}]: `{field}` belongs to the subgraph, not to its records"
                    )));
                }
            }
            fields.insert("event".into(), Value::from(raw.event.clone()));
            fields.insert("version".into(), Value::from(raw.version));
            let mapping: GraphMapping = serde_json::from_value(record)
                .map_err(|e| D::Error::custom(format!("records[{i}]: {e}")))?;
            if matches!(mapping, GraphMapping::Subgraph(_)) {
                return Err(D::Error::custom(format!(
                    "records[{i}]: a subgraph's records are nodes and edges, not subgraphs"
                )));
            }
            records.push(mapping);
        }
        Ok(Self {
            event: raw.event,
            version: raw.version,
            records,
        })
    }
}

/// `kind: node` — a node *is* an entity, so facets (Phase 15) apply verbatim.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct GraphNodeMapping {
    /// The event type this mapping projects.
    pub event: String,
    /// The node label.
    pub label: String,
    /// The payload version this mapping is written for; older events are upcast
    /// to it.
    pub version: u32,
    /// Path expression resolving this event's node id (must be a JSON scalar).
    pub key: String,
    /// JSON template, leaves are payload/metadata paths. Empty for `operation: delete`.
    #[serde(default)]
    pub properties: Value,
    /// What an upsert does to a node already projected ([`OnExisting`]; the
    /// default ignores the event).
    #[serde(default)]
    pub on_existing: OnExisting,
    /// Upsert (the default) or delete.
    #[serde(default)]
    pub operation: Operation,
    /// With `operation: delete`: a permanent tombstone, so no later event
    /// projects this node again.
    #[serde(default)]
    pub permanent: bool,
    /// Phase 15.1: the named facet this mapping owns (nodes only).
    #[serde(default)]
    pub facet: Option<String>,
    /// Capabilities the adapter this is routed to must declare.
    #[serde(default)]
    pub requires_capabilities: Vec<AdapterCapability>,
}

/// `kind: edge` — identity is the canonical `[edge_type, from, to]`, plus an
/// optional `discriminator` for parallel edges (a multigraph). Edges are not
/// faceted.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct GraphEdgeMapping {
    /// The event type this mapping projects.
    pub event: String,
    /// The relationship type.
    pub edge_type: String,
    /// The `from` endpoint's node label. Required: nodes are per label, so an
    /// endpoint is a label and an id (decision 2026-11-28-edge-labels).
    pub from_label: String,
    /// The `to` endpoint's node label. Required, as `from_label`.
    pub to_label: String,
    /// The payload version this mapping is written for; older events are upcast
    /// to it.
    pub version: u32,
    /// Path expression resolving the `from` endpoint node id (scalar).
    pub from: String,
    /// Path expression resolving the `to` endpoint node id (scalar).
    pub to: String,
    /// Optional 4th key component for parallel edges between one ordered pair.
    #[serde(default)]
    pub discriminator: Option<String>,
    /// JSON template whose leaves are payload/metadata paths: the edge's
    /// properties. Empty for `operation: delete`.
    #[serde(default)]
    pub properties: Value,
    /// What an upsert does to an edge already projected ([`OnExisting`]; the
    /// default ignores the event).
    #[serde(default)]
    pub on_existing: OnExisting,
    /// Upsert (the default) or delete.
    #[serde(default)]
    pub operation: Operation,
    /// With `operation: delete`: a permanent tombstone, so no later event
    /// projects this edge again.
    #[serde(default)]
    pub permanent: bool,
    /// Capabilities the adapter this is routed to must declare.
    #[serde(default)]
    pub requires_capabilities: Vec<AdapterCapability>,
}

/// A resolved graph record — the single node or edge a `GraphMapping` emits
/// for one event, plus its projected properties.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum ResolvedRecord {
    /// A node.
    Node {
        /// The node label.
        label: String,
        /// The node id the mapping's `key` resolved to.
        node_id: String,
        /// The projected properties.
        properties: Value,
    },
    /// An edge.
    Edge {
        /// The relationship type.
        edge_type: String,
        /// The `from` endpoint's label.
        from_label: String,
        /// The `from` endpoint's node id.
        from: String,
        /// The `to` endpoint's label.
        to_label: String,
        /// The `to` endpoint's node id.
        to: String,
        /// The resolved discriminator, for parallel edges.
        discriminator: Option<String>,
        /// The projected properties.
        properties: Value,
    },
}

/// What the single-record accessors below answer for a subgraph, which has
/// no single operation, mode or template: see [`GraphMapping::records`].
static NO_PROPERTIES: Value = Value::Null;

impl GraphMapping {
    /// The event type this mapping projects.
    pub fn event(&self) -> &str {
        match self {
            GraphMapping::Node(m) => &m.event,
            GraphMapping::Edge(m) => &m.event,
            GraphMapping::Subgraph(m) => &m.event,
        }
    }

    /// The payload version this mapping is written for.
    pub fn version(&self) -> u32 {
        match self {
            GraphMapping::Node(m) => m.version,
            GraphMapping::Edge(m) => m.version,
            GraphMapping::Subgraph(m) => m.version,
        }
    }

    /// The node and edge mappings this mapping applies for one event: itself
    /// for a node or an edge, its `records` for a subgraph. The accessors
    /// below describe one record; ask each of these.
    pub fn records(&self) -> Vec<&GraphMapping> {
        match self {
            GraphMapping::Subgraph(m) => m.records.iter().collect(),
            single => vec![single],
        }
    }

    /// A node or edge mapping's operation; a subgraph's is the default (upsert).
    pub fn operation(&self) -> Operation {
        match self {
            GraphMapping::Node(m) => m.operation,
            GraphMapping::Edge(m) => m.operation,
            GraphMapping::Subgraph(_) => Operation::default(),
        }
    }

    /// A node or edge mapping's `on_existing`; a subgraph's is the default.
    pub fn on_existing(&self) -> OnExisting {
        match self {
            GraphMapping::Node(m) => m.on_existing,
            GraphMapping::Edge(m) => m.on_existing,
            GraphMapping::Subgraph(_) => OnExisting::default(),
        }
    }

    /// Whether a node or edge mapping's delete is permanent; `false` for a subgraph.
    pub fn permanent(&self) -> bool {
        match self {
            GraphMapping::Node(m) => m.permanent,
            GraphMapping::Edge(m) => m.permanent,
            GraphMapping::Subgraph(_) => false,
        }
    }

    /// A node or edge mapping's required capabilities; none for a subgraph.
    pub fn requires_capabilities(&self) -> &[AdapterCapability] {
        match self {
            GraphMapping::Node(m) => &m.requires_capabilities,
            GraphMapping::Edge(m) => &m.requires_capabilities,
            GraphMapping::Subgraph(_) => &[],
        }
    }

    /// Normalized facet key — `""` for the default facet and for every edge
    /// (edges are not faceted).
    pub fn facet_key(&self) -> &str {
        match self {
            GraphMapping::Node(m) => m.facet.as_deref().unwrap_or(""),
            GraphMapping::Edge(_) | GraphMapping::Subgraph(_) => "",
        }
    }

    /// The projected-properties template (empty for a `delete` mapping).
    pub fn properties(&self) -> &Value {
        match self {
            GraphMapping::Node(m) => &m.properties,
            GraphMapping::Edge(m) => &m.properties,
            GraphMapping::Subgraph(_) => &NO_PROPERTIES,
        }
    }

    /// Resolve the single record a node or edge mapping emits for `event`.
    /// A subgraph has several: resolve each of its [`Self::records`].
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
                // `properties` is optional for edges: absent, the edge has
                // none (Neo4j refused a null `SET r = $properties`).
                let properties = if m.properties.is_null() {
                    Value::Object(Map::new())
                } else {
                    expand_value(&m.properties, payload_obj, &event.metadata)?
                };
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
                    from_label: m.from_label.clone(),
                    from,
                    to_label: m.to_label.clone(),
                    to,
                    discriminator,
                    properties,
                })
            }
            GraphMapping::Subgraph(_) => Err(GraphError::BuildFailed(
                "a subgraph mapping resolves one record per entry of `records`".into(),
            )),
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
/// JSON array `[edge_type, from_label, from, to_label, to]`, plus the
/// discriminator for a multigraph. The endpoints' labels are part of it
/// (graph format 2; decision 2026-11-28-edge-labels): `User u1 → u2` and
/// `Org u1 → u2` are two edges. One encoding path, so a key never collides
/// with a differently-split one.
pub fn canonical_edge_key(
    edge_type: &str,
    from_label: &str,
    from: &str,
    to_label: &str,
    to: &str,
    discriminator: Option<&str>,
) -> String {
    let mut parts: Vec<&str> = vec![edge_type, from_label, from, to_label, to];
    parts.extend(discriminator);
    serde_json::to_string(&parts)
        .unwrap_or_else(|_| format!("{edge_type}/{from_label}/{from}/{to_label}/{to}"))
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
