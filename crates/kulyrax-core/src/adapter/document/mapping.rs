use super::adapter::DocumentError;
use crate::adapter::{OnExisting, Operation, is_identity_scalar, json_scalar_to_string};
use crate::event::Event;
use crate::runtime::config::AdapterCapability;
use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Debug, Clone, Deserialize)]
pub struct DocumentMapping {
    pub event: String,
    pub collection: String,

    /// Target schema version this mapping projects into. Must be explicit ($\ge 1$).
    pub version: u32,

    /// Path expression (`payload.` / `metadata.`, same syntax as `document` leaves)
    /// resolving this event's entity identity (Phase 11.1). Keys the idempotency
    /// guard and the output filename, so a redelivered creation event with a new
    /// `event_id` for the same entity is skipped, not silently duplicated.
    pub id: String,

    /// JSON-like structure where leaf values are payload/metadata paths
    pub document: Value,

    /// Write mode for an entity that already has a projected document (Phase
    /// 12.2). Defaults to `ignore` — every mapping written before Phase 12
    /// keeps its Phase 11 behavior unchanged. Read only when `operation:
    /// upsert` (Phase 13.1).
    #[serde(default)]
    pub on_existing: OnExisting,

    /// What this mapping does: create/update the entity, or remove it (Phase
    /// 13.1). Defaults to `upsert` — every mapping written before Phase 13
    /// keeps its Phase 11/12 behavior unchanged. A `delete` mapping's
    /// `document` must be empty (`null`, `{}`, or `[]`) — no projection body,
    /// it only resolves the entity to remove.
    #[serde(default)]
    pub operation: Operation,

    /// `operation: delete` only (Phase 13.4): this tombstone rejects every
    /// later event for this key, forever — no resurrection. Ignored for
    /// `operation: upsert`.
    #[serde(default)]
    pub permanent: bool,

    /// Phase 15.1: the named facet this mapping owns. `None` → the **default
    /// facet** (the whole document — every pre-Phase-15 mapping).
    /// `Some("contact")` → this mapping shallow-merges only its own top-level
    /// keys into the existing document and gates on its own per-facet sequence
    /// lane. A named facet is always a sequence-gated partial replace;
    /// `operation: delete` is only valid on the default facet.
    #[serde(default)]
    pub facet: Option<String>,

    /// Capabilities adapters must provide to run this projection.
    #[serde(default)]
    pub requires_capabilities: Vec<AdapterCapability>,
}

impl DocumentMapping {
    /// Normalized facet key — `""` for the default facet (Phase 15.1).
    pub fn facet_key(&self) -> &str {
        self.facet.as_deref().unwrap_or("")
    }

    /// Build document JSON from event payload + metadata, plus the resolved
    /// entity identity (Phase 11.1), canonicalized to a string.
    pub fn apply(&self, event: &Event) -> Result<(Value, String), DocumentError> {
        // Parse payload JSON
        let payload: Value = serde_json::from_str(&event.payload)
            .map_err(|e| DocumentError::BuildFailed(format!("invalid payload JSON: {}", e)))?;

        let payload_obj = payload
            .as_object()
            .ok_or_else(|| DocumentError::BuildFailed("payload must be a JSON object".into()))?;

        // Expand template recursively
        let document = self.expand_value(&self.document, payload_obj, &event.metadata)?;
        let id_value = self.resolve_path(&self.id, payload_obj, &event.metadata)?;

        // Phase 11.1: an identity must be a JSON scalar — an object/array/null
        // means `id` points at the wrong place; fail the build, don't let a
        // JSON blob (or "") reach the guard key / output filename.
        if !is_identity_scalar(&id_value) {
            return Err(DocumentError::BuildFailed(format!(
                "id path '{}' resolved to a non-scalar value; entity identity must be a string, number, or bool",
                self.id
            )));
        }

        Ok((document, json_scalar_to_string(&id_value)))
    }

    fn expand_value(
        &self,
        template: &Value,
        payload: &Map<String, Value>,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<Value, DocumentError> {
        match template {
            Value::Object(map) => {
                let mut out = Map::new();
                for (k, v) in map {
                    out.insert(k.clone(), self.expand_value(v, payload, metadata)?);
                }
                Ok(Value::Object(out))
            }

            Value::Array(arr) => {
                let mut out = Vec::with_capacity(arr.len());
                for v in arr {
                    out.push(self.expand_value(v, payload, metadata)?);
                }
                Ok(Value::Array(out))
            }

            Value::String(path) => {
                // Interpret as path expression
                self.resolve_path(path, payload, metadata)
            }

            // Literal value (number, bool, null)
            other => Ok(other.clone()),
        }
    }

    fn resolve_path(
        &self,
        path: &str,
        payload: &Map<String, Value>,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<Value, DocumentError> {
        if let Some(rest) = path.strip_prefix("payload.") {
            payload.get(rest).cloned().ok_or_else(|| {
                DocumentError::BuildFailed(format!("payload field '{}' not found", rest))
            })
        } else if let Some(rest) = path.strip_prefix("metadata.") {
            metadata
                .get(rest)
                .map(|v| Value::String(v.clone()))
                .ok_or_else(|| {
                    DocumentError::BuildFailed(format!("metadata field '{}' not found", rest))
                })
        } else {
            Err(DocumentError::BuildFailed(format!(
                "invalid document path '{}'",
                path
            )))
        }
    }
}
