use super::adapter::KvError;
use crate::adapter::{OnExisting, Operation, is_identity_scalar, json_scalar_to_string};
use crate::event::Event;
use crate::runtime::config::AdapterCapability;
use serde::Deserialize;
use serde_json::{Map, Value};

/// A flat key-value mapping (Phase 14) — the third storage kind, sharing the
/// exact `operation` / `on_existing` / `permanent` / `version` vocabulary
/// SQL and document mappings already use. `key` and `value` are the KV
/// analogs of a document mapping's `id` and `document`.
#[derive(Debug, Clone, Deserialize)]
pub struct KvMapping {
    pub event: String,

    /// Keyspace segment — the KV analog of a SQL `table` / document `collection`.
    pub namespace: String,

    /// Target schema version this mapping projects into. Must be explicit ($\ge 1$).
    pub version: u32,

    /// Path expression (`payload.` / `metadata.`, same syntax as `value` leaves)
    /// resolving this event's entity identity (Phase 11.1 concept, reused as-is).
    pub key: String,

    /// JSON-like structure where leaf values are payload/metadata paths — the
    /// projected value. Ignored (and must be empty) for `operation: delete`,
    /// same rule as a document mapping's `document` (Phase 13.1).
    pub value: Value,

    /// Write mode for a key that already has a projected value (Phase 12.2).
    /// Read only when `operation: upsert`.
    #[serde(default)]
    pub on_existing: OnExisting,

    /// What this mapping does: set/update the value, or remove it (Phase 13.1).
    #[serde(default)]
    pub operation: Operation,

    /// `operation: delete` only (Phase 13.4): this tombstone rejects every
    /// later event for this key, forever — no resurrection.
    #[serde(default)]
    pub permanent: bool,

    /// Phase 15.1: the named facet this mapping owns. `None` → the **default
    /// facet** (the whole value — every pre-Phase-15 mapping).
    /// `Some("contact")` → this mapping shallow-merges only its own top-level
    /// keys into the existing value and gates on its own per-facet sequence
    /// lane. A named facet is always a sequence-gated partial replace;
    /// `operation: delete` is only valid on the default facet.
    #[serde(default)]
    pub facet: Option<String>,

    /// Capabilities adapters must provide to run this projection.
    #[serde(default)]
    pub requires_capabilities: Vec<AdapterCapability>,
}

impl KvMapping {
    /// Normalized facet key — `""` for the default facet (Phase 15.1).
    pub fn facet_key(&self) -> &str {
        self.facet.as_deref().unwrap_or("")
    }

    /// Build the projected value JSON from event payload + metadata, plus the
    /// resolved entity identity, canonicalized to a string.
    pub fn apply(&self, event: &Event) -> Result<(Value, String), KvError> {
        let payload: Value = serde_json::from_str(&event.payload)
            .map_err(|e| KvError::BuildFailed(format!("invalid payload JSON: {}", e)))?;

        let payload_obj = payload
            .as_object()
            .ok_or_else(|| KvError::BuildFailed("payload must be a JSON object".into()))?;

        let value = self.expand_value(&self.value, payload_obj, &event.metadata)?;
        let key_value = self.resolve_path(&self.key, payload_obj, &event.metadata)?;

        // Phase 11.1: an identity must be a JSON scalar — an object/array/null
        // means `key` points at the wrong place; fail the build, don't let a
        // JSON blob (or "") reach the guard key / output filename.
        if !is_identity_scalar(&key_value) {
            return Err(KvError::BuildFailed(format!(
                "key path '{}' resolved to a non-scalar value; entity identity must be a string, number, or bool",
                self.key
            )));
        }

        Ok((value, json_scalar_to_string(&key_value)))
    }

    fn expand_value(
        &self,
        template: &Value,
        payload: &Map<String, Value>,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<Value, KvError> {
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

            Value::String(path) => self.resolve_path(path, payload, metadata),

            // Literal value (number, bool, null)
            other => Ok(other.clone()),
        }
    }

    fn resolve_path(
        &self,
        path: &str,
        payload: &Map<String, Value>,
        metadata: &std::collections::HashMap<String, String>,
    ) -> Result<Value, KvError> {
        if let Some(rest) = path.strip_prefix("payload.") {
            payload
                .get(rest)
                .cloned()
                .ok_or_else(|| KvError::BuildFailed(format!("payload field '{}' not found", rest)))
        } else if let Some(rest) = path.strip_prefix("metadata.") {
            metadata
                .get(rest)
                .map(|v| Value::String(v.clone()))
                .ok_or_else(|| KvError::BuildFailed(format!("metadata field '{}' not found", rest)))
        } else {
            Err(KvError::BuildFailed(format!(
                "invalid path '{}' - must start with 'payload.' or 'metadata.'",
                path
            )))
        }
    }
}
