use std::collections::HashMap;

use super::adapter::KvError;
use super::mapping::KvMapping;
use crate::adapter::{OnExisting, Operation};
use crate::event::Event;
use crate::upcast::UpcasterRegistry;

/// Key-value projection plus the version metadata used to produce it (Phase
/// 10.3 concept, reused as-is).
pub struct KvProjection {
    pub value: serde_json::Value,
    pub source_version: u32,
    pub projected_version: u32,
    /// Resolved entity identity (Phase 11.1) — keys the idempotency guard and
    /// the output filename, rather than `event.event_id`.
    pub entity_key: String,
    /// The mapping's keyspace segment (Phase 14.1) — the KV analog of a SQL
    /// `table` / document `collection`.
    pub namespace: String,
    /// The mapping's write mode (Phase 12.2). Read only when `operation:
    /// upsert` (Phase 13.1).
    pub on_existing: OnExisting,
    /// What this mapping does — set/update, or remove (Phase 13.1).
    pub operation: Operation,
    /// `operation: delete` only (Phase 13.4): mark the tombstone permanent.
    pub permanent: bool,
    /// Phase 15.1: the facet this mapping owns, normalized (`""` = default
    /// facet / whole value). A named facet shallow-merges its top-level keys
    /// and gates on its own lane in the guard sidecar's `facets` map.
    pub facet: String,
}

/// Owned runtime builder — analog of `SqlRuntimeBuilder` / `DocumentRuntimeBuilder`.
pub struct KvRuntimeBuilder {
    mappings: HashMap<String, KvMapping>,
}

impl KvRuntimeBuilder {
    pub fn new(mappings: HashMap<String, KvMapping>) -> Self {
        Self { mappings }
    }

    /// Build the key-value projection for `event`, upcasting its payload to
    /// the mapping's target version first when the versions differ.
    pub fn build(
        &self,
        event: &Event,
        upcasters: &UpcasterRegistry,
    ) -> Result<KvProjection, KvError> {
        let mapping = self
            .mappings
            .get(&event.event_type)
            .ok_or_else(|| KvError::MappingNotFound(event.event_type.clone()))?;

        let source_version = event.version();
        let projected_version = mapping.version;

        if source_version == projected_version {
            let (value, entity_key) = mapping.apply(event)?;
            return Ok(KvProjection {
                value,
                entity_key,
                source_version,
                projected_version,
                namespace: mapping.namespace.clone(),
                on_existing: mapping.on_existing,
                operation: mapping.operation,
                permanent: mapping.permanent,
                facet: mapping.facet_key().to_string(),
            });
        }

        let payload: serde_json::Value = serde_json::from_str(&event.payload)
            .map_err(|e| KvError::BuildFailed(format!("invalid payload JSON: {}", e)))?;

        let upcasted_payload = upcasters
            .upcast(
                &event.event_type,
                payload,
                source_version,
                projected_version,
            )
            .map_err(|e| KvError::UnsupportedVersion {
                event_type: event.event_type.clone(),
                from_version: source_version,
                to_version: projected_version,
                reason: e.to_string(),
            })?;

        let upcasted_event = Event {
            payload: serde_json::to_string(&upcasted_payload).map_err(|e| {
                KvError::BuildFailed(format!("failed to serialize upcasted payload: {}", e))
            })?,
            version: projected_version,
            ..event.clone()
        };

        let (value, entity_key) = mapping.apply(&upcasted_event)?;
        Ok(KvProjection {
            value,
            entity_key,
            source_version,
            projected_version,
            namespace: mapping.namespace.clone(),
            on_existing: mapping.on_existing,
            operation: mapping.operation,
            permanent: mapping.permanent,
            facet: mapping.facet_key().to_string(),
        })
    }
}
