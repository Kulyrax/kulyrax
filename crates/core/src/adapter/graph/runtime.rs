use std::collections::HashMap;

use super::adapter::GraphError;
use super::mapping::{GraphMapping, ResolvedRecord};
use crate::adapter::{OnExisting, Operation};
use crate::event::Event;
use crate::upcast::UpcasterRegistry;

/// Graph projection plus the version metadata used to produce it (Phase 10.3
/// concept, reused as-is).
pub struct GraphProjection {
    /// The single node or edge this event projects.
    pub record: ResolvedRecord,
    pub source_version: u32,
    pub projected_version: u32,
    /// Write mode (Phase 12.2). Read only when `operation: upsert`.
    pub on_existing: OnExisting,
    /// Create/update, or remove (Phase 13.1). `delete` on a node = detach delete.
    pub operation: Operation,
    /// `operation: delete` only (Phase 13.4): mark the tombstone permanent.
    pub permanent: bool,
    /// Phase 15.1: the facet this mapping owns, normalized (`""` = default
    /// facet / whole record). Always `""` for edges.
    pub facet: String,
}

/// Owned runtime builder — analog of `KvRuntimeBuilder` / `SqlRuntimeBuilder`.
pub struct GraphRuntimeBuilder {
    mappings: HashMap<String, GraphMapping>,
}

impl GraphRuntimeBuilder {
    pub fn new(mappings: HashMap<String, GraphMapping>) -> Self {
        Self { mappings }
    }

    /// The mappings this builder projects, by event type.
    pub fn mappings(&self) -> &HashMap<String, GraphMapping> {
        &self.mappings
    }

    /// Build the graph projection for `event` from a node or edge mapping,
    /// upcasting its payload to the mapping's target version first when the
    /// versions differ. A subgraph mapping has several; see [`Self::build_all`].
    pub fn build(
        &self,
        event: &Event,
        upcasters: &UpcasterRegistry,
    ) -> Result<GraphProjection, GraphError> {
        let mut all = self.build_all(event, upcasters)?;
        if all.len() != 1 {
            return Err(GraphError::BuildFailed(format!(
                "the mapping for {} projects {} records; use build_all",
                event.event_type,
                all.len()
            )));
        }
        all.pop()
            .ok_or_else(|| GraphError::MappingNotFound(event.event_type.clone()))
    }

    /// Build every record `event` projects, in mapping order: one for a node
    /// or edge mapping, one per entry of `records` for a subgraph.
    pub fn build_all(
        &self,
        event: &Event,
        upcasters: &UpcasterRegistry,
    ) -> Result<Vec<GraphProjection>, GraphError> {
        let mapping = self
            .mappings
            .get(&event.event_type)
            .ok_or_else(|| GraphError::MappingNotFound(event.event_type.clone()))?;

        let source_version = event.version();
        let projected_version = mapping.version();

        let upcasted;
        let at_version = if source_version == projected_version {
            event
        } else {
            let payload: serde_json::Value = serde_json::from_str(&event.payload)
                .map_err(|e| GraphError::BuildFailed(format!("invalid payload JSON: {}", e)))?;
            let upcasted_payload = upcasters
                .upcast(
                    &event.event_type,
                    payload,
                    source_version,
                    projected_version,
                )
                .map_err(|e| GraphError::UnsupportedVersion {
                    event_type: event.event_type.clone(),
                    from_version: source_version,
                    to_version: projected_version,
                    reason: e.to_string(),
                })?;
            upcasted = Event {
                payload: serde_json::to_string(&upcasted_payload).map_err(|e| {
                    GraphError::BuildFailed(format!("failed to serialize upcasted payload: {}", e))
                })?,
                version: projected_version,
                ..event.clone()
            };
            &upcasted
        };

        mapping
            .records()
            .into_iter()
            .map(|record| {
                Ok(GraphProjection {
                    record: record.resolve(at_version)?,
                    source_version,
                    projected_version,
                    on_existing: record.on_existing(),
                    operation: record.operation(),
                    permanent: record.permanent(),
                    facet: record.facet_key().to_string(),
                })
            })
            .collect()
    }
}
