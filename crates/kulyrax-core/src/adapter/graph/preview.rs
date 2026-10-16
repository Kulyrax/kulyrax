use std::collections::HashMap;

use super::adapter::GraphError;
use super::mapping::{GraphMapping, ResolvedRecord};
use crate::event::Event;

/// Borrowed graph builder for preview / dry-run — resolves the record without
/// writing. Symmetry with `SqlPreviewBuilder` / `KvPreviewBuilder`.
pub struct GraphPreviewBuilder<'a> {
    mappings: &'a HashMap<String, GraphMapping>,
}

impl<'a> GraphPreviewBuilder<'a> {
    pub fn new(mappings: &'a HashMap<String, GraphMapping>) -> Self {
        Self { mappings }
    }

    pub fn preview(&self, event: &Event) -> Result<ResolvedRecord, GraphError> {
        let mapping = self
            .mappings
            .get(&event.event_type)
            .ok_or_else(|| GraphError::MappingNotFound(event.event_type.clone()))?;
        mapping.resolve(event)
    }
}
