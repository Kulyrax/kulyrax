use std::collections::HashMap;

use super::adapter::DocumentError;
use super::mapping::DocumentMapping;
use crate::event::Event;

/// Borrowed preview builder (Phase 1)
pub struct DocumentPreviewBuilder<'a> {
    mappings: &'a HashMap<String, DocumentMapping>,
}

impl<'a> DocumentPreviewBuilder<'a> {
    pub fn new(mappings: &'a HashMap<String, DocumentMapping>) -> Self {
        Self { mappings }
    }

    pub fn preview(&self, event: &Event) -> Result<(serde_json::Value, String), DocumentError> {
        let mapping = self
            .mappings
            .get(&event.event_type)
            .ok_or_else(|| DocumentError::MappingNotFound(event.event_type.clone()))?;

        mapping.apply(event)
    }
}
