use std::collections::HashMap;

use super::adapter::KvError;
use super::mapping::KvMapping;
use crate::event::Event;

/// Borrowed preview builder — analog of `SqlPreviewBuilder` / `DocumentPreviewBuilder`.
pub struct KvPreviewBuilder<'a> {
    mappings: &'a HashMap<String, KvMapping>,
}

impl<'a> KvPreviewBuilder<'a> {
    pub fn new(mappings: &'a HashMap<String, KvMapping>) -> Self {
        Self { mappings }
    }

    pub fn preview(&self, event: &Event) -> Result<(serde_json::Value, String), KvError> {
        let mapping = self
            .mappings
            .get(&event.event_type)
            .ok_or_else(|| KvError::MappingNotFound(event.event_type.clone()))?;

        mapping.apply(event)
    }
}
