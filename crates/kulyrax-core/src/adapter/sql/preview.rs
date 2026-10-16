use std::collections::HashMap;

use super::adapter::SqlError;
use super::mapping::SqlMapping;
use crate::event::Event;

/// Borrowed SQL builder for preview / test-event
pub struct SqlPreviewBuilder<'a> {
    mappings: &'a HashMap<String, SqlMapping>,
}

impl<'a> SqlPreviewBuilder<'a> {
    pub fn new(mappings: &'a HashMap<String, SqlMapping>) -> Self {
        Self { mappings }
    }

    pub fn preview(
        &self,
        event: &Event,
    ) -> Result<(super::exec::SqlWrite, Vec<serde_json::Value>), SqlError> {
        let mapping = self
            .mappings
            .get(&event.event_type)
            .ok_or_else(|| SqlError::MappingNotFound(event.event_type.clone()))?;

        mapping.build(event)
    }
}
