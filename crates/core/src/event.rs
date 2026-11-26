use std::collections::HashMap;

use serde::{Deserialize, Serialize};

fn default_event_version() -> u32 {
    1
}

/// One event from the log: what Kulyrax projects. Build one with
/// [`Event::new`] and the `with_*` methods.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[non_exhaustive]
pub struct Event {
    /// The event's id. Unique per event; dead-letter files are named after it.
    pub event_id: String,
    /// The event's type, which routing and the mappings match on.
    pub event_type: String,
    /// The payload, a JSON document as a string. Mappings read it by path
    /// (`payload.id`).
    pub payload: String,
    /// String metadata. Mappings read it by path (`metadata.tenant`).
    pub metadata: HashMap<String, String>,
    /// Payload schema version. Unversioned (legacy) events default to 1.
    #[serde(default = "default_event_version")]
    pub version: u32,
    /// Monotonic per-entity sequence number from the source stream. Required —
    /// no default. This is what `decide()` (`adapter/mod.rs`) gates every write
    /// on: a redelivered or superseded event (`sequence <= last_sequence` on its
    /// guard lane) is skipped rather than reapplied. See
    /// `docs/producer-contract.md` for the monotonicity contract a producer
    /// must satisfy.
    pub sequence: u64,
}

impl Event {
    /// An event with no metadata, at payload schema version 1. Add the rest with
    /// [`with_metadata`](Self::with_metadata) and [`with_version`](Self::with_version).
    /// `Event` is `#[non_exhaustive]`: build it with this, not a struct literal,
    /// so a field added later is not a breaking change.
    pub fn new(
        event_id: impl Into<String>,
        event_type: impl Into<String>,
        sequence: u64,
        payload: impl Into<String>,
    ) -> Self {
        Self {
            event_id: event_id.into(),
            event_type: event_type.into(),
            payload: payload.into(),
            metadata: HashMap::new(),
            version: default_event_version(),
            sequence,
        }
    }

    /// The event with `metadata` in place of its current metadata.
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// The event at payload schema version `version`.
    pub fn with_version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    /// The event's id.
    pub fn id(&self) -> &str {
        &self.event_id
    }
    /// The event's type.
    pub fn event_type(&self) -> &str {
        &self.event_type
    }
    /// The payload's schema version.
    pub fn version(&self) -> u32 {
        self.version
    }
    /// The event's per-entity sequence.
    pub fn sequence(&self) -> u64 {
        self.sequence
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unversioned_legacy_json_defaults_to_version_one() {
        let json = r#"{
            "event_id": "e1",
            "event_type": "UserCreated",
            "payload": "{}",
            "metadata": {},
            "sequence": 1
        }"#;
        let event: Event = serde_json::from_str(json).unwrap();
        assert_eq!(event.version(), 1);
    }

    #[test]
    fn explicit_version_is_preserved() {
        let json = r#"{
            "event_id": "e1",
            "event_type": "UserCreated",
            "payload": "{}",
            "metadata": {},
            "version": 3,
            "sequence": 1
        }"#;
        let event: Event = serde_json::from_str(json).unwrap();
        assert_eq!(event.version(), 3);
    }

    #[test]
    fn sequence_is_required_with_no_default() {
        let json = r#"{
            "event_id": "e1",
            "event_type": "UserCreated",
            "payload": "{}",
            "metadata": {}
        }"#;
        let err = serde_json::from_str::<Event>(json).unwrap_err();
        assert!(err.to_string().contains("sequence"));
    }

    #[test]
    fn explicit_sequence_is_preserved() {
        let json = r#"{
            "event_id": "e1",
            "event_type": "UserCreated",
            "payload": "{}",
            "metadata": {},
            "sequence": 42
        }"#;
        let event: Event = serde_json::from_str(json).unwrap();
        assert_eq!(event.sequence(), 42);
    }
}
