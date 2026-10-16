//! Phase 15.6: a live (unsorted) stream that delivers a facet event before the
//! entity's create → `Skipped(EntityAbsent)`. This is the documented live
//! contract (15.3 / 15.5): out-of-order facet-before-create is skipped, not
//! buffered; replay handles ordering by sorting (see `facet_before_create_replay`).

use kulyrax_core::adapter::document::file::FileDocumentAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

fn created(sequence: u64) -> Event {
    Event {
        event_id: format!("evt-created-{sequence}"),
        event_type: "UserCreated".to_string(),
        payload: r#"{ "id": "u1", "display_name": "Ada" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

fn email_changed(sequence: u64, email: &str) -> Event {
    Event {
        event_id: format!("evt-email-{sequence}"),
        event_type: "UserEmailChanged".to_string(),
        payload: format!(r#"{{ "id": "u1", "email": "{email}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

fn mappings() -> HashMap<String, DocumentMapping> {
    let mut m = HashMap::new();
    m.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            r#"
event: UserCreated
collection: users
version: 1
id: payload.id
document:
  id: payload.id
  display_name: payload.display_name
"#,
        )
        .unwrap(),
    );
    m.insert(
        "UserEmailChanged".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            r#"
event: UserEmailChanged
collection: users
version: 1
id: payload.id
facet: contact
document:
  email: payload.email
"#,
        )
        .unwrap(),
    );
    m
}

#[test]
fn facet_before_create_is_entity_absent() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let root = dir.path();
    let adapter = FileDocumentAdapter::new(
        "file".to_string(),
        root.to_path_buf(),
        10,
        DocumentRuntimeBuilder::new(mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    // Facet event arrives first (higher sequence, but the entity is not created yet).
    let r_facet = adapter.handle(&email_changed(5, "ada@x.com"));
    assert!(
        matches!(
            r_facet.outcome,
            AdapterOutcome::Skipped(SkipReason::EntityAbsent)
        ),
        "{:?}",
        r_facet.outcome
    );
    assert!(!root.join("users").join("u1.json").exists());

    // Create arrives; then the (redelivered) facet event applies.
    adapter.handle(&created(1));
    let r_facet2 = adapter.handle(&email_changed(5, "ada@x.com"));
    assert!(
        matches!(r_facet2.outcome, AdapterOutcome::Created),
        "{:?}",
        r_facet2.outcome
    );

    let doc: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        root.join("users").join("u1.json"),
    )?)?;
    assert_eq!(doc["display_name"], "Ada");
    assert_eq!(doc["email"], "ada@x.com");

    Ok(())
}
