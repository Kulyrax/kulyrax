use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::document::file::FileDocumentAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

// ------------------------------------------------------------
// Helpers
// ------------------------------------------------------------

fn test_event() -> Event {
    Event {
        event_id: "evt-1".to_string(),
        event_type: "UserCreated".to_string(),
        payload: r#"{ "id": "u1", "email": "a@b.com" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    }
}

// ------------------------------------------------------------
// Test
// ------------------------------------------------------------

#[test]
fn document_adapter_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    // Temp directory for document output
    let dir = tempdir()?;
    let root = dir.path();

    // Minimal document mapping
    let mut doc_mappings = HashMap::new();
    doc_mappings.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            r#"
event: UserCreated
collection: users
version: 1
id: payload.id
document:
  id: payload.id
  email: payload.email
"#,
        )?,
    );

    let builder = DocumentRuntimeBuilder::new(doc_mappings);

    let adapter = FileDocumentAdapter::new(
        "file".to_string(),
        root.to_path_buf(),
        10,
        builder,
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let event = test_event();

    // --------------------------------------------------
    // Call adapter DIRECTLY (no routing, no dispatch)
    // --------------------------------------------------

    let r1 = adapter.handle(&event);
    assert!(r1.is_success());

    let r2 = adapter.handle(&event);
    assert!(r2.is_success());

    // Verify document exists exactly once, keyed by entity id (Phase 11.1/11.3)
    let doc_path = root.join("users").join("u1.json");

    assert!(doc_path.exists(), "document not written");

    // Verify idempotency guard exists, keyed by (collection, entity_id)
    let guard_path = root
        .join(".conduit")
        .join("entities")
        .join("users")
        .join("u1.done");

    assert!(guard_path.exists(), "idempotency guard missing");

    // Verify no duplicate documents
    let entries: Vec<_> = std::fs::read_dir(root.join("users"))?.collect();

    assert_eq!(entries.len(), 1, "duplicate document written");

    Ok(())
}
