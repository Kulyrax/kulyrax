//! Phase 11.1/11.3: the document adapter's output path is
//! `root/{collection}/{entity_id}.json` (re-keyed from `{event_type}` to
//! `{collection}` in Phase 13.3) — a behavior change from the prior
//! `root/{event_type}/{event_id}.json` (Phase 3/4). Confirms the file (and the
//! idempotency guard) are keyed by entity, not by the delivering event.

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

#[test]
fn output_file_and_guard_are_named_by_entity_id_not_event_id()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let root = dir.path();

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
"#,
        )?,
    );

    let adapter = FileDocumentAdapter::new(
        "file".to_string(),
        root.to_path_buf(),
        10,
        DocumentRuntimeBuilder::new(doc_mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    // event_id is deliberately unrelated to the entity id resolved from the payload.
    let event = Event {
        event_id: "evt-completely-unrelated".to_string(),
        event_type: "UserCreated".to_string(),
        payload: r#"{ "id": "entity-42" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    };

    let result = adapter.handle(&event);
    assert!(result.is_success(), "{:?}", result.outcome);

    let by_entity = root.join("users").join("entity-42.json");
    let by_event = root.join("users").join("evt-completely-unrelated.json");
    assert!(
        by_entity.exists(),
        "output must be written at the entity-keyed path"
    );
    assert!(
        !by_event.exists(),
        "output must not be written at the old event_id-keyed path"
    );

    let guard_by_entity = root
        .join(".conduit")
        .join("entities")
        .join("users")
        .join("entity-42.done");
    let guard_by_event = root
        .join(".conduit")
        .join("events")
        .join("evt-completely-unrelated.done");
    assert!(guard_by_entity.exists(), "guard must be keyed by entity id");
    assert!(
        !guard_by_event.exists(),
        "guard must not be keyed by event_id anymore"
    );

    Ok(())
}
