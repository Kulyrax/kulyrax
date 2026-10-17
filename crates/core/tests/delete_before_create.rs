//! Phase 13.6: a `delete` arriving before any `create` for that entity still
//! writes a tombstone (Phase 13.1's "delete | stored: none | Delete" row) —
//! so the create, whenever it eventually arrives with a lower sequence, is
//! rejected `SkipStale` and the entity stays absent. Convergence doesn't
//! depend on the create having been seen first.

use kulyrax_core::adapter::document::file::FileDocumentAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

fn create_event(sequence: u64) -> Event {
    Event {
        event_id: format!("evt-create-{sequence}"),
        event_type: "UserCreated".to_string(),
        payload: r#"{ "id": "u1", "state": "s1" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

fn delete_event(sequence: u64) -> Event {
    Event {
        event_id: format!("evt-delete-{sequence}"),
        event_type: "UserDeleted".to_string(),
        payload: r#"{ "id": "u1" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

#[test]
fn sql_delete_then_create_leaves_entity_absent() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, state TEXT)", [])?;
    }

    let mut mappings = HashMap::new();
    mappings.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserCreated
table: users
primary_key: id
version: 1
on_existing: replace
columns:
  id: payload.id
  state: payload.state
"#,
        )?,
    );
    mappings.insert(
        "UserDeleted".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserDeleted
table: users
primary_key: id
version: 1
operation: delete
columns:
  id: payload.id
"#,
        )?,
    );

    let adapter = SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    // Delete at sequence 2 arrives first — no row exists yet.
    let r_delete = adapter.handle(&delete_event(2));
    assert!(
        matches!(r_delete.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r_delete.outcome
    );

    // The create, at a lower sequence, arrives after.
    let r_create = adapter.handle(&create_event(1));
    assert!(
        matches!(
            r_create.outcome,
            AdapterOutcome::Skipped(SkipReason::StaleSequence)
        ),
        "expected Skipped(StaleSequence), got {:?}",
        r_create.outcome
    );

    let conn = Connection::open(&db_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    assert_eq!(count, 0, "entity must remain absent");

    Ok(())
}

#[test]
fn document_delete_then_create_leaves_entity_absent() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let root = dir.path();

    let mut mappings = HashMap::new();
    mappings.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            r#"
event: UserCreated
collection: users
version: 1
id: payload.id
on_existing: replace
document:
  id: payload.id
  state: payload.state
"#,
        )?,
    );
    mappings.insert(
        "UserDeleted".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            r#"
event: UserDeleted
collection: users
version: 1
id: payload.id
operation: delete
document: {}
"#,
        )?,
    );

    let adapter = FileDocumentAdapter::new(
        "file".to_string(),
        root.to_path_buf(),
        10,
        DocumentRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let r_delete = adapter.handle(&delete_event(2));
    assert!(
        matches!(r_delete.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r_delete.outcome
    );

    let r_create = adapter.handle(&create_event(1));
    assert!(
        matches!(
            r_create.outcome,
            AdapterOutcome::Skipped(SkipReason::StaleSequence)
        ),
        "expected Skipped(StaleSequence), got {:?}",
        r_create.outcome
    );

    assert!(
        !root.join("users").join("u1.json").exists(),
        "entity must remain absent"
    );

    Ok(())
}
