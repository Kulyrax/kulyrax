//! Phase 13.4: `delete permanent: true` at sequence 2, then an `upsert` at
//! sequence 5 (which would otherwise resurrect the entity) → `Skipped(Tombstoned)`.
//! The entity stays absent forever — a permanent tombstone is terminal.

mod common;

use common::prepared;
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

fn upsert_event(sequence: u64, state: &str) -> Event {
    Event {
        event_id: format!("evt-upsert-{sequence}"),
        event_type: "UserUpdated".to_string(),
        payload: format!(r#"{{ "id": "u1", "state": "{state}" }}"#),
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
fn sql_permanent_tombstone_rejects_later_resurrection_attempt()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, state TEXT)", [])?;
    }

    let mut mappings = HashMap::new();
    mappings.insert(
        "UserUpdated".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserUpdated
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
permanent: true
columns:
  id: payload.id
"#,
        )?,
    );

    let adapter = prepared(SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ));

    adapter.handle(&upsert_event(1, "s1"));
    let r2 = adapter.handle(&delete_event(2));
    assert!(
        matches!(r2.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r2.outcome
    );

    let r3 = adapter.handle(&upsert_event(5, "s5"));
    assert!(
        matches!(r3.outcome, AdapterOutcome::Skipped(SkipReason::Tombstoned)),
        "expected Skipped(Tombstoned), got {:?}",
        r3.outcome
    );

    let conn = Connection::open(&db_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    assert_eq!(count, 0, "entity must stay absent");

    // A further delete for the same key is also rejected — the tombstone is terminal.
    let r4 = adapter.handle(&delete_event(9));
    assert!(
        matches!(r4.outcome, AdapterOutcome::Skipped(SkipReason::Tombstoned)),
        "expected Skipped(Tombstoned), got {:?}",
        r4.outcome
    );

    Ok(())
}

#[test]
fn document_permanent_tombstone_rejects_later_resurrection_attempt()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let root = dir.path();

    let mut mappings = HashMap::new();
    mappings.insert(
        "UserUpdated".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            r#"
event: UserUpdated
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
permanent: true
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

    adapter.handle(&upsert_event(1, "s1"));
    let r2 = adapter.handle(&delete_event(2));
    assert!(
        matches!(r2.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r2.outcome
    );

    let r3 = adapter.handle(&upsert_event(5, "s5"));
    assert!(
        matches!(r3.outcome, AdapterOutcome::Skipped(SkipReason::Tombstoned)),
        "expected Skipped(Tombstoned), got {:?}",
        r3.outcome
    );

    assert!(!root.join("users").join("u1.json").exists());

    Ok(())
}
