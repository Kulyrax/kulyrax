//! Phase 13.6: `[create s1, update s2, delete s3]` (SQL and document) →
//! entity absent; the guard is a tombstone recorded at sequence 3.

use kulyrax_core::adapter::document::file::FileDocumentAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::adapter::{AdapterOutcome, StorageAdapter};
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

fn sql_mappings() -> HashMap<String, SqlMapping> {
    let mut m = HashMap::new();
    m.insert(
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
        )
        .unwrap(),
    );
    m.insert(
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
        )
        .unwrap(),
    );
    m
}

fn doc_mappings() -> HashMap<String, DocumentMapping> {
    let mut m = HashMap::new();
    m.insert(
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
        )
        .unwrap(),
    );
    m.insert(
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
        )
        .unwrap(),
    );
    m
}

#[test]
fn sql_full_lifecycle_ends_absent_with_tombstone() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, state TEXT)", [])?;
    }

    let adapter = SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(sql_mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let r1 = adapter.handle(&upsert_event(1, "s1"));
    assert!(
        matches!(r1.outcome, AdapterOutcome::Created),
        "{:?}",
        r1.outcome
    );
    let r2 = adapter.handle(&upsert_event(2, "s2"));
    assert!(
        matches!(r2.outcome, AdapterOutcome::Updated),
        "{:?}",
        r2.outcome
    );
    let r3 = adapter.handle(&delete_event(3));
    assert!(
        matches!(r3.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r3.outcome
    );

    let conn = Connection::open(&db_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    assert_eq!(count, 0, "entity must be absent after delete");

    let (last_sequence, deleted): (i64, i64) = conn.query_row(
        "SELECT last_sequence, deleted FROM conduit_projection_state WHERE target_table = 'users'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(last_sequence, 3);
    assert_eq!(deleted, 1, "guard must be a tombstone");

    Ok(())
}

#[test]
fn document_full_lifecycle_ends_absent_with_tombstone() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let root = dir.path();

    let adapter = FileDocumentAdapter::new(
        "file".to_string(),
        root.to_path_buf(),
        10,
        DocumentRuntimeBuilder::new(doc_mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let r1 = adapter.handle(&upsert_event(1, "s1"));
    assert!(
        matches!(r1.outcome, AdapterOutcome::Created),
        "{:?}",
        r1.outcome
    );
    let r2 = adapter.handle(&upsert_event(2, "s2"));
    assert!(
        matches!(r2.outcome, AdapterOutcome::Updated),
        "{:?}",
        r2.outcome
    );
    let r3 = adapter.handle(&delete_event(3));
    assert!(
        matches!(r3.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r3.outcome
    );

    assert!(
        !root.join("users").join("u1.json").exists(),
        "entity file must be removed after delete"
    );

    let guard_content = std::fs::read_to_string(
        root.join(".conduit")
            .join("entities")
            .join("users")
            .join("u1.done"),
    )?;
    let guard: serde_json::Value = serde_json::from_str(&guard_content)?;
    assert_eq!(guard["last_sequence"], 3);
    assert_eq!(guard["deleted"], true);

    Ok(())
}
