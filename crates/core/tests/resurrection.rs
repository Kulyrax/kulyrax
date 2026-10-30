//! Phase 13.4: `[create s1, delete s2, recreate s3]` → the entity exists
//! again, carrying the sequence-3 state (resurrection is the default — a
//! tombstone is not permanent unless the delete mapping opts in). A further,
//! older attempt (`s0`) after the resurrection is `Skipped(StaleSequence)`.

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
fn sql_resurrection_after_delete() -> Result<(), Box<dyn std::error::Error>> {
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

    let r1 = adapter.handle(&upsert_event(1, "s1"));
    assert!(
        matches!(r1.outcome, AdapterOutcome::Created),
        "{:?}",
        r1.outcome
    );
    let r2 = adapter.handle(&delete_event(2));
    assert!(
        matches!(r2.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r2.outcome
    );
    let r3 = adapter.handle(&upsert_event(3, "s3"));
    assert!(
        matches!(r3.outcome, AdapterOutcome::Created),
        "resurrection must be a Created outcome: {:?}",
        r3.outcome
    );

    let conn = Connection::open(&db_path)?;
    let state: String =
        conn.query_row("SELECT state FROM users WHERE id = 'u1'", [], |r| r.get(0))?;
    assert_eq!(state, "s3");
    let (last_sequence, deleted): (i64, i64) = conn.query_row(
        "SELECT last_sequence, deleted FROM conduit_projection_state WHERE target_table = 'users'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(last_sequence, 3);
    assert_eq!(
        deleted, 0,
        "resurrected entity must not still show as deleted"
    );

    // An older attempt, after the resurrection, must not resurrect the old delete.
    let r_old = adapter.handle(&upsert_event(0, "stale"));
    assert!(
        matches!(
            r_old.outcome,
            AdapterOutcome::Skipped(SkipReason::StaleSequence)
        ),
        "expected Skipped(StaleSequence), got {:?}",
        r_old.outcome
    );
    let state: String =
        conn.query_row("SELECT state FROM users WHERE id = 'u1'", [], |r| r.get(0))?;
    assert_eq!(
        state, "s3",
        "the stale attempt must not have changed anything"
    );

    Ok(())
}

#[test]
fn document_resurrection_after_delete() -> Result<(), Box<dyn std::error::Error>> {
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
    assert!(!root.join("users").join("u1.json").exists());

    let r3 = adapter.handle(&upsert_event(3, "s3"));
    assert!(
        matches!(r3.outcome, AdapterOutcome::Created),
        "{:?}",
        r3.outcome
    );

    let content = std::fs::read_to_string(root.join("users").join("u1.json"))?;
    assert!(content.contains("\"s3\""), "{content}");

    Ok(())
}
