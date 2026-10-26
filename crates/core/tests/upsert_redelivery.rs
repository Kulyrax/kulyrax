//! Phase 12.6: redelivering the exact same event under `on_existing: replace`
//! is a clean `Skipped(StaleSequence)` — `event_seq == last_sequence` is
//! stale, not "already projected" (that reason is `ignore` mode's, Phase 11).
//! The row / file / guard are byte-identical to after the first delivery.

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

fn user_updated() -> Event {
    Event {
        event_id: "evt-1".to_string(),
        event_type: "UserUpdated".to_string(),
        payload: r#"{ "id": "u1", "state": "s1" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 5,
    }
}

#[test]
fn sql_redelivery_is_stale_not_already_projected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, state TEXT)", [])?;
    }

    let mut sql_mappings = HashMap::new();
    sql_mappings.insert(
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

    let adapter = prepared(SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(sql_mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ));

    let event = user_updated();

    let r1 = adapter.handle(&event);
    assert!(
        matches!(r1.outcome, AdapterOutcome::Created),
        "{:?}",
        r1.outcome
    );

    let row_before = row_state(&db_path)?;

    let r2 = adapter.handle(&event);
    assert!(
        matches!(
            r2.outcome,
            AdapterOutcome::Skipped(SkipReason::StaleSequence)
        ),
        "expected Skipped(StaleSequence), got {:?}",
        r2.outcome
    );

    assert_eq!(
        row_state(&db_path)?,
        row_before,
        "row must be untouched by the redelivery"
    );

    Ok(())
}

fn row_state(db_path: &std::path::Path) -> rusqlite::Result<(String, i64)> {
    let conn = Connection::open(db_path)?;
    let state: String =
        conn.query_row("SELECT state FROM users WHERE id = 'u1'", [], |r| r.get(0))?;
    let last_sequence: i64 = conn.query_row(
        "SELECT last_sequence FROM conduit_projection_state WHERE target_table = 'users'",
        [],
        |r| r.get(0),
    )?;
    Ok((state, last_sequence))
}

#[test]
fn document_redelivery_is_stale_not_already_projected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let root = dir.path();

    let mut doc_mappings = HashMap::new();
    doc_mappings.insert(
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

    let adapter = FileDocumentAdapter::new(
        "file".to_string(),
        root.to_path_buf(),
        10,
        DocumentRuntimeBuilder::new(doc_mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let event = user_updated();

    let r1 = adapter.handle(&event);
    assert!(
        matches!(r1.outcome, AdapterOutcome::Created),
        "{:?}",
        r1.outcome
    );

    let doc_path = root.join("users").join("u1.json");
    let content_before = std::fs::read_to_string(&doc_path)?;
    let guard_path = root
        .join(".conduit")
        .join("entities")
        .join("users")
        .join("u1.done");
    let guard_before = std::fs::read_to_string(&guard_path)?;

    let r2 = adapter.handle(&event);
    assert!(
        matches!(
            r2.outcome,
            AdapterOutcome::Skipped(SkipReason::StaleSequence)
        ),
        "expected Skipped(StaleSequence), got {:?}",
        r2.outcome
    );

    assert_eq!(
        std::fs::read_to_string(&doc_path)?,
        content_before,
        "document must be untouched"
    );
    assert_eq!(
        std::fs::read_to_string(&guard_path)?,
        guard_before,
        "guard must be untouched"
    );

    Ok(())
}
