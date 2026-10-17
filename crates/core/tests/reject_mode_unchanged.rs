//! Phase 12.6: `on_existing: ignore` (the default) is unchanged by Phase 12 —
//! a later event for an already-projected entity is `Skipped(AlreadyProjected)`
//! regardless of its `sequence`, never an `Updated`. Phase 11's idempotency
//! guarantee still holds verbatim.

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

fn user_updated(sequence: u64, state: &str) -> Event {
    Event {
        event_id: format!("evt-seq-{sequence}"),
        event_type: "UserUpdated".to_string(),
        payload: format!(r#"{{ "id": "u1", "state": "{state}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

#[test]
fn sql_ignore_mode_skips_a_higher_sequence_update() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, state TEXT)", [])?;
    }

    // No `on_existing` field at all — the Phase 11 mapping shape, unchanged.
    let mut sql_mappings = HashMap::new();
    sql_mappings.insert(
        "UserUpdated".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserUpdated
table: users
primary_key: id
version: 1
columns:
  id: payload.id
  state: payload.state
"#,
        )?,
    );

    let adapter = SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(sql_mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let r1 = adapter.handle(&user_updated(1, "s1"));
    assert!(
        matches!(r1.outcome, AdapterOutcome::Created),
        "{:?}",
        r1.outcome
    );

    // A later, strictly-newer-sequence event for the same entity: still just
    // an idempotent skip under `ignore` — sequence is irrelevant here.
    let r2 = adapter.handle(&user_updated(2, "s2"));
    assert!(
        matches!(
            r2.outcome,
            AdapterOutcome::Skipped(SkipReason::AlreadyProjected)
        ),
        "expected Skipped(AlreadyProjected), got {:?}",
        r2.outcome
    );

    let conn = Connection::open(&db_path)?;
    let state: String =
        conn.query_row("SELECT state FROM users WHERE id = 'u1'", [], |r| r.get(0))?;
    assert_eq!(state, "s1", "ignore mode must never update the row");

    Ok(())
}

#[test]
fn document_ignore_mode_skips_a_higher_sequence_update() -> Result<(), Box<dyn std::error::Error>> {
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

    let r1 = adapter.handle(&user_updated(1, "s1"));
    assert!(
        matches!(r1.outcome, AdapterOutcome::Created),
        "{:?}",
        r1.outcome
    );

    let r2 = adapter.handle(&user_updated(2, "s2"));
    assert!(
        matches!(
            r2.outcome,
            AdapterOutcome::Skipped(SkipReason::AlreadyProjected)
        ),
        "expected Skipped(AlreadyProjected), got {:?}",
        r2.outcome
    );

    let content = std::fs::read_to_string(root.join("users").join("u1.json"))?;
    assert!(
        content.contains("\"s1\""),
        "ignore mode must never rewrite the document: {content}"
    );

    Ok(())
}
