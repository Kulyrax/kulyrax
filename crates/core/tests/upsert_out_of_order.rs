//! Phase 12.6: applying sequences out of delivery order converges to the
//! state carried by the highest sequence — for both SQL and document
//! adapters — because `decide()` gates on `sequence`, not arrival order.

mod common;

use common::prepared;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::document::file::FileDocumentAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
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
fn sql_out_of_order_converges_to_highest_sequence() -> Result<(), Box<dyn std::error::Error>> {
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

    // Delivery order 3, 1, 2 — not sorted.
    for (sequence, state) in [(3, "s3"), (1, "s1"), (2, "s2")] {
        let result = adapter.handle(&user_updated(sequence, state));
        assert!(result.is_success(), "seq {sequence}: {:?}", result.outcome);
    }

    let conn = Connection::open(&db_path)?;
    let state: String =
        conn.query_row("SELECT state FROM users WHERE id = 'u1'", [], |r| r.get(0))?;
    assert_eq!(
        state, "s3",
        "final state must be the one carried by the highest sequence"
    );

    let last_sequence: i64 = conn.query_row(
        "SELECT last_sequence FROM conduit_projection_state WHERE target_table = 'users'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(
        last_sequence, 3,
        "guard must never regress below the max sequence applied"
    );

    Ok(())
}

#[test]
fn document_out_of_order_converges_to_highest_sequence() -> Result<(), Box<dyn std::error::Error>> {
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

    for (sequence, state) in [(3, "s3"), (1, "s1"), (2, "s2")] {
        let result = adapter.handle(&user_updated(sequence, state));
        assert!(result.is_success(), "seq {sequence}: {:?}", result.outcome);
    }

    let content = std::fs::read_to_string(root.join("users").join("u1.json"))?;
    assert!(
        content.contains("\"s3\""),
        "final document must carry the state from the highest sequence: {content}"
    );

    Ok(())
}
