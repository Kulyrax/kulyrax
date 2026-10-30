//! Phase 13.6: the same `[create, update, delete]` lifecycle stream projected
//! independently into SQL and a document store — both end absent, and both
//! tombstones agree on the sequence at which the entity was removed.

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

fn lifecycle_events() -> Vec<Event> {
    vec![
        Event {
            event_id: "e1".to_string(),
            event_type: "UserUpdated".to_string(),
            payload: r#"{"id":"u1","name":"Alice"}"#.to_string(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        },
        Event {
            event_id: "e2".to_string(),
            event_type: "UserUpdated".to_string(),
            payload: r#"{"id":"u1","name":"Alice R1"}"#.to_string(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 2,
        },
        Event {
            event_id: "e3".to_string(),
            event_type: "UserDeleted".to_string(),
            payload: r#"{"id":"u1"}"#.to_string(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 3,
        },
    ]
}

#[test]
fn sql_and_document_agree_after_the_same_lifecycle() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;

    // --- SQL ---
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)", [])?;
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
  name: payload.name
"#,
        )?,
    );
    sql_mappings.insert(
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
    let sql_adapter = prepared(SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(sql_mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ));

    // --- Document ---
    let root = dir.path().join("docs");
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
  name: payload.name
"#,
        )?,
    );
    doc_mappings.insert(
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
    let doc_adapter = FileDocumentAdapter::new(
        "file".to_string(),
        root.clone(),
        20,
        DocumentRuntimeBuilder::new(doc_mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    for event in lifecycle_events() {
        let sql_result = sql_adapter.handle(&event);
        assert!(sql_result.is_success(), "{:?}", sql_result.outcome);
        let doc_result = doc_adapter.handle(&event);
        assert!(doc_result.is_success(), "{:?}", doc_result.outcome);
    }

    let conn = Connection::open(&db_path)?;
    let sql_count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    let sql_last_sequence: i64 = conn.query_row(
        "SELECT last_sequence FROM conduit_projection_state WHERE target_table = 'users'",
        [],
        |r| r.get(0),
    )?;

    assert_eq!(sql_count, 0, "SQL entity must be absent");
    assert!(
        !root.join("users").join("u1.json").exists(),
        "document entity must be absent"
    );

    let doc_guard_content = std::fs::read_to_string(
        root.join(".conduit")
            .join("entities")
            .join("users")
            .join("u1.done"),
    )?;
    let doc_guard: serde_json::Value = serde_json::from_str(&doc_guard_content)?;

    assert_eq!(sql_last_sequence, 3);
    assert_eq!(doc_guard["last_sequence"], 3);
    assert_eq!(doc_guard["deleted"], true);

    Ok(())
}
