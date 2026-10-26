//! Phase 11.4: a mapping's `primary_key` (SQL) or `id` (document) path
//! resolving to a JSON object/array means the mapping points at the wrong
//! place — the build must fail fast and never reach the guard table/file.

mod common;

use common::prepared;
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

fn failure_message(outcome: &AdapterOutcome) -> String {
    match outcome {
        AdapterOutcome::Failed(err) => err.to_string(),
        other => panic!("expected a failed outcome, got {:?}", other),
    }
}

fn event_with_non_scalar_id(payload: &str) -> Event {
    Event {
        event_id: "evt-1".to_string(),
        event_type: "UserCreated".to_string(),
        payload: payload.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    }
}

#[test]
fn sql_primary_key_resolving_to_an_array_fails_the_build() -> Result<(), Box<dyn std::error::Error>>
{
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY)", [])?;
    }

    let mut sql_mappings = HashMap::new();
    sql_mappings.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserCreated
table: users
primary_key: id
version: 1
columns:
  id: payload.id
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

    // "id" resolves to a JSON array, not a scalar.
    let event = event_with_non_scalar_id(r#"{ "id": ["a", "b"] }"#);
    let result = adapter.handle(&event);

    assert!(
        !result.is_success(),
        "non-scalar primary key must fail the build"
    );
    let message = failure_message(&result.outcome);
    assert!(
        message.contains("non-scalar"),
        "unexpected error: {}",
        message
    );

    let conn = Connection::open(&db_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    assert_eq!(
        count, 0,
        "no row should be written when the key is rejected"
    );

    Ok(())
}

#[test]
fn document_id_resolving_to_an_object_fails_the_build() -> Result<(), Box<dyn std::error::Error>> {
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

    // "id" resolves to a JSON object, not a scalar.
    let event = event_with_non_scalar_id(r#"{ "id": { "nested": true } }"#);
    let result = adapter.handle(&event);

    assert!(!result.is_success(), "non-scalar id must fail the build");
    let message = failure_message(&result.outcome);
    assert!(
        message.contains("non-scalar"),
        "unexpected error: {}",
        message
    );

    assert!(
        !root.join("users").exists(),
        "no document should be written when the id is rejected"
    );

    Ok(())
}
