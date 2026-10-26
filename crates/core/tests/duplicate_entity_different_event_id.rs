//! Phase 11.4: the actual point of Phase 11 — two distinct `event_id`s that
//! resolve to the same entity are deduplicated by entity identity, not
//! `event_id`. Phase 4 idempotency (event_id-only) left this open: SQL threw a
//! raw PK constraint violation, and the document adapter silently wrote a
//! second file for the same logical entity.

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

// ------------------------------------------------------------
// Helpers
// ------------------------------------------------------------

/// Same entity ("u1"), a different `event_id`/`sequence` each call, and a
/// distinguishable payload field so we can tell which write actually landed.
fn event_for_entity_u1(event_id: &str, sequence: u64, email: &str) -> Event {
    Event {
        event_id: event_id.to_string(),
        event_type: "UserCreated".to_string(),
        payload: format!(r#"{{ "id": "u1", "email": "{}" }}"#, email),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

// ------------------------------------------------------------
// SQL
// ------------------------------------------------------------

#[test]
fn sql_second_event_id_for_same_entity_is_skipped_first_write_persists()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT)", [])?;
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
  email: payload.email
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

    let first = event_for_entity_u1("evt-1", 1, "first@example.com");
    let second = event_for_entity_u1("evt-2", 2, "second@example.com");

    let r1 = adapter.handle(&first);
    assert!(r1.is_success(), "{:?}", r1.outcome);

    let r2 = adapter.handle(&second);
    assert!(
        r2.is_success(),
        "a second event_id for an already-created entity must be a clean skip, \
         never a raw driver error: {:?}",
        r2.outcome
    );
    assert!(
        matches!(
            r2.outcome,
            AdapterOutcome::Skipped(SkipReason::AlreadyProjected)
        ),
        "expected Skipped(AlreadyProjected), got {:?}",
        r2.outcome
    );

    let conn = Connection::open(&db_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    assert_eq!(count, 1, "duplicate entity must not be inserted twice");

    let email: String =
        conn.query_row("SELECT email FROM users WHERE id = 'u1'", [], |r| r.get(0))?;
    assert_eq!(
        email, "first@example.com",
        "first creation wins — its data is what persists"
    );

    Ok(())
}

// ------------------------------------------------------------
// Document
// ------------------------------------------------------------

#[test]
fn document_second_event_id_for_same_entity_is_skipped_first_write_persists()
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
  email: payload.email
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

    let first = event_for_entity_u1("evt-1", 1, "first@example.com");
    let second = event_for_entity_u1("evt-2", 2, "second@example.com");

    let r1 = adapter.handle(&first);
    assert!(r1.is_success(), "{:?}", r1.outcome);

    let r2 = adapter.handle(&second);
    assert!(
        r2.is_success(),
        "a second event_id for an already-created entity must be a clean skip: {:?}",
        r2.outcome
    );
    assert!(
        matches!(
            r2.outcome,
            AdapterOutcome::Skipped(SkipReason::AlreadyProjected)
        ),
        "expected Skipped(AlreadyProjected), got {:?}",
        r2.outcome
    );

    // No second file: this is the case Phase 4 (event_id-keyed guard, event_id-keyed
    // path) missed entirely — a redelivered creation event under a new event_id used
    // to write a second, silent duplicate file.
    let entries: Vec<_> = std::fs::read_dir(root.join("users"))?.collect();
    assert_eq!(
        entries.len(),
        1,
        "duplicate entity must not produce a second file"
    );

    let content = std::fs::read_to_string(root.join("users").join("u1.json"))?;
    assert!(
        content.contains("first@example.com"),
        "first creation wins — its data is what persists: {}",
        content
    );

    Ok(())
}
