//! Phase 11.4: the pre-existing Phase 4 guarantee — replaying the exact same
//! `event_id` twice is deduplicated — re-verified against the Phase 11.2/11.3
//! entity-aware guard (`conduit_projection_state`, `.conduit/entities/...`)
//! that replaced the Phase 4 event_id-only guard.

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

fn test_event() -> Event {
    Event {
        event_id: "evt-1".to_string(),
        event_type: "UserCreated".to_string(),
        payload: r#"{ "id": "u1", "email": "a@b.com" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    }
}

#[test]
fn sql_replaying_the_same_event_id_is_still_deduplicated() -> Result<(), Box<dyn std::error::Error>>
{
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

    let event = test_event();

    let r1 = adapter.handle(&event);
    assert!(r1.is_success(), "{:?}", r1.outcome);
    let r2 = adapter.handle(&event);
    assert!(r2.is_success(), "{:?}", r2.outcome);

    let conn = Connection::open(&db_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    assert_eq!(count, 1, "exact event_id replay must not duplicate the row");

    Ok(())
}

#[test]
fn document_replaying_the_same_event_id_is_still_deduplicated()
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

    let event = test_event();

    let r1 = adapter.handle(&event);
    assert!(r1.is_success(), "{:?}", r1.outcome);
    let r2 = adapter.handle(&event);
    assert!(r2.is_success(), "{:?}", r2.outcome);

    let entries: Vec<_> = std::fs::read_dir(root.join("users"))?.collect();
    assert_eq!(
        entries.len(),
        1,
        "exact event_id replay must not duplicate the document"
    );

    Ok(())
}
