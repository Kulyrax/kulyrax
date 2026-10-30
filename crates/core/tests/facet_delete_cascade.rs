//! Phase 15.6: `[create s1, contact s2, delete s3, contact s7]` → the entity
//! is absent and every guard lane is a tombstone at seq 3, so the later
//! (higher-sequence) `contact` update at seq 7 is `Skipped(EntityAbsent)` —
//! a facet update never resurrects an entity.

mod common;

use common::prepared;
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

fn created(sequence: u64) -> Event {
    Event {
        event_id: format!("evt-created-{sequence}"),
        event_type: "UserCreated".to_string(),
        payload: r#"{ "id": "u1", "display_name": "Ada" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

fn email_changed(sequence: u64, email: &str) -> Event {
    Event {
        event_id: format!("evt-email-{sequence}"),
        event_type: "UserEmailChanged".to_string(),
        payload: format!(r#"{{ "id": "u1", "email": "{email}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

fn deleted(sequence: u64) -> Event {
    Event {
        event_id: format!("evt-deleted-{sequence}"),
        event_type: "UserDeleted".to_string(),
        payload: r#"{ "id": "u1" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

fn mappings() -> HashMap<String, SqlMapping> {
    let mut m = HashMap::new();
    m.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserCreated
table: users
primary_key: id
version: 1
columns:
  id: payload.id
  display_name: payload.display_name
"#,
        )
        .unwrap(),
    );
    m.insert(
        "UserEmailChanged".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserEmailChanged
table: users
primary_key: id
version: 1
facet: contact
columns:
  id: payload.id
  email: payload.email
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

#[test]
fn default_facet_delete_cascades_to_facet_lanes() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute(
            "CREATE TABLE users (id TEXT PRIMARY KEY, display_name TEXT, email TEXT)",
            [],
        )?;
    }
    let adapter = prepared(SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ));

    adapter.handle(&created(1));
    adapter.handle(&email_changed(2, "ada@x.com"));
    let r3 = adapter.handle(&deleted(3));
    assert!(
        matches!(r3.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r3.outcome
    );

    let r7 = adapter.handle(&email_changed(7, "later@x.com"));
    assert!(
        matches!(
            r7.outcome,
            AdapterOutcome::Skipped(SkipReason::EntityAbsent)
        ),
        "{:?}",
        r7.outcome
    );

    let conn = Connection::open(&db_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    assert_eq!(count, 0);

    let contact_deleted: i64 = conn.query_row(
        "SELECT deleted FROM conduit_projection_state
         WHERE target_table = 'users' AND facet = 'contact'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(
        contact_deleted, 1,
        "contact lane was cascaded to a tombstone"
    );

    Ok(())
}
