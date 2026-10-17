//! Phase 15.6: `[create s1, delete s2, create s3, contact s4]` → the entity is
//! present again (resurrected by the default-facet upsert at seq 3, which
//! clears every facet lane's tombstone) and the `contact` facet update at
//! seq 4 then applies normally.

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

fn created(sequence: u64, name: &str) -> Event {
    Event {
        event_id: format!("evt-created-{sequence}"),
        event_type: "UserCreated".to_string(),
        payload: format!(r#"{{ "id": "u1", "display_name": "{name}" }}"#),
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
    m
}

#[test]
fn default_facet_resurrection_reopens_facet_lanes() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute(
            "CREATE TABLE users (id TEXT PRIMARY KEY, display_name TEXT, email TEXT)",
            [],
        )?;
    }
    let adapter = SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    adapter.handle(&created(1, "Ada"));
    adapter.handle(&deleted(2));
    let r3 = adapter.handle(&created(3, "Ada2"));
    assert!(
        matches!(r3.outcome, AdapterOutcome::Created),
        "{:?}",
        r3.outcome
    );

    let r4 = adapter.handle(&email_changed(4, "ada@x.com"));
    assert!(
        matches!(r4.outcome, AdapterOutcome::Created),
        "{:?}",
        r4.outcome
    );

    let conn = Connection::open(&db_path)?;
    let (name, email): (String, String) = conn.query_row(
        "SELECT display_name, email FROM users WHERE id = 'u1'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(name, "Ada2");
    assert_eq!(email, "ada@x.com");

    let contact_deleted: i64 = conn.query_row(
        "SELECT deleted FROM conduit_projection_state
         WHERE target_table = 'users' AND facet = 'contact'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(contact_deleted, 0);

    Ok(())
}
