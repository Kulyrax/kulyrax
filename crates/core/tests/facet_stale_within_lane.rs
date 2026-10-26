//! Phase 15.6: within a single facet lane the Phase 12 sequence gate still
//! holds — a `contact` event at seq 5 then another at seq 3 → the seq-3 one is
//! `Skipped(StaleSequence)` and the email stays at the seq-5 value.

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
    m
}

#[test]
fn stale_event_within_a_facet_lane_is_skipped() -> Result<(), Box<dyn std::error::Error>> {
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

    let r5 = adapter.handle(&email_changed(5, "new@x.com"));
    assert!(
        matches!(r5.outcome, AdapterOutcome::Created),
        "{:?}",
        r5.outcome
    );

    let r3 = adapter.handle(&email_changed(3, "old@x.com"));
    assert!(
        matches!(
            r3.outcome,
            AdapterOutcome::Skipped(SkipReason::StaleSequence)
        ),
        "{:?}",
        r3.outcome
    );

    let conn = Connection::open(&db_path)?;
    let email: String =
        conn.query_row("SELECT email FROM users WHERE id = 'u1'", [], |r| r.get(0))?;
    assert_eq!(email, "new@x.com");

    let (last_sequence, deleted): (i64, i64) = conn.query_row(
        "SELECT last_sequence, deleted FROM conduit_projection_state
         WHERE target_table = 'users' AND facet = 'contact'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(last_sequence, 5, "contact lane stays at the seq-5 value");
    assert_eq!(deleted, 0);

    Ok(())
}
