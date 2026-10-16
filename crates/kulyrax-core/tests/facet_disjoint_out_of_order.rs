//! Phase 15.6: two disjoint facets of one row (`contact` at seq 5, `login` at
//! seq 6) applied in **both** arrival orders → the row ends with the seq-5
//! email and the seq-6 login. Neither facet's sequence gate sees the other's
//! sequence, so arrival order cannot make one falsely supersede the other.

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

fn logged_in(sequence: u64, at: &str) -> Event {
    Event {
        event_id: format!("evt-login-{sequence}"),
        event_type: "UserLoggedIn".to_string(),
        payload: format!(r#"{{ "id": "u1", "last_login": "{at}" }}"#),
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
        "UserLoggedIn".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserLoggedIn
table: users
primary_key: id
version: 1
facet: login
columns:
  id: payload.id
  last_login: payload.last_login
"#,
        )
        .unwrap(),
    );
    m
}

fn run(order: &[Event]) -> (String, String) {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute(
            "CREATE TABLE users (id TEXT PRIMARY KEY, display_name TEXT, email TEXT, last_login TEXT)",
            [],
        )
        .unwrap();
    }
    let adapter = SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );
    for e in order {
        let r = adapter.handle(e);
        assert!(
            !matches!(r.outcome, AdapterOutcome::Failed(_)),
            "{:?}",
            r.outcome
        );
    }
    let conn = Connection::open(&db_path).unwrap();
    conn.query_row(
        "SELECT email, last_login FROM users WHERE id = 'u1'",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
    )
    .unwrap()
}

#[test]
fn disjoint_facets_converge_regardless_of_arrival_order() {
    let contact_first = run(&[
        created(1),
        email_changed(5, "ada@x.com"),
        logged_in(6, "2026-01-02"),
    ]);
    let login_first = run(&[
        created(1),
        logged_in(6, "2026-01-02"),
        email_changed(5, "ada@x.com"),
    ]);

    assert_eq!(
        contact_first,
        ("ada@x.com".to_string(), "2026-01-02".to_string())
    );
    assert_eq!(login_first, contact_first);
}
