//! Phase 15.6: the same faceted stream into a SQL adapter and a document
//! adapter → the two converge. `[create s1, contact s5, login s6, contact s3]`
//! (the seq-3 contact event is stale within its lane) → both back ends end
//! with the seq-5 email, the seq-6 login, and the create's display_name.

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

fn created(seq: u64) -> Event {
    Event {
        event_id: format!("evt-created-{seq}"),
        event_type: "UserCreated".to_string(),
        payload: r#"{ "id": "u1", "display_name": "Ada" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn email_changed(seq: u64, email: &str) -> Event {
    Event {
        event_id: format!("evt-email-{seq}"),
        event_type: "UserEmailChanged".to_string(),
        payload: format!(r#"{{ "id": "u1", "email": "{email}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn logged_in(seq: u64, at: &str) -> Event {
    Event {
        event_id: format!("evt-login-{seq}"),
        event_type: "UserLoggedIn".to_string(),
        payload: format!(r#"{{ "id": "u1", "last_login": "{at}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn stream() -> Vec<Event> {
    vec![
        created(1),
        email_changed(5, "ada@x.com"),
        logged_in(6, "2026-01-02"),
        email_changed(3, "stale@x.com"),
    ]
}

fn sql_mappings() -> HashMap<String, SqlMapping> {
    let mut m = HashMap::new();
    m.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            "event: UserCreated\ntable: users\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n  display_name: payload.display_name\n",
        )
        .unwrap(),
    );
    m.insert(
        "UserEmailChanged".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            "event: UserEmailChanged\ntable: users\nprimary_key: id\nversion: 1\nfacet: contact\ncolumns:\n  id: payload.id\n  email: payload.email\n",
        )
        .unwrap(),
    );
    m.insert(
        "UserLoggedIn".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            "event: UserLoggedIn\ntable: users\nprimary_key: id\nversion: 1\nfacet: login\ncolumns:\n  id: payload.id\n  last_login: payload.last_login\n",
        )
        .unwrap(),
    );
    m
}

fn doc_mappings() -> HashMap<String, DocumentMapping> {
    let mut m = HashMap::new();
    m.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            "event: UserCreated\ncollection: users\nversion: 1\nid: payload.id\ndocument:\n  id: payload.id\n  display_name: payload.display_name\n",
        )
        .unwrap(),
    );
    m.insert(
        "UserEmailChanged".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            "event: UserEmailChanged\ncollection: users\nversion: 1\nid: payload.id\nfacet: contact\ndocument:\n  email: payload.email\n",
        )
        .unwrap(),
    );
    m.insert(
        "UserLoggedIn".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            "event: UserLoggedIn\ncollection: users\nversion: 1\nid: payload.id\nfacet: login\ndocument:\n  last_login: payload.last_login\n",
        )
        .unwrap(),
    );
    m
}

#[test]
fn sql_and_document_facets_converge() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute(
            "CREATE TABLE users (id TEXT PRIMARY KEY, display_name TEXT, email TEXT, last_login TEXT)",
            [],
        )?;
    }
    let sql = prepared(SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(sql_mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ));
    let doc_root = dir.path().join("docs");
    let doc = FileDocumentAdapter::new(
        "file".to_string(),
        doc_root.clone(),
        20,
        DocumentRuntimeBuilder::new(doc_mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    for e in stream() {
        let rs = sql.handle(&e);
        assert!(
            !matches!(rs.outcome, kulyrax_core::adapter::AdapterOutcome::Failed(_)),
            "sql {:?}",
            rs.outcome
        );
        let rd = doc.handle(&e);
        assert!(
            !matches!(rd.outcome, kulyrax_core::adapter::AdapterOutcome::Failed(_)),
            "doc {:?}",
            rd.outcome
        );
    }

    let conn = Connection::open(&db_path)?;
    let (name, email, login): (String, String, String) = conn.query_row(
        "SELECT display_name, email, last_login FROM users WHERE id = 'u1'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;

    let doc_json: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        doc_root.join("users").join("u1.json"),
    )?)?;

    assert_eq!(name, "Ada");
    assert_eq!(email, "ada@x.com");
    assert_eq!(login, "2026-01-02");
    assert_eq!(doc_json["display_name"], name);
    assert_eq!(doc_json["email"], email);
    assert_eq!(doc_json["last_login"], login);

    Ok(())
}
