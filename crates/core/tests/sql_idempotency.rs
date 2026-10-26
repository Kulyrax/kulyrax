mod common;

use common::prepared;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
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

// ------------------------------------------------------------
// Test
// ------------------------------------------------------------

#[test]
fn sql_adapter_is_idempotent() -> Result<(), Box<dyn std::error::Error>> {
    // Temp directory for SQLite DB
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");

    // Create schema (explicit, no magic)
    {
        let conn = Connection::open(&db_path)?;
        conn.execute(
            "CREATE TABLE users (
                id TEXT PRIMARY KEY,
                email TEXT
            )",
            [],
        )?;
        // conduit_projection_state (Phase 11.2 guard) is self-managed by the
        // adapter via CREATE TABLE IF NOT EXISTS; nothing to set up here.
    }

    // Minimal SQL mapping
    let mut sql_mappings = HashMap::new();
    sql_mappings.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str(
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

    let builder = SqlRuntimeBuilder::new(sql_mappings);

    let sql_adapter = prepared(SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        builder,
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ));

    let event = test_event();

    // --------------------------------------------------
    // Call adapter DIRECTLY (no routing, no dispatch)
    // --------------------------------------------------

    let r1 = sql_adapter.handle(&event);
    assert!(r1.is_success());

    let r2 = sql_adapter.handle(&event);
    assert!(r2.is_success());

    // Verify only one row exists
    let conn = Connection::open(&db_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;

    assert_eq!(count, 1, "duplicate row inserted");

    Ok(())
}
