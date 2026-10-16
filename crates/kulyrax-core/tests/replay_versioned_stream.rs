//! Phase 10.4: replaying a mixed-version (V1/V2/V3) event stream projects
//! deterministically, and produces the same target storage state as
//! real-time dispatch through the same upcaster registry.

use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::event::Event;
use kulyrax_core::execute_event_with_upcasters;
use kulyrax_core::replay::{ReplayContext, events_from_path};
use kulyrax_core::runtime::config::{
    AdapterConfig, KulyraxConfig, RoutingConfig, SqliteAdapterConfig, SqliteConfig,
};
use kulyrax_core::upcast::{Upcaster, UpcasterRegistry};

use rusqlite::Connection;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;
use tempfile::tempdir;

// ------------------------------------------------------------
// Upcasters: UserCreated v1 -> v2 -> v3
// ------------------------------------------------------------

struct AddEmail;

impl Upcaster for AddEmail {
    fn event_type(&self) -> &str {
        "UserCreated"
    }
    fn source_version(&self) -> u32 {
        1
    }
    fn upcast(&self, payload: Value) -> Result<Value, String> {
        let mut obj = payload
            .as_object()
            .cloned()
            .ok_or_else(|| "payload must be an object".to_string())?;
        obj.entry("email".to_string())
            .or_insert_with(|| json!("unknown@example.com"));
        Ok(Value::Object(obj))
    }
}

struct AddTier;

impl Upcaster for AddTier {
    fn event_type(&self) -> &str {
        "UserCreated"
    }
    fn source_version(&self) -> u32 {
        2
    }
    fn upcast(&self, payload: Value) -> Result<Value, String> {
        let mut obj = payload
            .as_object()
            .cloned()
            .ok_or_else(|| "payload must be an object".to_string())?;
        obj.entry("tier".to_string())
            .or_insert_with(|| json!("standard"));
        Ok(Value::Object(obj))
    }
}

fn registry() -> UpcasterRegistry {
    let mut r = UpcasterRegistry::new();
    r.register(Box::new(AddEmail));
    r.register(Box::new(AddTier));
    r
}

// ------------------------------------------------------------
// Shared fixtures
// ------------------------------------------------------------

/// Mapping targets schema v3 (id, email, tier columns).
fn sql_mapping_v3() -> SqlMapping {
    serde_yaml::from_str(
        r#"
event: UserCreated
table: users
primary_key: id
version: 3
columns:
  id: payload.id
  email: payload.email
  tier: payload.tier
"#,
    )
    .unwrap()
}

fn init_users_table(db_path: &std::path::Path) {
    let conn = Connection::open(db_path).unwrap();
    conn.execute(
        "CREATE TABLE users (id TEXT PRIMARY KEY, email TEXT, tier TEXT)",
        [],
    )
    .unwrap();
}

/// Three events for the same event type at three different source versions.
/// - v1 needs both upcast steps.
/// - v2 already has `email`; needs one step.
/// - v3 already has both fields; needs none.
fn mixed_version_events() -> Vec<Event> {
    vec![
        Event {
            event_id: "e1".to_string(),
            event_type: "UserCreated".to_string(),
            payload: r#"{"id":"u1"}"#.to_string(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        },
        Event {
            event_id: "e2".to_string(),
            event_type: "UserCreated".to_string(),
            payload: r#"{"id":"u2","email":"u2@x.com"}"#.to_string(),
            metadata: HashMap::new(),
            version: 2,
            sequence: 1,
        },
        Event {
            event_id: "e3".to_string(),
            event_type: "UserCreated".to_string(),
            payload: r#"{"id":"u3","email":"u3@x.com","tier":"gold"}"#.to_string(),
            metadata: HashMap::new(),
            version: 3,
            sequence: 1,
        },
    ]
}

/// Every row in `users`, sorted by id, as (id, email, tier) tuples.
fn read_all_users(db_path: &std::path::Path) -> Vec<(String, String, String)> {
    let conn = Connection::open(db_path).unwrap();
    let mut stmt = conn
        .prepare("SELECT id, email, tier FROM users ORDER BY id")
        .unwrap();
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .map(|row| row.unwrap())
        .collect()
}

const EXPECTED_ROWS: &[(&str, &str, &str)] = &[
    ("u1", "unknown@example.com", "standard"),
    ("u2", "u2@x.com", "standard"),
    ("u3", "u3@x.com", "gold"),
];

fn assert_expected_rows(rows: &[(String, String, String)]) {
    let actual: Vec<(&str, &str, &str)> = rows
        .iter()
        .map(|(a, b, c)| (a.as_str(), b.as_str(), c.as_str()))
        .collect();
    assert_eq!(actual, EXPECTED_ROWS);
}

// ------------------------------------------------------------
// Test
// ------------------------------------------------------------

#[test]
fn replay_and_real_time_dispatch_produce_identical_state_for_mixed_versions() {
    let tmp = tempdir().unwrap();

    // -----------------------------------------------------------------
    // Part 1: replay a directory of mixed-version event files.
    // -----------------------------------------------------------------
    let replay_db = tmp.path().join("replay.db");
    init_users_table(&replay_db);

    let events_dir = tmp.path().join("events");
    fs::create_dir_all(&events_dir).unwrap();
    for (i, event) in mixed_version_events().into_iter().enumerate() {
        fs::write(
            events_dir.join(format!("{:02}.json", i)),
            serde_json::to_string(&event).unwrap(),
        )
        .unwrap();
    }

    let mut sql_mappings = HashMap::new();
    sql_mappings.insert("UserCreated".to_string(), sql_mapping_v3());

    let mut routing_rules = HashMap::new();
    routing_rules.insert("UserCreated".to_string(), vec!["sql-primary".to_string()]);

    let replay_config = KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "routing.json".into(),
        },
        adapters: vec![AdapterConfig::Sqlite(SqliteAdapterConfig {
            id: "sql-primary".into(),
            priority: 10,
            config: SqliteConfig {
                path: replay_db.to_string_lossy().into(),
            },
            capabilities: None,
            depends_on: vec![],
        })],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    };
    replay_config.validate().unwrap();

    let mut ctx = ReplayContext::new_with_upcasters(
        &replay_config,
        routing_rules.clone(),
        sql_mappings.clone(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        Arc::new(registry()),
    );

    let mut iter = events_from_path(&events_dir).unwrap();
    let report = ctx.run_stream(&mut iter).unwrap();

    assert_eq!(report.events_processed, 3, "{:?}", report.per_event);
    assert_eq!(report.events_succeeded, 3);
    assert_eq!(report.events_failed, 0);
    assert!(!report.stopped_early);

    let replay_rows = read_all_users(&replay_db);
    assert_expected_rows(&replay_rows);

    // -----------------------------------------------------------------
    // Part 2: dispatch the same events one at a time in "real time",
    // through the same mappings and a frozen upcaster registry, into a
    // separate database with identical schema.
    // -----------------------------------------------------------------
    let live_db = tmp.path().join("live.db");
    init_users_table(&live_db);

    let live_config = KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "routing.json".into(),
        },
        adapters: vec![AdapterConfig::Sqlite(SqliteAdapterConfig {
            id: "sql-primary".into(),
            priority: 10,
            config: SqliteConfig {
                path: live_db.to_string_lossy().into(),
            },
            capabilities: None,
            depends_on: vec![],
        })],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    };
    live_config.validate().unwrap();

    let shared_upcasters = Arc::new(registry());
    for event in mixed_version_events() {
        let report = execute_event_with_upcasters(
            &live_config,
            sql_mappings.clone(),
            HashMap::new(),
            HashMap::new(),
            HashMap::new(),
            &routing_rules,
            event,
            Arc::clone(&shared_upcasters),
        );
        assert_eq!(
            report.status,
            kulyrax_core::execution::ExecutionStatus::Succeeded,
            "{:?}",
            report
        );
    }

    let live_rows = read_all_users(&live_db);
    assert_expected_rows(&live_rows);

    // -----------------------------------------------------------------
    // The two independently-driven pipelines must agree exactly.
    // -----------------------------------------------------------------
    assert_eq!(
        replay_rows, live_rows,
        "replay must produce identical target storage state to real-time dispatch"
    );
}
