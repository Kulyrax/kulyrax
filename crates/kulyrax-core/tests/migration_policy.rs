//! Phase 10.3: MigrationPolicy + UpcasterRegistry integration at the adapter level.

use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::adapter::{AdapterError, AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::{Upcaster, UpcasterRegistry};

use rusqlite::Connection;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

// ------------------------------------------------------------
// Helpers
// ------------------------------------------------------------

fn test_event(version: u32) -> Event {
    Event {
        event_id: "evt-1".to_string(),
        event_type: "UserCreated".to_string(),
        payload: r#"{ "id": "u1" }"#.to_string(),
        metadata: HashMap::new(),
        version,
        sequence: 1,
    }
}

/// Mapping targets schema v2 (expects a `tier` column added by the v1->v2 upcaster).
fn sql_mapping_v2() -> SqlMapping {
    serde_yaml::from_str(
        r#"
event: UserCreated
table: users
primary_key: id
version: 2
columns:
  id: payload.id
  tier: payload.tier
"#,
    )
    .unwrap()
}

fn setup_db(db_path: &Path) {
    let conn = Connection::open(db_path).unwrap();
    conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, tier TEXT)", [])
        .unwrap();
    // conduit_projection_state (Phase 11.2 guard) is self-managed by the
    // adapter via CREATE TABLE IF NOT EXISTS; nothing to set up here.
}

struct AddDefaultTier;

impl Upcaster for AddDefaultTier {
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
        obj.insert("tier".to_string(), json!("standard"));
        Ok(Value::Object(obj))
    }
}

fn adapter(db_path: &Path, upcasters: UpcasterRegistry, policy: MigrationPolicy) -> SqliteAdapter {
    let mut mappings = HashMap::new();
    mappings.insert("UserCreated".to_string(), sql_mapping_v2());
    let builder = SqlRuntimeBuilder::new(mappings);
    SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        builder,
        Arc::new(upcasters),
        policy,
    )
}

// ------------------------------------------------------------
// Tests
// ------------------------------------------------------------

#[test]
fn registered_upcaster_projects_event_to_mapping_version() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    setup_db(&db_path);

    let mut registry = UpcasterRegistry::new();
    registry.register(Box::new(AddDefaultTier));

    let adapter = adapter(&db_path, registry, MigrationPolicy::Strict);
    let result = adapter.handle(&test_event(1));

    assert!(result.is_success(), "{:?}", result.outcome);
    assert_eq!(result.source_version, Some(1));
    assert_eq!(result.projected_version, Some(2));

    let conn = Connection::open(&db_path).unwrap();
    let tier: String = conn
        .query_row("SELECT tier FROM users WHERE id = 'u1'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(tier, "standard");
}

#[test]
fn strict_policy_fails_when_no_upcaster_chain_exists() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    setup_db(&db_path);

    // No upcasters registered: v1 -> v2 chain is broken.
    let adapter = adapter(&db_path, UpcasterRegistry::new(), MigrationPolicy::Strict);
    let result = adapter.handle(&test_event(1));

    assert!(!result.is_success());
    assert!(matches!(
        result.outcome,
        AdapterOutcome::Failed(AdapterError::UnsupportedVersion(_))
    ));
    assert_eq!(result.source_version, Some(1));
    assert_eq!(result.projected_version, Some(2));

    let conn = Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
        .unwrap();
    assert_eq!(count, 0, "no row should be written on a strict failure");
}

#[test]
fn ignore_unmatched_policy_skips_instead_of_failing_the_batch() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    setup_db(&db_path);

    let adapter = adapter(
        &db_path,
        UpcasterRegistry::new(),
        MigrationPolicy::IgnoreUnmatched,
    );
    let result = adapter.handle(&test_event(1));

    assert!(result.is_success(), "ignore policy must not fail the batch");
    assert!(matches!(
        result.outcome,
        AdapterOutcome::Skipped(SkipReason::UnsupportedVersion)
    ));
    assert_eq!(result.source_version, Some(1));
    assert_eq!(result.projected_version, Some(2));

    let conn = Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
        .unwrap();
    assert_eq!(
        count, 0,
        "no row should be written when projection is skipped"
    );
}

#[test]
fn matching_versions_skip_upcasting_entirely() {
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    let conn = Connection::open(&db_path).unwrap();
    conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, tier TEXT)", [])
        .unwrap();
    // conduit_projection_state (Phase 11.2 guard) is self-managed by the
    // adapter via CREATE TABLE IF NOT EXISTS; nothing to set up here.
    drop(conn);

    let mut mappings = HashMap::new();
    // Mapping and event both at v1: no upcaster needed, no registry lookup required.
    mappings.insert(
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
        )
        .unwrap(),
    );
    let builder = SqlRuntimeBuilder::new(mappings);
    let adapter = SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        builder,
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::Strict,
    );

    let result = adapter.handle(&test_event(1));

    assert!(result.is_success(), "{:?}", result.outcome);
    assert_eq!(result.source_version, Some(1));
    assert_eq!(result.projected_version, Some(1));
}
