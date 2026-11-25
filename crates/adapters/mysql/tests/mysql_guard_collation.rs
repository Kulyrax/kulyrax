//! `prepare` converts a guard table created with case-insensitive keys (as
//! versions before 0.11 did, under the server's default collation) to binary
//! keys, keeping its rows. Afterwards `A` and `a` are two guard lanes.
//!
//! **Gated on `KULYRAX_MYSQL_URL`**, like `mysql_backend.rs`. Its own test
//! file, so its own process: it drops and recreates the shared
//! `kulyrax_projection_state`, which would break other tests running
//! alongside it.

use kulyrax_adapter_mysql::MySqlAdapter;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::{AdapterOutcome, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use mysql::prelude::Queryable;
use std::collections::HashMap;
use std::sync::Arc;

mod common;

type R = Result<(), Box<dyn std::error::Error>>;

#[test]
fn prepare_makes_old_case_insensitive_guard_keys_binary() -> R {
    let Some(url) = common::mysql_url() else {
        eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
        return Ok(());
    };
    let pool = mysql::Pool::new(url.as_str())?;
    let mut c = pool.get_conn()?;
    let table = "t_guard_collation";
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))?;
    c.query_drop(format!(
        "CREATE TABLE {table} (id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY, name TEXT)"
    ))?;
    // The guard table as 0.10 created it on a MySQL 8 default server, with
    // one existing lane.
    c.query_drop("DROP TABLE IF EXISTS kulyrax_projection_state")?;
    c.query_drop(
        "CREATE TABLE kulyrax_projection_state (
            target_table  VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
            entity_key    VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL,
            facet         VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci NOT NULL DEFAULT '',
            last_sequence BIGINT       NOT NULL,
            last_event_id VARCHAR(191) NOT NULL,
            deleted       BOOLEAN      NOT NULL DEFAULT FALSE,
            permanent     BOOLEAN      NOT NULL DEFAULT FALSE,
            processed_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (target_table, entity_key, facet)
        )",
    )?;
    c.exec_drop(
        "INSERT INTO kulyrax_projection_state (target_table, entity_key, last_sequence, last_event_id)
         VALUES (?, 'A', 7, 'evt-old')",
        (table,),
    )?;

    let mapping: SqlMapping = kulyrax_core::yaml::from_str(&format!(
        "event: Up\ntable: {table}\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  name: payload.name\n"
    ))?;
    let adapter = MySqlAdapter::new(
        "mysql".into(),
        &url,
        2,
        10,
        SqlRuntimeBuilder::new(HashMap::from([("Up".to_string(), mapping)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );
    adapter.prepare().map_err(|e| e.to_string())?;

    let collations: Vec<(String, String)> = c.query(
        "SELECT COLUMN_NAME, COLLATION_NAME FROM information_schema.COLUMNS
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kulyrax_projection_state'
           AND COLUMN_NAME IN ('target_table', 'entity_key', 'facet') ORDER BY 1",
    )?;
    assert_eq!(collations.len(), 3, "{collations:?}");
    assert!(
        collations.iter().all(|(_, c)| c == "utf8mb4_bin"),
        "{collations:?}"
    );
    let kept: Option<i64> = c.exec_first(
        "SELECT last_sequence FROM kulyrax_projection_state
         WHERE target_table = ? AND entity_key = 'A'",
        (table,),
    )?;
    assert_eq!(kept, Some(7), "the existing lane survives");

    // `a` is its own entity now: an event at sequence 1 is applied, where
    // the old table matched `A`'s lane at 7 and skipped it.
    let event = Event {
        event_id: "evt-a-1".into(),
        event_type: "Up".into(),
        payload: serde_json::json!({ "id": "a", "name": "lower" }).to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    };
    let r = adapter.handle(&event);
    assert!(
        matches!(r.outcome, AdapterOutcome::Created),
        "{:?}",
        r.outcome
    );

    // Running prepare again leaves the table as it is.
    adapter.prepare().map_err(|e| e.to_string())?;
    c.query_drop(format!("DROP TABLE IF EXISTS {table}"))?;
    Ok(())
}
