//! A MySQL database last used by Kulyrax 0.12 (guard table
//! `conduit_projection_state`) opens under this version: renamed in place
//! once, every guard lane kept, a tombstone view left under the old name.
//!
//! **Gated on `KULYRAX_MYSQL_URL`.** Each test runs in a database of its own,
//! so renaming the guard table never touches another test's.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use kulyrax_adapter_mysql::MySqlAdapter;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;
use mysql::prelude::Queryable;

type R = Result<(), Box<dyn std::error::Error>>;

static SEQ: AtomicU32 = AtomicU32::new(0);

/// A fresh database and a URL that uses it.
fn database(base: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let name = format!(
        "kx_rename_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    exec(
        base,
        &format!("DROP DATABASE IF EXISTS {name}; CREATE DATABASE {name}"),
    )?;
    let (server, _) = base
        .rsplit_once('/')
        .ok_or("KULYRAX_MYSQL_URL has no database")?;
    let url = format!("{server}/{name}");
    // Fail loudly on leftovers: the fresh database holds nothing.
    let leftovers = table_types(&url)?;
    assert!(
        leftovers.is_empty(),
        "the fresh test database {name} is not empty: {leftovers:?}"
    );
    exec(
        &url,
        "CREATE TABLE users (id VARCHAR(64) PRIMARY KEY, v BIGINT)",
    )?;
    Ok((name, url))
}

fn exec(url: &str, sql: &str) -> R {
    let pool = mysql::Pool::new(url)?;
    let mut conn = pool.get_conn()?;
    conn.query_drop(sql)?;
    Ok(())
}

fn table_types(url: &str) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let pool = mysql::Pool::new(url)?;
    let mut conn = pool.get_conn()?;
    Ok(conn.query(
        "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES
         WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE '%projection_state%'
            OR TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE '%tombstone%'
         ORDER BY TABLE_NAME",
    )?)
}

fn adapter(url: &str) -> MySqlAdapter {
    let m: SqlMapping = kulyrax_core::yaml::from_str(
        "event: U\ntable: users\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  v: payload.v\n",
    )
    .unwrap();
    MySqlAdapter::new(
        "mysql".into(),
        url,
        2,
        10,
        SqlRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn ev(id: &str, seq: u64) -> Event {
    Event::new(
        format!("e-{id}-{seq}"),
        "U",
        seq,
        serde_json::json!({ "id": id, "v": seq }).to_string(),
    )
}

fn stale(o: &AdapterOutcome) -> bool {
    matches!(o, AdapterOutcome::Skipped(SkipReason::StaleSequence))
}

/// Lanes written by this version, then put back under the 0.12 name.
fn as_0_12_1(url: &str) -> R {
    let a = adapter(url);
    a.prepare()?;
    assert!(matches!(
        a.handle(&ev("u1", 5)).outcome,
        AdapterOutcome::Created
    ));
    assert!(matches!(
        a.handle(&ev("u2", 3)).outcome,
        AdapterOutcome::Created
    ));
    drop(a);
    exec(
        url,
        "RENAME TABLE kulyrax_projection_state TO conduit_projection_state",
    )
}

#[test]
fn a_0_12_1_mysql_database_opens_with_every_lane() -> R {
    let Ok(base) = std::env::var("KULYRAX_MYSQL_URL") else {
        eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
        return Ok(());
    };
    let (name, url) = database(&base)?;
    as_0_12_1(&url)?;

    let a = adapter(&url);
    a.prepare()?;
    assert!(stale(&a.handle(&ev("u1", 2)).outcome));
    assert!(stale(&a.handle(&ev("u2", 1)).outcome));
    assert!(matches!(
        a.handle(&ev("u1", 9)).outcome,
        AdapterOutcome::Updated
    ));
    let objects = table_types(&url)?;
    assert_eq!(
        objects,
        vec![
            ("conduit_projection_state".to_string(), "VIEW".to_string()),
            (
                "kulyrax_projection_state".to_string(),
                "BASE TABLE".to_string()
            ),
        ]
    );
    let write = exec(
        &url,
        "INSERT INTO conduit_projection_state (target_table, entity_key, facet, last_sequence, \
         last_event_id) VALUES ('users', '[\"u9\"]', '', 1, 'e')",
    );
    assert!(write.is_err());
    drop(a);

    // A second start is a no-op.
    let a = adapter(&url);
    a.prepare()?;
    assert!(stale(&a.handle(&ev("u1", 3)).outcome));
    assert_eq!(table_types(&url)?, objects);
    exec(&base, &format!("DROP DATABASE {name}"))
}

#[test]
fn both_mysql_guard_tables_are_refused() -> R {
    let Ok(base) = std::env::var("KULYRAX_MYSQL_URL") else {
        eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
        return Ok(());
    };
    let (name, url) = database(&base)?;
    as_0_12_1(&url)?;
    exec(&url, "CREATE TABLE kulyrax_projection_state (x TEXT)")?;
    let err = adapter(&url).prepare().err().map(|e| e.to_string());
    assert!(
        err.as_deref()
            .is_some_and(|e| e.contains("conduit_projection_state")
                && e.contains("kulyrax_projection_state")),
        "{err:?}"
    );
    exec(&base, &format!("DROP DATABASE {name}"))
}

/// A tombstone left half-made (the temporary view, before the rename) is
/// replaced, and the migration completes.
#[test]
fn a_leftover_temporary_tombstone_is_replaced() -> R {
    let Ok(base) = std::env::var("KULYRAX_MYSQL_URL") else {
        eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
        return Ok(());
    };
    let (name, url) = database(&base)?;
    as_0_12_1(&url)?;
    exec(&url, "CREATE VIEW kulyrax_tombstone_tmp AS SELECT 1 AS x")?;
    let a = adapter(&url);
    a.prepare()?;
    assert!(stale(&a.handle(&ev("u1", 2)).outcome));
    let objects = table_types(&url)?;
    assert_eq!(objects.len(), 2, "{objects:?}");
    exec(&base, &format!("DROP DATABASE {name}"))
}

/// Several adapters (or processes) start on one 0.12 database at once: the
/// named lock lets one migrate, the others find it migrated.
#[test]
fn concurrent_starts_all_migrate() -> R {
    let Ok(base) = std::env::var("KULYRAX_MYSQL_URL") else {
        eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
        return Ok(());
    };
    let (name, url) = database(&base)?;
    as_0_12_1(&url)?;
    let results: Vec<Result<(), String>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|_| s.spawn(|| adapter(&url).prepare().map_err(|e| e.to_string())))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err("panicked".into())))
            .collect()
    });
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    let a = adapter(&url);
    a.prepare()?;
    assert!(stale(&a.handle(&ev("u1", 2)).outcome));
    drop(a);
    exec(&base, &format!("DROP DATABASE {name}"))
}
