//! A Postgres database last used by Kulyrax 0.12 (guard table
//! `conduit_projection_state`) opens under this version: renamed in place
//! once, every guard lane kept, a tombstone view left under the old name.
//!
//! **Gated on `KULYRAX_PG_URL`.** Each test runs in a schema of its own
//! (`search_path`), so renaming the guard table never touches another test's.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use kulyrax_adapter_postgres::PostgresAdapter;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

type R = Result<(), Box<dyn std::error::Error>>;

static SEQ: AtomicU32 = AtomicU32::new(0);

/// A fresh schema and a URL whose connections use it.
fn schema(base: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let name = format!(
        "kx_rename_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let mut c = postgres::Client::connect(base, postgres::NoTls)?;
    c.batch_execute(&format!(
        "DROP SCHEMA IF EXISTS {name} CASCADE; CREATE SCHEMA {name}"
    ))?;
    // Fail loudly on leftovers: the fresh schema holds nothing.
    let objects: i64 = c
        .query_one(
            "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE n.nspname = $1",
            &[&name],
        )?
        .get(0);
    assert_eq!(objects, 0, "the fresh test schema {name} is not empty");
    c.batch_execute(&format!(
        "CREATE TABLE {name}.users (id text PRIMARY KEY, v bigint)"
    ))?;
    let sep = if base.contains('?') { '&' } else { '?' };
    Ok((
        name.clone(),
        format!("{base}{sep}options=-c%20search_path%3D{name}"),
    ))
}

fn exec(url: &str, sql: &str) -> R {
    postgres::Client::connect(url, postgres::NoTls)?.batch_execute(sql)?;
    Ok(())
}

fn relkinds(url: &str, schema: &str) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let mut c = postgres::Client::connect(url, postgres::NoTls)?;
    Ok(c.query(
        "SELECT c.relname::text, c.relkind::text FROM pg_class c
         JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = $1 AND c.relname LIKE '%projection_state%' ORDER BY 1",
        &[&schema],
    )?
    .iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect())
}

fn adapter(url: &str) -> PostgresAdapter {
    let m: SqlMapping = serde_yaml::from_str(
        "event: U\ntable: users\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  v: payload.v\n",
    )
    .unwrap();
    PostgresAdapter::new(
        "pg".into(),
        url,
        2,
        10,
        SqlRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn ev(id: &str, seq: u64) -> Event {
    Event {
        event_id: format!("e-{id}-{seq}"),
        event_type: "U".into(),
        payload: serde_json::json!({ "id": id, "v": seq }).to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn stale(o: &AdapterOutcome) -> bool {
    matches!(o, AdapterOutcome::Skipped(SkipReason::StaleSequence))
}

/// Lanes written by this version, then put back under the 0.12 names.
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
        "ALTER TABLE kulyrax_projection_state RENAME TO conduit_projection_state;
         ALTER INDEX kulyrax_projection_state_pkey RENAME TO conduit_projection_state_pkey",
    )
}

#[test]
fn a_0_12_1_postgres_database_opens_with_every_lane() -> R {
    let Ok(base) = std::env::var("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return Ok(());
    };
    let (schema, url) = schema(&base)?;
    as_0_12_1(&url)?;

    let a = adapter(&url);
    a.prepare()?;
    assert!(stale(&a.handle(&ev("u1", 2)).outcome));
    assert!(stale(&a.handle(&ev("u2", 1)).outcome));
    assert!(matches!(
        a.handle(&ev("u1", 9)).outcome,
        AdapterOutcome::Updated
    ));
    let objects = relkinds(&url, &schema)?;
    assert_eq!(
        objects,
        vec![
            ("conduit_projection_state".to_string(), "v".to_string()),
            ("kulyrax_projection_state".to_string(), "r".to_string()),
            ("kulyrax_projection_state_pkey".to_string(), "i".to_string()),
        ]
    );
    // The tombstone refuses a 0.12-shaped guard write.
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
    assert_eq!(relkinds(&url, &schema)?, objects);
    exec(&base, &format!("DROP SCHEMA {schema} CASCADE"))
}

#[test]
fn both_postgres_guard_tables_are_refused() -> R {
    let Ok(base) = std::env::var("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return Ok(());
    };
    let (schema, url) = schema(&base)?;
    as_0_12_1(&url)?;
    exec(&url, "CREATE TABLE kulyrax_projection_state (x text)")?;
    let err = adapter(&url).prepare().err().map(|e| e.to_string());
    assert!(
        err.as_deref()
            .is_some_and(|e| e.contains("conduit_projection_state")
                && e.contains("kulyrax_projection_state")),
        "{err:?}"
    );
    exec(&base, &format!("DROP SCHEMA {schema} CASCADE"))
}

/// Several adapters (or processes) start on one 0.12 database at once: the
/// advisory lock lets one migrate, the others find it migrated.
#[test]
fn concurrent_starts_all_migrate() -> R {
    let Ok(base) = std::env::var("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return Ok(());
    };
    let (schema, url) = schema(&base)?;
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
    exec(&base, &format!("DROP SCHEMA {schema} CASCADE"))
}
