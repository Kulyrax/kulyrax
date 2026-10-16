//! Phase 19.6 (moved to this crate in Phase 28): Postgres backend integration
//! scenarios.
//!
//! **Gated on `KULYRAX_PG_URL`** (`postgres://user:pass@host/db`). When it is
//! unset every test prints a skip line and returns `Ok` — the hermetic SQLite
//! suite stays the default and CI without a database stays green. Point it at a
//! throwaway Postgres (`docker run --rm -e POSTGRES_PASSWORD=pw -p 5432:5432
//! postgres:16`) to actually exercise these.
//!
//! Consolidated into one file (rather than the seven the phase doc sketches)
//! because every scenario shares the same skip guard and fresh-table helper.

use kulyrax_adapter_postgres::PostgresAdapter;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

type R = Result<(), Box<dyn std::error::Error>>;

fn pg_url() -> Option<String> {
    std::env::var("KULYRAX_PG_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

macro_rules! require_pg {
    ($url:ident) => {
        let Some($url) = pg_url() else {
            eprintln!("SKIP: KULYRAX_PG_URL not set");
            return Ok(());
        };
    };
}

static TABLE_SEQ: AtomicU32 = AtomicU32::new(0);

/// A unique table name per test invocation, so the shared
/// `conduit_projection_state` (keyed by target_table) stays isolated.
fn fresh_table() -> String {
    format!("t_{}", TABLE_SEQ.fetch_add(1, Ordering::Relaxed))
}

fn pg_exec(url: &str, sql: &str) -> R {
    let mut c = postgres::Client::connect(url, postgres::NoTls)?;
    c.batch_execute(sql)?;
    Ok(())
}

fn pg_adapter(url: &str, mappings: HashMap<String, SqlMapping>) -> PostgresAdapter {
    PostgresAdapter::new(
        "pg".into(),
        url,
        2,
        10,
        SqlRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn upsert_mapping(event: &str, table: &str, cols: &str) -> SqlMapping {
    serde_yaml::from_str(&format!(
        "event: {event}\ntable: {table}\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n{cols}"
    ))
    .unwrap()
}

fn ev(event_type: &str, seq: u64, payload: serde_json::Value) -> Event {
    Event {
        event_id: format!("evt-{event_type}-{seq}"),
        event_type: event_type.into(),
        payload: payload.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

// ---------------------------------------------------------------------------

#[test]
fn pg_upsert_out_of_order() -> R {
    require_pg!(url);
    let t = fresh_table();
    pg_exec(
        &url,
        &format!("DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id text primary key, name text)"),
    )?;
    pg_exec(
        &url,
        &format!("DELETE FROM conduit_projection_state WHERE target_table = '{t}'"),
    )
    .ok();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        upsert_mapping("U", &t, "  id: payload.id\n  name: payload.name\n"),
    );
    let a = pg_adapter(&url, m);

    for seq in [3u64, 1, 2] {
        let r = a.handle(&ev(
            "U",
            seq,
            serde_json::json!({ "id": "u1", "name": format!("s{seq}") }),
        ));
        assert!(
            !matches!(r.outcome, AdapterOutcome::Failed(_)),
            "{:?}",
            r.outcome
        );
    }

    let mut c = postgres::Client::connect(&url, postgres::NoTls)?;
    let name: String = c
        .query_one(&format!("SELECT name FROM {t} WHERE id = 'u1'"), &[])?
        .get(0);
    assert_eq!(
        name, "s3",
        "highest sequence wins regardless of arrival order"
    );
    Ok(())
}

#[test]
fn pg_delete_lifecycle() -> R {
    require_pg!(url);
    let t = fresh_table();
    pg_exec(
        &url,
        &format!("DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id text primary key, name text)"),
    )?;
    pg_exec(
        &url,
        &format!("DELETE FROM conduit_projection_state WHERE target_table = '{t}'"),
    )
    .ok();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        upsert_mapping("U", &t, "  id: payload.id\n  name: payload.name\n"),
    );
    m.insert(
        "D".into(),
        serde_yaml::from_str(&format!(
            "event: D\ntable: {t}\nprimary_key: id\nversion: 1\noperation: delete\ncolumns:\n  id: payload.id\n"
        ))
        .unwrap(),
    );
    let a = pg_adapter(&url, m);

    a.handle(&ev("U", 1, serde_json::json!({ "id": "u1", "name": "a" })));
    a.handle(&ev("U", 2, serde_json::json!({ "id": "u1", "name": "b" })));
    let d = a.handle(&ev("D", 3, serde_json::json!({ "id": "u1" })));
    assert!(
        matches!(d.outcome, AdapterOutcome::Deleted),
        "{:?}",
        d.outcome
    );

    let mut c = postgres::Client::connect(&url, postgres::NoTls)?;
    let n: i64 = c
        .query_one(&format!("SELECT count(*) FROM {t}"), &[])?
        .get(0);
    assert_eq!(n, 0);
    let (seq, deleted): (i64, bool) = {
        let row = c.query_one(
            "SELECT last_sequence, deleted FROM conduit_projection_state WHERE target_table = $1 AND facet = ''",
            &[&t],
        )?;
        (row.get(0), row.get(1))
    };
    assert_eq!(seq, 3);
    assert!(deleted);
    Ok(())
}

#[test]
fn pg_facet_disjoint() -> R {
    require_pg!(url);
    let t = fresh_table();
    pg_exec(
        &url,
        &format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id text primary key, display_name text, email text, last_login text)"
        ),
    )?;
    pg_exec(
        &url,
        &format!("DELETE FROM conduit_projection_state WHERE target_table = '{t}'"),
    )
    .ok();

    let mut m = HashMap::new();
    m.insert("C".into(), serde_yaml::from_str(&format!(
        "event: C\ntable: {t}\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n  display_name: payload.display_name\n"
    )).unwrap());
    m.insert("Email".into(), serde_yaml::from_str(&format!(
        "event: Email\ntable: {t}\nprimary_key: id\nversion: 1\nfacet: contact\ncolumns:\n  id: payload.id\n  email: payload.email\n"
    )).unwrap());
    m.insert("Login".into(), serde_yaml::from_str(&format!(
        "event: Login\ntable: {t}\nprimary_key: id\nversion: 1\nfacet: login\ncolumns:\n  id: payload.id\n  last_login: payload.last_login\n"
    )).unwrap());
    let a = pg_adapter(&url, m);

    a.handle(&ev(
        "C",
        1,
        serde_json::json!({ "id": "u1", "display_name": "Ada" }),
    ));
    a.handle(&ev(
        "Login",
        6,
        serde_json::json!({ "id": "u1", "last_login": "2026-01-02" }),
    ));
    a.handle(&ev(
        "Email",
        5,
        serde_json::json!({ "id": "u1", "email": "ada@x.com" }),
    ));

    let mut c = postgres::Client::connect(&url, postgres::NoTls)?;
    let row = c.query_one(
        &format!("SELECT display_name, email, last_login FROM {t} WHERE id = 'u1'"),
        &[],
    )?;
    assert_eq!(row.get::<_, String>(0), "Ada");
    assert_eq!(row.get::<_, String>(1), "ada@x.com");
    assert_eq!(row.get::<_, String>(2), "2026-01-02");
    Ok(())
}

#[test]
fn pg_column_missing_rejected() -> R {
    require_pg!(url);
    let t = fresh_table();
    pg_exec(
        &url,
        &format!("DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id text primary key)"),
    )?;
    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        upsert_mapping("U", &t, "  id: payload.id\n  ghost: payload.ghost\n"),
    );
    let a = pg_adapter(&url, m);
    let r = a.handle(&ev("U", 1, serde_json::json!({ "id": "u1", "ghost": "x" })));
    assert!(
        matches!(r.outcome, AdapterOutcome::Failed(_)),
        "{:?}",
        r.outcome
    );
    Ok(())
}

#[test]
fn pg_type_coercion() -> R {
    require_pg!(url);
    let t = fresh_table();
    pg_exec(
        &url,
        &format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id text primary key, n bigint, active boolean, seen timestamptz)"
        ),
    )?;
    pg_exec(
        &url,
        &format!("DELETE FROM conduit_projection_state WHERE target_table = '{t}'"),
    )
    .ok();
    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        upsert_mapping(
            "U",
            &t,
            "  id: payload.id\n  n: payload.n\n  active: payload.active\n  seen: payload.seen\n",
        ),
    );
    let a = pg_adapter(&url, m);
    let r = a.handle(&ev(
        "U",
        1,
        serde_json::json!({
            "id": "u1", "n": 42, "active": true, "seen": "2026-01-02T03:04:05Z"
        }),
    ));
    assert!(
        matches!(r.outcome, AdapterOutcome::Created),
        "{:?}",
        r.outcome
    );

    let mut c = postgres::Client::connect(&url, postgres::NoTls)?;
    let row = c.query_one(&format!("SELECT n, active FROM {t} WHERE id = 'u1'"), &[])?;
    assert_eq!(row.get::<_, i64>(0), 42);
    assert!(row.get::<_, bool>(1));
    Ok(())
}

#[test]
fn sql_cross_backend() -> R {
    require_pg!(url);
    let t = fresh_table();
    pg_exec(
        &url,
        &format!("DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id text primary key, name text)"),
    )?;
    pg_exec(
        &url,
        &format!("DELETE FROM conduit_projection_state WHERE target_table = '{t}'"),
    )
    .ok();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        upsert_mapping("U", &t, "  id: payload.id\n  name: payload.name\n"),
    );

    // SQLite
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("x.db");
    {
        let c = rusqlite::Connection::open(&db)?;
        c.execute(
            &format!("CREATE TABLE {t} (id TEXT PRIMARY KEY, name TEXT)"),
            [],
        )?;
    }
    let sqlite = SqliteAdapter::new(
        "sqlite".into(),
        db.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(m.clone()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );
    let pg = pg_adapter(&url, m);

    for (seq, name) in [(1u64, "a"), (3, "c"), (2, "b")] {
        let e = ev("U", seq, serde_json::json!({ "id": "u1", "name": name }));
        sqlite.handle(&e);
        pg.handle(&e);
    }

    let sqlite_name: String = rusqlite::Connection::open(&db)?.query_row(
        &format!("SELECT name FROM {t} WHERE id = 'u1'"),
        [],
        |r| r.get(0),
    )?;
    let pg_name: String = postgres::Client::connect(&url, postgres::NoTls)?
        .query_one(&format!("SELECT name FROM {t} WHERE id = 'u1'"), &[])?
        .get(0);
    assert_eq!(sqlite_name, "c");
    assert_eq!(
        pg_name, sqlite_name,
        "both backends converge to the seq-3 value"
    );
    Ok(())
}

#[test]
fn pg_concurrent_writers() -> R {
    require_pg!(url);
    let t = fresh_table();
    pg_exec(
        &url,
        &format!("DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id text primary key, name text)"),
    )?;
    pg_exec(
        &url,
        &format!("DELETE FROM conduit_projection_state WHERE target_table = '{t}'"),
    )
    .ok();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        upsert_mapping("U", &t, "  id: payload.id\n  name: payload.name\n"),
    );
    let a = Arc::new(pg_adapter(&url, m));
    // Seed at seq 4: the racing seq-5 event is a valid Update in either
    // ordering, the racing seq-3 event is stale in either ordering — so the
    // result is deterministic and the FOR UPDATE lock is what prevents a lost
    // write or a raw constraint error.
    a.handle(&ev(
        "U",
        4,
        serde_json::json!({ "id": "u1", "name": "seed" }),
    ));

    let a2 = Arc::clone(&a);
    let h1 = std::thread::spawn(move || {
        a.handle(&ev(
            "U",
            5,
            serde_json::json!({ "id": "u1", "name": "five" }),
        ))
        .outcome
    });
    let h2 = std::thread::spawn(move || {
        a2.handle(&ev(
            "U",
            3,
            serde_json::json!({ "id": "u1", "name": "three" }),
        ))
        .outcome
    });
    let o1 = h1.join().unwrap();
    let o2 = h2.join().unwrap();

    let outcomes = [&o1, &o2];
    let updated = outcomes
        .iter()
        .filter(|o| matches!(o, AdapterOutcome::Updated))
        .count();
    let stale = outcomes
        .iter()
        .filter(|o| matches!(o, AdapterOutcome::Skipped(SkipReason::StaleSequence)))
        .count();
    assert_eq!(updated, 1, "{o1:?} {o2:?}");
    assert_eq!(stale, 1, "{o1:?} {o2:?}");

    let name: String = postgres::Client::connect(&url, postgres::NoTls)?
        .query_one(&format!("SELECT name FROM {t} WHERE id = 'u1'"), &[])?
        .get(0);
    assert_eq!(
        name, "five",
        "the higher sequence's write is the one that lands"
    );
    Ok(())
}
