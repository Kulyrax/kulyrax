//! MySQL backend integration scenarios.
//!
//! **Gated on `KULYRAX_MYSQL_URL`** (`mysql://user:pass@host/db`). When it is
//! unset every test prints a skip line and returns `Ok` — the hermetic SQLite
//! suite stays the default and CI without a database stays green. Point it at
//! a throwaway MySQL (`docker run --rm -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -e
//! MYSQL_DATABASE=kulyrax -p 3306:3306 mysql:8`) to actually exercise these.
//!
//! Mirrors `pg_backend.rs`'s exact skip-guard / fresh-table pattern and the
//! same scenario suite (out-of-order upsert, delete, facets, type coercion,
//! cross-backend convergence, concurrent writers) — the phase doc's stated
//! guarantee that MySQL gets the same coverage as SQLite/Postgres.

use kulyrax_adapter_mysql::MySqlAdapter;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use mysql::prelude::Queryable;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

type R = Result<(), Box<dyn std::error::Error>>;

fn mysql_url() -> Option<String> {
    std::env::var("KULYRAX_MYSQL_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

macro_rules! require_mysql {
    ($url:ident) => {
        let Some($url) = mysql_url() else {
            eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
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

fn my_exec(url: &str, sql: &str) -> R {
    let pool = mysql::Pool::new(url)?;
    let mut conn = pool.get_conn()?;
    conn.query_drop(sql)?;
    Ok(())
}

fn my_adapter(url: &str, mappings: HashMap<String, SqlMapping>) -> MySqlAdapter {
    MySqlAdapter::new(
        "mysql".into(),
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
fn mysql_upsert_out_of_order() -> R {
    require_mysql!(url);
    let t = fresh_table();
    my_exec(
        &url,
        &format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id VARCHAR(64) PRIMARY KEY, name VARCHAR(64))"
        ),
    )?;
    my_exec(
        &url,
        &format!("DELETE FROM conduit_projection_state WHERE target_table = '{t}'"),
    )
    .ok();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        upsert_mapping("U", &t, "  id: payload.id\n  name: payload.name\n"),
    );
    let a = my_adapter(&url, m);

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

    let pool = mysql::Pool::new(url.as_str())?;
    let mut conn = pool.get_conn()?;
    let name: String = conn
        .exec_first(format!("SELECT name FROM {t} WHERE id = 'u1'"), ())?
        .unwrap();
    assert_eq!(
        name, "s3",
        "highest sequence wins regardless of arrival order"
    );
    Ok(())
}

#[test]
fn mysql_delete_lifecycle() -> R {
    require_mysql!(url);
    let t = fresh_table();
    my_exec(
        &url,
        &format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id VARCHAR(64) PRIMARY KEY, name VARCHAR(64))"
        ),
    )?;
    my_exec(
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
    let a = my_adapter(&url, m);

    a.handle(&ev("U", 1, serde_json::json!({ "id": "u1", "name": "a" })));
    a.handle(&ev("U", 2, serde_json::json!({ "id": "u1", "name": "b" })));
    let d = a.handle(&ev("D", 3, serde_json::json!({ "id": "u1" })));
    assert!(
        matches!(d.outcome, AdapterOutcome::Deleted),
        "{:?}",
        d.outcome
    );

    let pool = mysql::Pool::new(url.as_str())?;
    let mut conn = pool.get_conn()?;
    let n: i64 = conn
        .exec_first(format!("SELECT count(*) FROM {t}"), ())?
        .unwrap();
    assert_eq!(n, 0);
    let (seq, deleted): (i64, bool) = conn
        .exec_first(
            "SELECT last_sequence, deleted FROM conduit_projection_state WHERE target_table = ? AND facet = ''",
            (t.clone(),),
        )?
        .unwrap();
    assert_eq!(seq, 3);
    assert!(deleted);
    Ok(())
}

#[test]
fn mysql_facet_disjoint() -> R {
    require_mysql!(url);
    let t = fresh_table();
    my_exec(
        &url,
        &format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id VARCHAR(64) PRIMARY KEY, display_name VARCHAR(64), email VARCHAR(64), last_login VARCHAR(64))"
        ),
    )?;
    my_exec(
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
    let a = my_adapter(&url, m);

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

    let pool = mysql::Pool::new(url.as_str())?;
    let mut conn = pool.get_conn()?;
    let (display_name, email, last_login): (String, String, String) = conn
        .exec_first(
            format!("SELECT display_name, email, last_login FROM {t} WHERE id = 'u1'"),
            (),
        )?
        .unwrap();
    assert_eq!(display_name, "Ada");
    assert_eq!(email, "ada@x.com");
    assert_eq!(last_login, "2026-01-02");
    Ok(())
}

#[test]
fn mysql_column_missing_rejected() -> R {
    require_mysql!(url);
    let t = fresh_table();
    my_exec(
        &url,
        &format!("DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id VARCHAR(64) PRIMARY KEY)"),
    )?;
    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        upsert_mapping("U", &t, "  id: payload.id\n  ghost: payload.ghost\n"),
    );
    let a = my_adapter(&url, m);
    let r = a.handle(&ev("U", 1, serde_json::json!({ "id": "u1", "ghost": "x" })));
    assert!(
        matches!(r.outcome, AdapterOutcome::Failed(_)),
        "{:?}",
        r.outcome
    );
    Ok(())
}

#[test]
fn mysql_type_coercion() -> R {
    require_mysql!(url);
    let t = fresh_table();
    my_exec(
        &url,
        &format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id VARCHAR(64) PRIMARY KEY, n BIGINT, active BOOLEAN, seen DATETIME)"
        ),
    )?;
    my_exec(
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
    let a = my_adapter(&url, m);
    // No RFC 3339 parsing here (unlike Postgres's typed bind) — MySQL's own
    // implicit string-to-DATETIME coercion needs its native
    // `YYYY-MM-DD HH:MM:SS` form, per this backend's module doc comment.
    let r = a.handle(&ev(
        "U",
        1,
        serde_json::json!({
            "id": "u1", "n": 42, "active": true, "seen": "2026-01-02 03:04:05"
        }),
    ));
    assert!(
        matches!(r.outcome, AdapterOutcome::Created),
        "{:?}",
        r.outcome
    );

    let pool = mysql::Pool::new(url.as_str())?;
    let mut conn = pool.get_conn()?;
    let (n, active): (i64, bool) = conn
        .exec_first(format!("SELECT n, active FROM {t} WHERE id = 'u1'"), ())?
        .unwrap();
    assert_eq!(n, 42);
    assert!(active);
    Ok(())
}

#[test]
fn sql_cross_backend_mysql() -> R {
    require_mysql!(url);
    let t = fresh_table();
    my_exec(
        &url,
        &format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id VARCHAR(64) PRIMARY KEY, name VARCHAR(64))"
        ),
    )?;
    my_exec(
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
    let my = my_adapter(&url, m);

    for (seq, name) in [(1u64, "a"), (3, "c"), (2, "b")] {
        let e = ev("U", seq, serde_json::json!({ "id": "u1", "name": name }));
        sqlite.handle(&e);
        my.handle(&e);
    }

    let sqlite_name: String = rusqlite::Connection::open(&db)?.query_row(
        &format!("SELECT name FROM {t} WHERE id = 'u1'"),
        [],
        |r| r.get(0),
    )?;
    let pool = mysql::Pool::new(url.as_str())?;
    let mut conn = pool.get_conn()?;
    let my_name: String = conn
        .exec_first(format!("SELECT name FROM {t} WHERE id = 'u1'"), ())?
        .unwrap();
    assert_eq!(sqlite_name, "c");
    assert_eq!(
        my_name, sqlite_name,
        "both backends converge to the seq-3 value"
    );
    Ok(())
}

#[test]
fn mysql_concurrent_writers() -> R {
    require_mysql!(url);
    let t = fresh_table();
    my_exec(
        &url,
        &format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id VARCHAR(64) PRIMARY KEY, name VARCHAR(64))"
        ),
    )?;
    my_exec(
        &url,
        &format!("DELETE FROM conduit_projection_state WHERE target_table = '{t}'"),
    )
    .ok();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        upsert_mapping("U", &t, "  id: payload.id\n  name: payload.name\n"),
    );
    let a = Arc::new(my_adapter(&url, m));
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

    let pool = mysql::Pool::new(url.as_str())?;
    let mut conn = pool.get_conn()?;
    let name: String = conn
        .exec_first(format!("SELECT name FROM {t} WHERE id = 'u1'"), ())?
        .unwrap();
    assert_eq!(
        name, "five",
        "the higher sequence's write is the one that lands"
    );
    Ok(())
}
