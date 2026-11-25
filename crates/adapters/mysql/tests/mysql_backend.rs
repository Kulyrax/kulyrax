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

mod common;
use common::mysql_url;

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
/// `kulyrax_projection_state` (keyed by target_table) stays isolated.
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
    prepared(MySqlAdapter::new(
        "mysql".into(),
        url,
        2,
        10,
        SqlRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ))
}

/// Prepare an adapter the way every write path does before projecting.
fn prepared<A: StorageAdapter>(adapter: A) -> A {
    adapter.prepare().expect("prepare adapter");
    adapter
}

fn upsert_mapping(event: &str, table: &str, cols: &str) -> SqlMapping {
    kulyrax_core::yaml::from_str(&format!(
        "event: {event}\ntable: {table}\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n{cols}"
    ))
    .unwrap()
}

fn ev(event_type: &str, seq: u64, payload: serde_json::Value) -> Event {
    Event::new(
        format!("evt-{event_type}-{seq}"),
        event_type,
        seq,
        payload.to_string(),
    )
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
        &format!("DELETE FROM kulyrax_projection_state WHERE target_table = '{t}'"),
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
        &format!("DELETE FROM kulyrax_projection_state WHERE target_table = '{t}'"),
    )
    .ok();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        upsert_mapping("U", &t, "  id: payload.id\n  name: payload.name\n"),
    );
    m.insert(
        "D".into(),
        kulyrax_core::yaml::from_str(&format!(
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
            "SELECT last_sequence, deleted FROM kulyrax_projection_state WHERE target_table = ? AND facet = ''",
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
        &format!("DELETE FROM kulyrax_projection_state WHERE target_table = '{t}'"),
    )
    .ok();

    let mut m = HashMap::new();
    m.insert("C".into(), kulyrax_core::yaml::from_str(&format!(
        "event: C\ntable: {t}\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n  display_name: payload.display_name\n"
    )).unwrap());
    m.insert("Email".into(), kulyrax_core::yaml::from_str(&format!(
        "event: Email\ntable: {t}\nprimary_key: id\nversion: 1\nfacet: contact\ncolumns:\n  id: payload.id\n  email: payload.email\n"
    )).unwrap());
    m.insert("Login".into(), kulyrax_core::yaml::from_str(&format!(
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
        &format!("DELETE FROM kulyrax_projection_state WHERE target_table = '{t}'"),
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
        &format!("DELETE FROM kulyrax_projection_state WHERE target_table = '{t}'"),
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
    let sqlite = prepared(SqliteAdapter::new(
        "sqlite".into(),
        db.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(m.clone()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ));
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
        &format!("DELETE FROM kulyrax_projection_state WHERE target_table = '{t}'"),
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

#[test]
fn mysql_prepare_twice_is_idempotent() -> R {
    require_mysql!(url);
    let a = my_adapter(&url, HashMap::new()); // prepared once by the helper
    a.prepare().map_err(|e| e.to_string())?;
    let pool = mysql::Pool::new(url.as_str())?;
    let mut conn = pool.get_conn()?;
    let tables: Option<i64> = conn.exec_first(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = 'kulyrax_projection_state'",
        (),
    )?;
    assert_eq!(tables, Some(1));
    Ok(())
}

/// The adapter's guard read takes no gap lock, so a new entity's guard row
/// cannot deadlock with another writer inserting into the same gap of the
/// shared `kulyrax_projection_state` (ERROR 1213). Under `REPEATABLE READ`,
/// `SELECT … FOR UPDATE` on a guard row that did not exist yet locked the gap
/// where it would go; two writers doing that for neighbouring keys, then
/// inserting, deadlocked even on different target tables.
///
/// Deterministic: a raw connection (`other`, `REPEATABLE READ`) holds that gap
/// as another writer's guard read would, the adapter projects a new entity
/// into the same gap (its guard insert waits for `other`), then `other`
/// inserts into the gap. Both must succeed.
#[test]
fn mysql_new_guard_rows_take_no_gap_lock() -> R {
    require_mysql!(url);
    let t = fresh_table();
    my_exec(
        &url,
        &format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id VARCHAR(64) PRIMARY KEY, name VARCHAR(64))"
        ),
    )?;
    let mut m = HashMap::new();
    m.insert(
        "Gap".into(),
        upsert_mapping("Gap", &t, "  id: payload.id\n  name: payload.name\n"),
    );
    let a = my_adapter(&url, m); // prepared: the guard table exists
    my_exec(
        &url,
        &format!("DELETE FROM kulyrax_projection_state WHERE target_table = '{t}'"),
    )?;

    let pool = mysql::Pool::new(url.as_str())?;
    let mut other = pool.get_conn()?;
    other.query_drop("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")?;
    other.query_drop("START TRANSACTION")?;
    other.query_drop(format!(
        "SELECT * FROM kulyrax_projection_state
         WHERE target_table = '{t}' AND entity_key = '[\"k1\"]' AND facet = '' FOR UPDATE"
    ))?;

    let projecting = std::thread::spawn(move || {
        a.handle(&ev(
            "Gap",
            1,
            serde_json::json!({ "id": "k2", "name": "x" }),
        ))
        .outcome
    });
    // Let the adapter reach its guard insert, which waits for `other`.
    std::thread::sleep(std::time::Duration::from_millis(500));
    let inserted = other.query_drop(format!(
        "INSERT INTO kulyrax_projection_state
            (target_table, entity_key, facet, last_sequence, last_event_id, deleted, permanent, processed_at)
         VALUES ('{t}', '[\"k1\"]', '', 1, 'other', 0, 0, NOW())"
    ));
    let committed = other.query_drop("COMMIT");
    let outcome = projecting.join().expect("adapter thread");

    assert!(inserted.is_ok(), "the other writer's insert: {inserted:?}");
    committed?;
    assert!(
        matches!(outcome, AdapterOutcome::Created),
        "the adapter's write: {outcome:?}"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// One transaction per batch
// ---------------------------------------------------------------------------

/// Upsert, delete and facet mappings on `table`, under event types
/// `{prefix}Up` / `{prefix}Del` / `{prefix}Mail`.
fn batch_mappings(prefix: &str, table: &str) -> HashMap<String, SqlMapping> {
    let yaml = |s: String| kulyrax_core::yaml::from_str::<SqlMapping>(&s).unwrap();
    HashMap::from([
        (
            format!("{prefix}Up"),
            yaml(format!(
                "event: {prefix}Up\ntable: {table}\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  name: payload.name\n"
            )),
        ),
        (
            format!("{prefix}Del"),
            yaml(format!(
                "event: {prefix}Del\ntable: {table}\nprimary_key: id\nversion: 1\noperation: delete\ncolumns:\n  id: payload.id\n"
            )),
        ),
        (
            format!("{prefix}Mail"),
            yaml(format!(
                "event: {prefix}Mail\ntable: {table}\nprimary_key: id\nversion: 1\nfacet: contact\ncolumns:\n  id: payload.id\n  email: payload.email\n"
            )),
        ),
    ])
}

/// One entity's lifecycle, a second entity deleted before it exists, and two
/// keys that differ only in case, all meant for one batch.
fn batch_stream(prefix: &str) -> Vec<Event> {
    let e = |kind: &str, seq: u64, payload: serde_json::Value| {
        let event_id = format!(
            "evt-{prefix}{kind}-{}-{seq}",
            payload["id"].as_str().unwrap_or("")
        );
        let mut o = ev(&format!("{prefix}{kind}"), seq, payload);
        o.event_id = event_id;
        o
    };
    use serde_json::json;
    vec![
        e("Up", 1, json!({ "id": "u1", "name": "a" })),
        e("Mail", 2, json!({ "id": "u1", "email": "a@x" })),
        e("Up", 3, json!({ "id": "u1", "name": "b" })),
        e("Del", 4, json!({ "id": "u1" })),
        e("Mail", 5, json!({ "id": "u1", "email": "gone@x" })),
        e("Up", 6, json!({ "id": "u1", "name": "c" })),
        e("Mail", 7, json!({ "id": "u1", "email": "c@x" })),
        e("Up", 3, json!({ "id": "u1", "name": "b" })),
        e("Del", 1, json!({ "id": "u2" })),
        e("Up", 1, json!({ "id": "u2", "name": "stale" })),
        e("Up", 2, json!({ "id": "u2", "name": "g" })),
        e("Up", 5, json!({ "id": "K", "name": "upper" })),
        e("Up", 1, json!({ "id": "k", "name": "lower" })),
    ]
}

type MyState = (
    Vec<(String, Option<String>, Option<String>)>,
    Vec<(String, String, i64, bool, bool)>,
);

fn my_state(url: &str, table: &str) -> Result<MyState, Box<dyn std::error::Error>> {
    let pool = mysql::Pool::new(url)?;
    let mut c = pool.get_conn()?;
    let rows = c.query(format!(
        "SELECT id, name, email FROM {table} ORDER BY id COLLATE utf8mb4_bin"
    ))?;
    let guards = c.exec(
        "SELECT entity_key, facet, last_sequence, deleted, permanent
         FROM kulyrax_projection_state WHERE target_table = ? ORDER BY 1, 2",
        (table,),
    )?;
    Ok((rows, guards))
}

/// A target table whose key is case-sensitive, so `K` and `k` are two rows.
fn fresh_batch_table(url: &str) -> Result<String, Box<dyn std::error::Error>> {
    let t = fresh_table();
    my_exec(url, &format!("DROP TABLE IF EXISTS {t}"))?;
    my_exec(
        url,
        &format!(
            "CREATE TABLE {t} (id VARCHAR(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PRIMARY KEY, name TEXT, email TEXT)"
        ),
    )?;
    my_exec(
        url,
        &format!("DELETE FROM kulyrax_projection_state WHERE target_table = '{t}'"),
    )
    .ok();
    Ok(t)
}

#[test]
fn mysql_batch_matches_one_event_at_a_time() -> R {
    require_mysql!(url);
    let (tb, ts) = (fresh_batch_table(&url)?, fresh_batch_table(&url)?);

    let batched = my_adapter(&url, batch_mappings("B", &tb));
    let events = batch_stream("B");
    let refs: Vec<&Event> = events.iter().collect();
    kulyrax_core::execution::stages::take();
    batched.begin_batch(&refs).map_err(|e| e.to_string())?;
    let batch_outcomes: Vec<String> = events
        .iter()
        .map(|e| format!("{:?}", batched.handle(e).outcome))
        .collect();
    batched.commit_batch().map_err(|e| e.to_string())?;
    let t = kulyrax_core::execution::stages::take();
    assert_eq!(t.commit.calls, 1, "one transaction for the batch");

    let single = my_adapter(&url, batch_mappings("S", &ts));
    let single_outcomes: Vec<String> = batch_stream("S")
        .iter()
        .map(|e| format!("{:?}", single.handle(e).outcome))
        .collect();

    assert_eq!(batch_outcomes, single_outcomes);
    assert!(
        !batch_outcomes.iter().any(|o| o.starts_with("Failed")),
        "{batch_outcomes:?}"
    );
    assert_eq!(my_state(&url, &tb)?, my_state(&url, &ts)?);
    let (rows, _) = my_state(&url, &tb)?;
    assert_eq!(
        rows,
        vec![
            ("K".into(), Some("upper".into()), None),
            ("k".into(), Some("lower".into()), None),
            ("u1".into(), Some("c".into()), Some("c@x".into())),
            ("u2".into(), Some("g".into()), None),
        ]
    );
    Ok(())
}

#[test]
fn mysql_batch_rollback_leaves_nothing() -> R {
    require_mysql!(url);
    let t = fresh_batch_table(&url)?;
    let a = my_adapter(&url, batch_mappings("R", &t));
    let events = batch_stream("R");
    let refs: Vec<&Event> = events.iter().collect();

    a.begin_batch(&refs).map_err(|e| e.to_string())?;
    for e in &events {
        assert!(a.handle(e).is_success());
    }
    a.rollback_batch();
    assert_eq!(my_state(&url, &t)?, (vec![], vec![]), "nothing written");

    // Outside a batch, each event commits on its own again.
    let r = a.handle(&events[0]);
    assert!(
        matches!(r.outcome, AdapterOutcome::Created),
        "{:?}",
        r.outcome
    );
    assert_eq!(my_state(&url, &t)?.0.len(), 1);
    Ok(())
}

/// A new adapter over entities an earlier batch wrote reads their lanes back
/// from the database (they are in no cache) and skips the redelivery.
#[test]
fn mysql_batch_redelivery_is_skipped() -> R {
    require_mysql!(url);
    let t = fresh_batch_table(&url)?;
    let events = batch_stream("D");
    let refs: Vec<&Event> = events.iter().collect();
    let first = my_adapter(&url, batch_mappings("D", &t));
    first.begin_batch(&refs).map_err(|e| e.to_string())?;
    for e in &events {
        assert!(first.handle(e).is_success());
    }
    first.commit_batch().map_err(|e| e.to_string())?;
    let before = my_state(&url, &t)?;

    let again = my_adapter(&url, batch_mappings("D", &t));
    again.begin_batch(&refs).map_err(|e| e.to_string())?;
    for e in &events {
        let r = again.handle(e);
        assert!(
            matches!(r.outcome, AdapterOutcome::Skipped(_)),
            "{:?}",
            r.outcome
        );
    }
    again.commit_batch().map_err(|e| e.to_string())?;
    assert_eq!(my_state(&url, &t)?, before);
    Ok(())
}

/// Table and column names are written into SQL unquoted: anything but a
/// plain identifier is refused before any statement runs.
#[test]
fn mysql_names_that_are_not_plain_identifiers_are_refused() -> R {
    require_mysql!(url);
    for (table, column) in [("users; DROP TABLE x", "name"), ("users", "a b")] {
        let mut m = HashMap::new();
        m.insert(
            "U".to_string(),
            upsert_mapping(
                "U",
                table,
                &format!("  id: payload.id\n  '{column}': payload.v\n"),
            ),
        );
        let a = MySqlAdapter::new(
            "mysql".into(),
            &url,
            2,
            10,
            SqlRuntimeBuilder::new(m),
            Arc::new(UpcasterRegistry::new()),
            MigrationPolicy::default(),
        );
        let err = a.prepare().err().map(|e| e.to_string());
        assert!(
            err.as_deref()
                .is_some_and(|e| e.contains("not plain identifiers")),
            "{table:?} {column:?}: {err:?}"
        );
    }
    Ok(())
}
