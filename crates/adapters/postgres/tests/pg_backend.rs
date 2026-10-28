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
use kulyrax_core::execution::stages;
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
    prepared(PostgresAdapter::new(
        "pg".into(),
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
    // `fresh_table`'s counter restarts at 0 every process invocation, so
    // against a long-lived (not freshly provisioned) database, a prior run
    // can leave a guard row behind for this same table name — every sibling
    // test in this file clears it first for exactly that reason.
    pg_exec(
        &url,
        &format!("DELETE FROM conduit_projection_state WHERE target_table = '{t}'"),
    )
    .ok();
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
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id text primary key, n bigint, small_n smallint, plain_n integer, active boolean, seen timestamptz)"
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
            "  id: payload.id\n  n: payload.n\n  small_n: payload.small_n\n  plain_n: payload.plain_n\n  active: payload.active\n  seen: payload.seen\n",
        ),
    );
    let a = pg_adapter(&url, m);
    let r = a.handle(&ev(
        "U",
        1,
        serde_json::json!({
            "id": "u1", "n": 42, "small_n": 7, "plain_n": 1234, "active": true, "seen": "2026-01-02T03:04:05Z"
        }),
    ));
    assert!(
        matches!(r.outcome, AdapterOutcome::Created),
        "{:?}",
        r.outcome
    );

    let mut c = postgres::Client::connect(&url, postgres::NoTls)?;
    let row = c.query_one(
        &format!("SELECT n, small_n, plain_n, active FROM {t} WHERE id = 'u1'"),
        &[],
    )?;
    assert_eq!(row.get::<_, i64>(0), 42);
    // `small_n` (smallint/int2) and `plain_n` (integer/int4) — not just
    // `n` (bigint/int8) — must bind correctly too: the wire protocol
    // requires an exact width match, and an ordinary `INTEGER` column (the
    // default width in nearly every real schema) is the one that most
    // needed this covered.
    assert_eq!(row.get::<_, i16>(1), 7);
    assert_eq!(row.get::<_, i32>(2), 1234);
    assert!(row.get::<_, bool>(3));
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
    let sqlite = prepared(SqliteAdapter::new(
        "sqlite".into(),
        db.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(m.clone()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ));
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

/// The guard read, target write and guard write are one transaction: a CHECK
/// constraint rejects one event's guard row after its target write
/// succeeded, and neither row may remain.
#[test]
fn pg_failed_guard_write_leaves_neither_row() -> R {
    require_pg!(url);
    let t = fresh_table();
    pg_exec(
        &url,
        &format!("DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id text PRIMARY KEY, name text)"),
    )?;
    pg_exec(
        &url,
        &format!("DELETE FROM conduit_projection_state WHERE target_table = '{t}'"),
    )
    .ok();

    let mut m = HashMap::new();
    m.insert(
        "Atomic".into(),
        upsert_mapping("Atomic", &t, "  id: payload.id\n  name: payload.name\n"),
    );
    let a = pg_adapter(&url, m);

    // An ordinary write first, for contrast with the forced failure.
    let ok = a.handle(&ev(
        "Atomic",
        1,
        serde_json::json!({ "id": "u1", "name": "a" }),
    ));
    assert!(
        matches!(ok.outcome, AdapterOutcome::Created),
        "{:?}",
        ok.outcome
    );

    let fail = ev("Atomic", 2, serde_json::json!({ "id": "u2", "name": "b" }));
    let check = format!("chk_{}_{t}", std::process::id());
    pg_exec(
        &url,
        &format!(
            "ALTER TABLE conduit_projection_state ADD CONSTRAINT {check} CHECK (last_event_id <> '{}')",
            fail.event_id
        ),
    )?;
    let r = a.handle(&fail);
    let dropped = pg_exec(
        &url,
        &format!("ALTER TABLE conduit_projection_state DROP CONSTRAINT {check}"),
    );

    assert!(
        matches!(r.outcome, AdapterOutcome::Failed(_)),
        "{:?}",
        r.outcome
    );
    let mut c = postgres::Client::connect(&url, postgres::NoTls)?;
    let target = c.query_opt(&format!("SELECT name FROM {t} WHERE id = 'u2'"), &[])?;
    let guards: i64 = c
        .query_one(
            &format!(
                "SELECT count(*) FROM conduit_projection_state WHERE target_table = '{t}' AND entity_key = '[\"u2\"]'"
            ),
            &[],
        )?
        .get(0);
    dropped?;
    assert!(
        target.is_none(),
        "the target row must roll back with the guard write"
    );
    assert_eq!(guards, 0, "no guard row");
    Ok(())
}

#[test]
fn pg_prepare_twice_is_idempotent() -> R {
    require_pg!(url);
    let a = pg_adapter(&url, HashMap::new()); // prepared once by the helper
    a.prepare().map_err(|e| e.to_string())?;
    let mut c = postgres::Client::connect(&url, postgres::NoTls)?;
    let tables: i64 = c
        .query_one(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'conduit_projection_state'",
            &[],
        )?
        .get(0);
    assert_eq!(tables, 1);
    Ok(())
}

// ---------------------------------------------------------------------------
// Phase 2: one transaction per batch
// ---------------------------------------------------------------------------

/// Upsert, delete and facet mappings on `table`, under event types
/// `{prefix}Up` / `{prefix}Del` / `{prefix}Mail`.
fn batch_mappings(prefix: &str, table: &str) -> HashMap<String, SqlMapping> {
    let mut m = HashMap::new();
    let yaml = |s: String| serde_yaml::from_str::<SqlMapping>(&s).unwrap();
    m.insert(
        format!("{prefix}Up"),
        yaml(format!(
            "event: {prefix}Up\ntable: {table}\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  name: payload.name\n"
        )),
    );
    m.insert(
        format!("{prefix}Del"),
        yaml(format!(
            "event: {prefix}Del\ntable: {table}\nprimary_key: id\nversion: 1\noperation: delete\ncolumns:\n  id: payload.id\n"
        )),
    );
    m.insert(
        format!("{prefix}Mail"),
        yaml(format!(
            "event: {prefix}Mail\ntable: {table}\nprimary_key: id\nversion: 1\nfacet: contact\ncolumns:\n  id: payload.id\n  email: payload.email\n"
        )),
    );
    m
}

/// One entity's lifecycle and a second entity deleted before it exists, all
/// meant for one batch.
fn batch_stream(prefix: &str) -> Vec<Event> {
    let e = |kind: &str, seq: u64, payload: serde_json::Value| Event {
        event_id: format!(
            "evt-{prefix}{kind}-{}-{seq}",
            payload["id"].as_str().unwrap_or("")
        ),
        ..ev(&format!("{prefix}{kind}"), seq, payload)
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
    ]
}

type PgState = (
    Vec<(String, Option<String>, Option<String>)>,
    Vec<(String, String, i64, bool, bool)>,
);

fn pg_state(url: &str, table: &str) -> Result<PgState, Box<dyn std::error::Error>> {
    let mut c = postgres::Client::connect(url, postgres::NoTls)?;
    let rows = c
        .query(
            &format!("SELECT id, name, email FROM {table} ORDER BY id"),
            &[],
        )?
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2)))
        .collect();
    let guards = c
        .query(
            "SELECT entity_key, facet, last_sequence, deleted, permanent
             FROM conduit_projection_state WHERE target_table = $1 ORDER BY 1, 2",
            &[&table],
        )?
        .iter()
        .map(|r| (r.get(0), r.get(1), r.get(2), r.get(3), r.get(4)))
        .collect();
    Ok((rows, guards))
}

fn fresh_batch_table(url: &str) -> Result<String, Box<dyn std::error::Error>> {
    let t = fresh_table();
    pg_exec(
        url,
        &format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id text primary key, name text, email text)"
        ),
    )?;
    // No guard table yet on a fresh database: nothing to clear.
    pg_exec(
        url,
        &format!("DELETE FROM conduit_projection_state WHERE target_table = '{t}'"),
    )
    .ok();
    Ok(t)
}

#[test]
fn pg_batch_matches_one_event_at_a_time() -> R {
    require_pg!(url);
    let (tb, ts) = (fresh_batch_table(&url)?, fresh_batch_table(&url)?);

    let batched = pg_adapter(&url, batch_mappings("B", &tb));
    let events = batch_stream("B");
    let refs: Vec<&Event> = events.iter().collect();
    stages::take();
    batched.begin_batch(&refs).map_err(|e| e.to_string())?;
    let batch_outcomes: Vec<String> = events
        .iter()
        .map(|e| format!("{:?}", batched.handle(e).outcome))
        .collect();
    batched.commit_batch().map_err(|e| e.to_string())?;
    let t = stages::take();
    assert_eq!(t.commit.calls, 1, "one transaction for the batch");
    assert_eq!(t.read.calls, 1, "guard lanes loaded in one query");

    let single = pg_adapter(&url, batch_mappings("S", &ts));
    let single_outcomes: Vec<String> = batch_stream("S")
        .iter()
        .map(|e| format!("{:?}", single.handle(e).outcome))
        .collect();

    assert_eq!(batch_outcomes, single_outcomes);
    assert!(
        !batch_outcomes.iter().any(|o| o.starts_with("Failed")),
        "{batch_outcomes:?}"
    );
    assert_eq!(pg_state(&url, &tb)?, pg_state(&url, &ts)?);
    let (rows, _) = pg_state(&url, &tb)?;
    assert_eq!(
        rows,
        vec![
            ("u1".into(), Some("c".into()), Some("c@x".into())),
            ("u2".into(), Some("g".into()), None),
        ]
    );
    Ok(())
}

#[test]
fn pg_batch_rollback_leaves_nothing() -> R {
    require_pg!(url);
    let t = fresh_batch_table(&url)?;
    let a = pg_adapter(&url, batch_mappings("R", &t));
    let events = batch_stream("R");
    let refs: Vec<&Event> = events.iter().collect();

    a.begin_batch(&refs).map_err(|e| e.to_string())?;
    for e in &events {
        assert!(a.handle(e).is_success());
    }
    a.rollback_batch();
    assert_eq!(pg_state(&url, &t)?, (vec![], vec![]), "nothing written");

    // Outside a batch, each event commits on its own again.
    let r = a.handle(&events[0]);
    assert!(
        matches!(r.outcome, AdapterOutcome::Created),
        "{:?}",
        r.outcome
    );
    assert_eq!(pg_state(&url, &t)?.0.len(), 1);
    Ok(())
}

/// One batch writes the same keys many times: updates, a delete and a
/// re-create, facet writes, interleaved across keys. The bulk write collapses
/// each key to its final state (one statement cannot touch a row twice) and
/// must end exactly where one-event-at-a-time does.
#[test]
fn pg_bulk_repeated_keys_in_one_batch() -> R {
    require_pg!(url);
    let stream = |prefix: &str| {
        use serde_json::json;
        let mut events = Vec::new();
        let mut seq = 0u64;
        let mut next = || {
            seq += 1;
            seq
        };
        for round in 0..5 {
            for key in ["a", "b", "c"] {
                let s = next();
                events.push(Event {
                    event_id: format!("evt-{prefix}-{key}-{s}"),
                    ..ev(
                        &format!("{prefix}Up"),
                        s,
                        json!({ "id": key, "name": format!("{key}{round}") }),
                    )
                });
                let s = next();
                events.push(Event {
                    event_id: format!("evt-{prefix}-{key}-{s}"),
                    ..ev(
                        &format!("{prefix}Mail"),
                        s,
                        json!({ "id": key, "email": format!("{key}{round}@x") }),
                    )
                });
            }
            if round == 2 {
                // Delete `b` mid-batch; the next rounds re-create it.
                let s = next();
                events.push(Event {
                    event_id: format!("evt-{prefix}-b-{s}"),
                    ..ev(&format!("{prefix}Del"), s, json!({ "id": "b" }))
                });
            }
        }
        // And delete `c` at the end, for good.
        let s = next();
        events.push(Event {
            event_id: format!("evt-{prefix}-c-{s}"),
            ..ev(&format!("{prefix}Del"), s, json!({ "id": "c" }))
        });
        events
    };
    let (tb, ts) = (fresh_batch_table(&url)?, fresh_batch_table(&url)?);

    let batched = pg_adapter(&url, batch_mappings("K", &tb));
    let events = stream("K");
    let refs: Vec<&Event> = events.iter().collect();
    batched.begin_batch(&refs).map_err(|e| e.to_string())?;
    for e in &events {
        let r = batched.handle(e);
        assert!(r.is_success(), "{:?}", r.outcome);
    }
    batched.commit_batch().map_err(|e| e.to_string())?;

    let single = pg_adapter(&url, batch_mappings("L", &ts));
    for e in stream("L") {
        assert!(single.handle(&e).is_success());
    }

    let (rows, guards) = pg_state(&url, &tb)?;
    let (rows_s, guards_s) = pg_state(&url, &ts)?;
    assert_eq!(rows, rows_s);
    // Event ids differ by prefix; compare the rest of every guard lane.
    assert_eq!(guards, guards_s);
    assert_eq!(
        rows,
        vec![
            ("a".into(), Some("a4".into()), Some("a4@x".into())),
            ("b".into(), Some("b4".into()), Some("b4@x".into())),
        ]
    );
    Ok(())
}

/// A row the database rejects (here a `CHECK` constraint) is only seen when
/// the batch's bulk write runs: `commit_batch` fails and nothing is left,
/// and the per-event path the run loop then falls back to fails just that
/// event.
#[test]
fn pg_bulk_failure_fails_the_commit_and_per_event_isolates_it() -> R {
    require_pg!(url);
    let t = fresh_batch_table(&url)?;
    pg_exec(
        &url,
        &format!("ALTER TABLE {t} ADD CONSTRAINT {t}_no_bad CHECK (name <> 'bad')"),
    )?;
    let a = pg_adapter(&url, batch_mappings("F", &t));
    let events: Vec<Event> = ["ok1", "bad", "ok2"]
        .iter()
        .enumerate()
        .map(|(i, name)| Event {
            event_id: format!("evt-F-{i}"),
            ..ev(
                "FUp",
                1,
                serde_json::json!({ "id": format!("u{i}"), "name": name }),
            )
        })
        .collect();
    let refs: Vec<&Event> = events.iter().collect();

    a.begin_batch(&refs).map_err(|e| e.to_string())?;
    for e in &events {
        assert!(a.handle(e).is_success(), "recorded, not yet written");
    }
    assert!(a.commit_batch().is_err(), "the bulk write is rejected");
    assert_eq!(pg_state(&url, &t)?, (vec![], vec![]), "nothing written");

    let outcomes: Vec<bool> = events.iter().map(|e| a.handle(e).is_success()).collect();
    assert_eq!(outcomes, [true, false, true]);
    assert_eq!(pg_state(&url, &t)?.0.len(), 2);
    Ok(())
}
