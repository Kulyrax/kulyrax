//! The Postgres outbox source against a real server.
//!
//! **Gated on `KULYRAX_PG_URL`**, like the Postgres adapter's suite: unset, every
//! test prints a skip line and passes. The tests run one at a time (`SERIAL`):
//! the gap rule reads the database's oldest running transaction, so a test
//! holding a transaction open would hold back another test's gaps.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, Instant};

use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::runtime::config::{
    AdapterConfig, KulyraxConfig, RoutingConfig, SqliteAdapterConfig, SqliteConfig,
};
use kulyrax_core::source::{EventSource, SourcedEvent};
use kulyrax_core::{RunMode, SourceRunOptions};
use kulyrax_source_postgres::{OutboxColumns, OutboxConfig, PostgresOutboxSource};

type R = Result<(), Box<dyn std::error::Error>>;

static SERIAL: Mutex<()> = Mutex::new(());
static TABLE_SEQ: AtomicU32 = AtomicU32::new(0);

macro_rules! require_pg {
    ($url:ident, $guard:ident) => {
        let Some($url) = std::env::var("KULYRAX_PG_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())
        else {
            eprintln!("SKIP: KULYRAX_PG_URL not set");
            return Ok(());
        };
        let $guard = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    };
}

fn connect(url: &str) -> Result<postgres::Client, postgres::Error> {
    postgres::Client::connect(url, postgres::NoTls)
}

/// A fresh outbox table, the shape the decision record documents.
fn fresh_outbox(url: &str) -> Result<String, Box<dyn std::error::Error>> {
    let t = format!(
        "outbox_{}_{}",
        std::process::id(),
        TABLE_SEQ.fetch_add(1, Ordering::Relaxed)
    );
    connect(url)?.batch_execute(&format!(
        "DROP TABLE IF EXISTS {t};
         CREATE TABLE {t} (
             id bigserial PRIMARY KEY,
             event_id text NOT NULL,
             event_type text NOT NULL,
             payload jsonb NOT NULL,
             metadata jsonb,
             version int
         )"
    ))?;
    Ok(t)
}

fn config(url: &str, table: &str) -> OutboxConfig {
    OutboxConfig {
        url: url.to_string(),
        table: table.to_string(),
        sequence_column: "id".into(),
        columns: OutboxColumns::default(),
        gap_timeout_ms: 60_000,
        watch_retention_ms: 3_600_000,
        allow_unverified_sequence: false,
    }
}

/// Insert one event row (in its own transaction) and return its id.
fn insert(
    c: &mut impl postgres::GenericClient,
    table: &str,
    user: &str,
    name: &str,
) -> Result<i64, postgres::Error> {
    let row = c.query_one(
        &format!(
            "INSERT INTO {table} (event_id, event_type, payload, metadata)
             VALUES ($1, 'UserUpserted', jsonb_build_object('id', $2::text, 'name', $3::text),
                     '{{\"origin\": \"test\"}}')
             RETURNING id"
        ),
        &[&format!("evt-{user}-{name}"), &user, &name],
    )?;
    Ok(row.get(0))
}

fn open(
    dir: &Path,
    config: OutboxConfig,
) -> Result<PostgresOutboxSource, kulyrax_core::source::SourceError> {
    PostgresOutboxSource::open("outbox".into(), dir.to_path_buf(), config)
}

fn seqs(events: &[SourcedEvent]) -> Vec<u64> {
    events.iter().map(|e| e.event.sequence).collect()
}

fn detail(source: &PostgresOutboxSource, name: &str) -> String {
    source
        .details()
        .into_iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v)
        .unwrap_or_default()
}

// ---------------------------------------------------------------------------

#[test]
fn delivers_rows_in_sequence_order_with_the_envelope() -> R {
    require_pg!(url, _serial);
    let t = fresh_outbox(&url)?;
    let mut c = connect(&url)?;
    let ids: Vec<i64> = ["a", "b", "c"]
        .iter()
        .map(|n| insert(&mut c, &t, "u1", n))
        .collect::<Result<_, _>>()?;
    let dir = tempfile::tempdir()?;
    let mut s = open(dir.path(), config(&url, &t))?;

    let events = s.poll(10)?;
    assert_eq!(
        seqs(&events),
        ids.iter().map(|&i| i as u64).collect::<Vec<_>>()
    );
    let first = &events[0].event;
    assert_eq!(first.event_type, "UserUpserted");
    assert_eq!(first.event_id, "evt-u1-a");
    assert_eq!(first.version, 1, "a null version is 1");
    assert_eq!(
        first.metadata.get("origin").map(String::as_str),
        Some("test")
    );
    let payload: serde_json::Value = serde_json::from_str(&first.payload)?;
    assert_eq!(payload["name"], "a");
    // Positions sort as text in delivery order.
    let positions: Vec<&str> = events.iter().map(|e| e.position.as_str()).collect();
    let mut sorted = positions.clone();
    sorted.sort_unstable();
    assert_eq!(positions, sorted);

    // A batch limit is honoured.
    assert_eq!(s.poll(2)?.len(), 2);
    Ok(())
}

#[test]
fn an_empty_table_delivers_nothing() -> R {
    require_pg!(url, _serial);
    let t = fresh_outbox(&url)?;
    let dir = tempfile::tempdir()?;
    let mut s = open(dir.path(), config(&url, &t))?;
    assert!(s.poll(10)?.is_empty());
    assert!(s.poll(10)?.is_empty());
    assert_eq!(s.committed_position(), None);
    Ok(())
}

#[test]
fn resumes_from_the_committed_position() -> R {
    require_pg!(url, _serial);
    let t = fresh_outbox(&url)?;
    let mut c = connect(&url)?;
    for n in ["a", "b"] {
        insert(&mut c, &t, "u1", n)?;
    }
    let dir = tempfile::tempdir()?;
    {
        let mut s = open(dir.path(), config(&url, &t))?;
        let events = s.poll(10)?;
        assert_eq!(events.len(), 2);
        s.commit(events[1].position.clone())?;
    }
    let c_id = insert(&mut c, &t, "u1", "c")?;
    let mut s = open(dir.path(), config(&url, &t))?;
    assert_eq!(seqs(&s.poll(10)?), vec![c_id as u64], "only the new row");
    Ok(())
}

/// A row whose transaction is still open holds back the rows after it;
/// once it commits, both are delivered in order.
#[test]
fn a_gap_is_held_until_it_fills() -> R {
    require_pg!(url, _serial);
    let t = fresh_outbox(&url)?;
    let mut c = connect(&url)?;
    let first = insert(&mut c, &t, "u1", "a")?;
    let dir = tempfile::tempdir()?;
    let mut s = open(dir.path(), config(&url, &t))?;
    let events = s.poll(10)?;
    assert_eq!(seqs(&events), vec![first as u64]);
    s.commit(events[0].position.clone())?;

    let mut slow = connect(&url)?;
    let mut tx = slow.transaction()?;
    let held = insert(&mut tx, &t, "u2", "slow")?;
    let after = insert(&mut c, &t, "u3", "fast")?;
    assert!(
        s.poll(10)?.is_empty(),
        "the open transaction's row is a gap"
    );
    assert!(s.poll(10)?.is_empty(), "still held");
    tx.commit()?;
    assert_eq!(seqs(&s.poll(10)?), vec![held as u64, after as u64]);
    Ok(())
}

/// A rolled-back insert leaves a final gap: skipped on the next poll, with
/// `gap_timeout_ms` (60 s here) nowhere near elapsed.
#[test]
fn a_rolled_back_insert_is_skipped_without_waiting_for_the_timeout() -> R {
    require_pg!(url, _serial);
    let t = fresh_outbox(&url)?;
    let mut c = connect(&url)?;
    let first = insert(&mut c, &t, "u1", "a")?;
    let dir = tempfile::tempdir()?;
    let mut s = open(dir.path(), config(&url, &t))?;
    let events = s.poll(10)?;
    s.commit(events[0].position.clone())?;
    assert_eq!(seqs(&events), vec![first as u64]);

    let mut tx = c.transaction()?;
    let lost = insert(&mut tx, &t, "u2", "lost")?;
    tx.rollback()?;
    let after = insert(&mut c, &t, "u3", "after")?;

    let started = Instant::now();
    assert!(s.poll(10)?.is_empty(), "first sight of the gap holds it");
    let events = s.poll(10)?;
    assert_eq!(
        seqs(&events),
        vec![after as u64],
        "the gap at {lost} is final"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "{:?}",
        started.elapsed()
    );
    s.commit(events[0].position.clone())?;
    assert_eq!(
        detail(&s, "watched gaps"),
        "1",
        "the skipped value is watched"
    );
    Ok(())
}

#[test]
fn watched_values_expire() -> R {
    require_pg!(url, _serial);
    let t = fresh_outbox(&url)?;
    let mut c = connect(&url)?;
    insert(&mut c, &t, "u1", "a")?;
    let dir = tempfile::tempdir()?;
    let mut s = open(
        dir.path(),
        OutboxConfig {
            watch_retention_ms: 200,
            ..config(&url, &t)
        },
    )?;
    let events = s.poll(10)?;
    s.commit(events[0].position.clone())?;
    let mut tx = c.transaction()?;
    let lost = insert(&mut tx, &t, "u2", "lost")?;
    tx.rollback()?;
    insert(&mut c, &t, "u3", "after")?;
    assert!(s.poll(10)?.is_empty());
    let events = s.poll(10)?;
    s.commit(events[0].position.clone())?;
    assert_eq!(detail(&s, "watched gaps"), "1");

    std::thread::sleep(Duration::from_millis(400));
    // The skipped value now commits after all (inserted with its own id),
    // but its watch has expired: it is not read again.
    c.execute(
        &format!(
            "INSERT INTO {t} (id, event_id, event_type, payload)
             VALUES ($1, 'evt-late', 'UserUpserted', '{{\"id\": \"u2\", \"name\": \"late\"}}')"
        ),
        &[&lost],
    )?;
    let events = s.poll(10)?;
    assert!(events.is_empty(), "{:?}", seqs(&events));
    if let Some(last) = events.last() {
        s.commit(last.position.clone())?;
    }
    // Expiry is committed with the next batch; a poll alone recomputes it.
    let next = insert(&mut c, &t, "u4", "next")?;
    let events = s.poll(10)?;
    assert_eq!(seqs(&events), vec![next as u64]);
    s.commit(events[0].position.clone())?;
    assert_eq!(detail(&s, "watched gaps"), "0");
    Ok(())
}

#[test]
fn a_second_reader_is_refused() -> R {
    require_pg!(url, _serial);
    let t = fresh_outbox(&url)?;
    let dir = tempfile::tempdir()?;
    let _first = open(dir.path(), config(&url, &t))?;
    let err = open(dir.path(), config(&url, &t))
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("is in use by another process"), "{err}");
    assert!(err.contains("outbox.lock"), "{err}");
    Ok(())
}

#[test]
fn start_up_refuses_a_bad_table_or_sequence() -> R {
    require_pg!(url, _serial);
    let t = fresh_outbox(&url)?;
    let refused = |config: OutboxConfig| {
        let dir = tempfile::tempdir().expect("tempdir");
        open(dir.path(), config)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default()
    };

    // A column the config names but the table lacks.
    let err = refused(OutboxConfig {
        columns: OutboxColumns {
            payload: "body".into(),
            ..OutboxColumns::default()
        },
        ..config(&url, &t)
    });
    assert!(
        err.contains("columns.payload") && err.contains("no column \"body\""),
        "{err}"
    );

    // A config without sequence_column does not parse.
    let err = serde_yaml::from_str::<OutboxConfig>(&format!("url: {url}\ntable: {t}\n"))
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("sequence_column"), "{err}");

    let mut c = connect(&url)?;
    let seq: String = c
        .query_one("SELECT pg_get_serial_sequence($1, 'id')", &[&t])?
        .get(0);
    c.batch_execute(&format!("ALTER SEQUENCE {seq} INCREMENT 2"))?;
    let err = refused(config(&url, &t));
    assert!(err.contains(&seq) && err.contains("INCREMENT 2"), "{err}");
    c.batch_execute(&format!("ALTER SEQUENCE {seq} INCREMENT 1 CACHE 10"))?;
    let err = refused(config(&url, &t));
    assert!(err.contains(&seq) && err.contains("CACHE 10"), "{err}");

    // No sequence at all: refused unless explicitly allowed.
    let plain = fresh_outbox(&url)?;
    let plain_seq: String = c
        .query_one("SELECT pg_get_serial_sequence($1, 'id')", &[&plain])?
        .get(0);
    c.batch_execute(&format!(
        "ALTER TABLE {plain} ALTER COLUMN id DROP DEFAULT; DROP SEQUENCE {plain_seq}"
    ))?;
    let err = refused(config(&url, &plain));
    assert!(err.contains("has no sequence behind it"), "{err}");
    let dir = tempfile::tempdir()?;
    open(
        dir.path(),
        OutboxConfig {
            allow_unverified_sequence: true,
            ..config(&url, &plain)
        },
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Through the run loop, into SQLite
// ---------------------------------------------------------------------------

fn run_once(
    db: &Path,
    source: PostgresOutboxSource,
) -> Result<kulyrax_core::SourceRunReport, kulyrax_core::SourceError> {
    let config = KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "routing.yaml".into(),
        },
        adapters: vec![AdapterConfig::Sqlite(SqliteAdapterConfig {
            id: "sql".into(),
            priority: 10,
            config: SqliteConfig {
                path: db.to_string_lossy().into(),
            },
            capabilities: None,
            depends_on: vec![],
        })],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    };
    let mapping: SqlMapping = serde_yaml::from_str(
        "event: UserUpserted\ntable: users\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  name: payload.name\n",
    )
    .map_err(|e| kulyrax_core::SourceError::Parse(e.to_string()))?;
    kulyrax_core::run_sources(
        &config,
        HashMap::from([("UserUpserted".to_string(), vec!["sql".to_string()])]),
        HashMap::from([("UserUpserted".to_string(), mapping)]),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        vec![Box::new(source)],
        &SourceRunOptions {
            mode: RunMode::Once,
            max_batch: 100,
            retry_budget: 1,
            max_batch_delay: Duration::ZERO,
            dlq_dir: None,
        },
        &std::sync::atomic::AtomicBool::new(false),
    )
}

fn users(db: &Path) -> Vec<(String, String)> {
    let conn = rusqlite_open(db);
    let mut stmt = conn
        .prepare("SELECT id, name FROM users ORDER BY id")
        .expect("prepare");
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .expect("query")
        .collect::<Result<_, _>>()
        .expect("rows")
}

fn rusqlite_open(db: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(db).expect("open sqlite")
}

/// A crash after the projection but before the commit (here: the checkpoint
/// is lost) redelivers the batch; the guards skip every event of it.
#[test]
fn redelivery_after_a_crash_before_commit_is_skipped_by_the_guards() -> R {
    require_pg!(url, _serial);
    let t = fresh_outbox(&url)?;
    let mut c = connect(&url)?;
    for (u, n) in [("u1", "a"), ("u2", "b"), ("u1", "c")] {
        insert(&mut c, &t, u, n)?;
    }
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("app.db");
    rusqlite_open(&db).execute_batch("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)")?;
    let state = dir.path().join("state");

    let first = run_once(
        &db,
        PostgresOutboxSource::open("outbox".into(), state.clone(), config(&url, &t))?,
    )?;
    assert_eq!(first.events_processed, 3);
    assert_eq!(
        users(&db),
        vec![("u1".into(), "c".into()), ("u2".into(), "b".into())]
    );

    std::fs::remove_file(state.join("outbox.json"))?;
    let again = run_once(
        &db,
        PostgresOutboxSource::open("outbox".into(), state, config(&url, &t))?,
    )?;
    assert_eq!(again.events_processed, 3, "the batch is delivered again");
    let skipped: u64 = again.batches.iter().map(|b| b.skipped).sum();
    assert_eq!(skipped, 3, "and every event of it is skipped");
    assert_eq!(
        users(&db),
        vec![("u1".into(), "c".into()), ("u2".into(), "b".into())]
    );
    Ok(())
}

/// A value skipped as a gap that commits after all is delivered late, and
/// judged by its sequence: here its entity already has a newer event, so it
/// is skipped, and the source counts it.
#[test]
fn a_late_row_is_delivered_logged_and_counted() -> R {
    require_pg!(url, _serial);
    let t = fresh_outbox(&url)?;
    let mut c = connect(&url)?;
    insert(&mut c, &t, "u1", "a")?;
    let mut tx = c.transaction()?;
    let lost = insert(&mut tx, &t, "u1", "lost")?;
    tx.rollback()?;
    insert(&mut c, &t, "u1", "newer")?;
    let dir = tempfile::tempdir()?;
    let db = dir.path().join("app.db");
    rusqlite_open(&db).execute_batch("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)")?;
    let state = dir.path().join("state");

    // Two passes: the first sees the gap and holds it, the second skips it.
    for _ in 0..2 {
        run_once(
            &db,
            PostgresOutboxSource::open("outbox".into(), state.clone(), config(&url, &t))?,
        )?;
    }
    assert_eq!(users(&db), vec![("u1".into(), "newer".into())]);

    c.execute(
        &format!(
            "INSERT INTO {t} (id, event_id, event_type, payload)
             VALUES ($1, 'evt-late', 'UserUpserted', '{{\"id\": \"u1\", \"name\": \"late\"}}')"
        ),
        &[&lost],
    )?;
    let report = run_once(
        &db,
        PostgresOutboxSource::open("outbox".into(), state.clone(), config(&url, &t))?,
    )?;
    assert_eq!(report.events_processed, 1, "the late row");
    assert_eq!(
        users(&db),
        vec![("u1".into(), "newer".into())],
        "stale: skipped"
    );

    let s = PostgresOutboxSource::inspect("outbox".into(), state, config(&url, &t))?;
    assert_eq!(detail(&s, "late rows"), "1");
    assert_eq!(detail(&s, "late rows skipped"), "1");
    assert_eq!(detail(&s, "watched gaps"), "0");
    Ok(())
}
