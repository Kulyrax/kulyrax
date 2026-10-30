//! The SQL write path is one transaction, and schema setup is an explicit
//! `prepare` step outside it.
//!
//! The atomicity test makes the database reject one event's guard row with a
//! trigger, so the failure lands exactly between the target write and the
//! guard write.

mod common;

use common::*;
use kulyrax_core::UpcasterRegistry;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::adapter::{AdapterOutcome, StorageAdapter};
use kulyrax_core::execution::stages;
use kulyrax_core::runtime::config::MigrationPolicy;

use std::path::Path;
use std::sync::Arc;
use tempfile::tempdir;

fn sqlite_adapter(db: &Path) -> SqliteAdapter {
    SqliteAdapter::new(
        "sql".into(),
        db.to_string_lossy().into(),
        10,
        SqlRuntimeBuilder::new(sql_mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn guard_table_exists(db: &Path) -> bool {
    let conn = rusqlite::Connection::open(db).unwrap();
    conn.query_row(
        "SELECT count(*) FROM sqlite_master WHERE name = 'conduit_projection_state'",
        [],
        |r| r.get::<_, i64>(0),
    )
    .unwrap()
        == 1
}

fn guard_rows(db: &Path, entity_key: &str) -> i64 {
    let conn = rusqlite::Connection::open(db).unwrap();
    conn.query_row(
        "SELECT count(*) FROM conduit_projection_state WHERE entity_key = ?1",
        [entity_key],
        |r| r.get(0),
    )
    .unwrap()
}

#[test]
fn failed_guard_write_leaves_neither_row() {
    let tmp = tempdir().unwrap();
    let db = tmp.path().join("test.db");
    init_db(&db);
    let adapter = sqlite_adapter(&db);
    adapter.prepare().unwrap();

    let ok = adapter.handle(&user_event(1, "u1", "Ada"));
    assert!(matches!(ok.outcome, AdapterOutcome::Created));

    let fail = user_event(1, "u2", "Grace");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch(&format!(
            "CREATE TRIGGER fail_guard BEFORE INSERT ON conduit_projection_state
             WHEN NEW.last_event_id = '{}'
             BEGIN SELECT RAISE(ABORT, 'forced guard failure'); END;",
            fail.event_id
        ))
        .unwrap();

    let result = adapter.handle(&fail);
    assert!(
        matches!(result.outcome, AdapterOutcome::Failed(_)),
        "the guard write must fail: {:?}",
        result.outcome
    );
    assert_eq!(read_user(&db, "u2"), None, "target row must roll back");
    assert_eq!(guard_rows(&db, r#"["u2"]"#), 0, "no guard row");
    assert_eq!(read_user(&db, "u1").map(|u| u.0), Some("Ada".into()));
}

#[test]
fn handle_before_prepare_fails_and_creates_nothing() {
    let tmp = tempdir().unwrap();
    let db = tmp.path().join("test.db");
    init_db(&db);
    let adapter = sqlite_adapter(&db);

    let result = adapter.handle(&user_event(1, "u1", "Ada"));
    match &result.outcome {
        AdapterOutcome::Failed(e) => assert!(e.to_string().contains("not prepared"), "{e}"),
        other => panic!("expected a failure, got {other:?}"),
    }
    assert!(
        !guard_table_exists(&db),
        "no table created as a side effect"
    );
    assert_eq!(read_user(&db, "u1"), None);
}

#[test]
fn prepare_does_the_schema_work_and_events_do_not() {
    let tmp = tempdir().unwrap();
    let db = tmp.path().join("test.db");
    init_db(&db);
    let adapter = sqlite_adapter(&db);

    stages::take();
    adapter.prepare().unwrap();
    assert!(guard_table_exists(&db));
    for (i, id) in ["u1", "u2", "u3"].into_iter().enumerate() {
        let r = adapter.handle(&user_event(i as u64 + 1, id, "x"));
        assert!(matches!(r.outcome, AdapterOutcome::Created));
    }
    let t = stages::take();
    assert_eq!(t.schema.calls, 1, "only prepare runs schema setup");
    assert_eq!(t.commit.calls, 3);
}

#[test]
fn prepare_twice_is_idempotent() {
    let tmp = tempdir().unwrap();
    let db = tmp.path().join("test.db");
    init_db(&db);
    let adapter = sqlite_adapter(&db);

    adapter.prepare().unwrap();
    adapter.prepare().unwrap();
    assert!(guard_table_exists(&db), "exactly one guard table");
    let r = adapter.handle(&user_event(1, "u1", "Ada"));
    assert!(matches!(r.outcome, AdapterOutcome::Created));
}
