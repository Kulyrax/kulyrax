//! Phase 2: one transaction per adapter per batch (SQLite).
//!
//! A batch must leave exactly the state one-event-at-a-time dispatch leaves,
//! including deletes, permanent tombstones, resurrections, facet cascades and
//! redeliveries of the same entity inside one batch; a failing event or a
//! failing commit rolls the batch back and it is re-dispatched one event at a
//! time; a rolled-back batch leaves nothing behind.

mod common;

use common::config;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::adapter::{AdapterError, AdapterOutcome, AdapterResult, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::execution::stages;
use kulyrax_core::replay::{ReplayContext, ReplayReport, ReplayRunOptions};
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::config::{
    AdapterConfig, KulyraxConfig, MigrationPolicy, SqliteAdapterConfig, SqliteConfig,
};
use kulyrax_core::source::EventSource;
use kulyrax_core::source::directory::DirectorySource;
use kulyrax_core::upcast::UpcasterRegistry;
use kulyrax_core::{RunMode, SourceRunOptions, StoppedReason, run_sources_with_adapters};

use rusqlite::Connection;
use std::cell::Cell;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tempfile::tempdir;

fn mapping(yaml: &str) -> SqlMapping {
    serde_yaml::from_str(yaml).unwrap()
}

fn mappings() -> HashMap<String, SqlMapping> {
    let mut m = HashMap::new();
    m.insert(
        "UserCreated".into(),
        mapping(
            "event: UserCreated\ntable: users\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  display_name: payload.name\n",
        ),
    );
    m.insert(
        "UserDeleted".into(),
        mapping(
            "event: UserDeleted\ntable: users\nprimary_key: id\nversion: 1\noperation: delete\ncolumns:\n  id: payload.id\n",
        ),
    );
    m.insert(
        "UserPurged".into(),
        mapping(
            "event: UserPurged\ntable: users\nprimary_key: id\nversion: 1\noperation: delete\npermanent: true\ncolumns:\n  id: payload.id\n",
        ),
    );
    m.insert(
        "UserEmailChanged".into(),
        mapping(
            "event: UserEmailChanged\ntable: users\nprimary_key: id\nversion: 1\nfacet: contact\ncolumns:\n  id: payload.id\n  email: payload.email\n",
        ),
    );
    // Writes to a table that does not exist: fails every time.
    m.insert(
        "UserBroken".into(),
        mapping(
            "event: UserBroken\ntable: no_such_table\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n",
        ),
    );
    m
}

fn routing(adapters: &[&str]) -> HashMap<String, Vec<String>> {
    [
        "UserCreated",
        "UserDeleted",
        "UserPurged",
        "UserEmailChanged",
        "UserBroken",
    ]
    .into_iter()
    .map(|t| {
        (
            t.to_string(),
            adapters.iter().map(|a| a.to_string()).collect(),
        )
    })
    .collect()
}

fn ev(sequence: u64, event_type: &str, id: &str, field: Option<(&str, &str)>) -> Event {
    let mut payload = serde_json::json!({ "id": id });
    if let Some((k, v)) = field {
        payload[k] = serde_json::Value::String(v.to_string());
    }
    Event {
        event_id: format!("{event_type}-{id}-{sequence}"),
        event_type: event_type.to_string(),
        payload: payload.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

/// Several entities, each going through most of the guard rules within one
/// batch.
fn stream() -> Vec<Event> {
    let name = |n| Some(("name", n));
    let email = |e| Some(("email", e));
    vec![
        // u1: create, facet, update, delete (cascades the facet), facet on a
        // deleted entity (absent), resurrect (clears the facet tombstone),
        // facet again, and a stale redelivery of the update.
        ev(1, "UserCreated", "u1", name("Ada")),
        ev(2, "UserEmailChanged", "u1", email("ada@x")),
        ev(3, "UserCreated", "u1", name("Ada L.")),
        ev(4, "UserDeleted", "u1", None),
        ev(5, "UserEmailChanged", "u1", email("gone@x")),
        ev(6, "UserCreated", "u1", name("Ada again")),
        ev(7, "UserEmailChanged", "u1", email("ada2@x")),
        ev(3, "UserCreated", "u1", name("Ada L.")),
        // u2: facet before create, create, delete twice (the second bumps
        // the tombstone), resurrect.
        ev(1, "UserEmailChanged", "u2", email("early@x")),
        ev(2, "UserCreated", "u2", name("Grace")),
        ev(3, "UserDeleted", "u2", None),
        ev(4, "UserDeleted", "u2", None),
        ev(5, "UserCreated", "u2", name("Grace H.")),
        // u3: create, purge (permanent), create again (tombstoned).
        ev(1, "UserCreated", "u3", name("Linus")),
        ev(2, "UserPurged", "u3", None),
        ev(3, "UserCreated", "u3", name("Linus T.")),
        // u4: delete before it exists, then a create that is not newer.
        ev(5, "UserDeleted", "u4", None),
        ev(5, "UserCreated", "u4", name("Ken")),
    ]
}

fn init_users(db: &Path) {
    Connection::open(db)
        .unwrap()
        .execute(
            "CREATE TABLE users (id TEXT PRIMARY KEY, display_name TEXT, email TEXT)",
            [],
        )
        .unwrap();
}

type Users = Vec<(String, Option<String>, Option<String>)>;
type Guards = Vec<(String, String, String, i64, String, i64, i64)>;

/// Every target row and every guard row except its timestamp.
fn dump(db: &Path) -> (Users, Guards) {
    let conn = Connection::open(db).unwrap();
    let users = conn
        .prepare("SELECT id, display_name, email FROM users ORDER BY id")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    let guards = conn
        .prepare(
            "SELECT target_table, entity_key, facet, last_sequence, last_event_id, deleted, permanent
             FROM conduit_projection_state ORDER BY target_table, entity_key, facet",
        )
        .unwrap()
        .query_map([], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
            ))
        })
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    (users, guards)
}

fn replay(db: &Path, events: Vec<Event>, max_batch: usize) -> ReplayReport {
    init_users(db);
    let mut ctx = ReplayContext::new(
        &config(db),
        routing(&["sql-primary"]),
        mappings(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );
    ctx.run_stream_with_options(
        events.into_iter().map(Ok),
        &ReplayRunOptions {
            max_batch: Some(max_batch),
            ..Default::default()
        },
    )
    .unwrap()
}

#[test]
fn batch_equals_one_event_at_a_time() {
    let tmp = tempdir().unwrap();
    let (batched, single) = (tmp.path().join("batched.db"), tmp.path().join("single.db"));

    stages::take();
    let rb = replay(&batched, stream(), 1000);
    let t = stages::take();
    assert_eq!(t.commit.calls, 1, "one transaction for the whole batch");
    assert_eq!(t.read.calls, 1, "guard lanes loaded in one query");
    let rs = replay(&single, stream(), 1);
    assert_eq!(stages::take().commit.calls, stream().len() as u64);

    assert_eq!(rb.events_failed, 0, "{rb:?}");
    assert_eq!(dump(&batched), dump(&single));
    let statuses = |r: &ReplayReport| r.per_event.iter().map(|e| e.status).collect::<Vec<_>>();
    assert_eq!(statuses(&rb), statuses(&rs));

    // And the state is the one the guard rules give.
    let (users, guards) = dump(&batched);
    assert_eq!(
        users,
        vec![
            ("u1".into(), Some("Ada again".into()), Some("ada2@x".into())),
            ("u2".into(), Some("Grace H.".into()), None),
        ]
    );
    let lane = |key: &str, facet: &str| {
        guards
            .iter()
            .find(|g| g.1 == key && g.2 == facet)
            .map(|g| (g.3, g.5, g.6))
    };
    assert_eq!(lane(r#"["u1"]"#, ""), Some((6, 0, 0)));
    assert_eq!(lane(r#"["u1"]"#, "contact"), Some((7, 0, 0)));
    assert_eq!(lane(r#"["u2"]"#, ""), Some((5, 0, 0)));
    assert_eq!(
        lane(r#"["u3"]"#, ""),
        Some((2, 1, 1)),
        "permanent tombstone"
    );
    assert_eq!(
        lane(r#"["u4"]"#, ""),
        Some((5, 1, 0)),
        "delete before create"
    );
}

#[test]
fn batch_boundaries_do_not_matter() {
    let tmp = tempdir().unwrap();
    let reference = tmp.path().join("one.db");
    replay(&reference, stream(), 1);
    for size in [2, 3, 5, 7] {
        let db = tmp.path().join(format!("b{size}.db"));
        replay(&db, stream(), size);
        assert_eq!(dump(&db), dump(&reference), "batch size {size}");
    }
}

fn sqlite(id: &str, db: &Path) -> SqliteAdapter {
    SqliteAdapter::new(
        id.to_string(),
        db.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

#[test]
fn rollback_leaves_nothing_and_the_adapter_carries_on() {
    let tmp = tempdir().unwrap();
    let db = tmp.path().join("t.db");
    init_users(&db);
    let a = sqlite("sql-primary", &db);
    a.prepare().unwrap();

    let events = [
        ev(1, "UserCreated", "u1", Some(("name", "Ada"))),
        ev(2, "UserCreated", "u1", Some(("name", "Ada L."))),
    ];
    let refs: Vec<&Event> = events.iter().collect();
    a.begin_batch(&refs).unwrap();
    for e in &events {
        assert!(a.handle(e).is_success());
    }
    a.rollback_batch();
    assert_eq!(dump(&db), (vec![], vec![]), "nothing written");

    // Outside a batch, handle commits each event as before.
    let r = a.handle(&events[0]);
    assert!(matches!(r.outcome, AdapterOutcome::Created));
    assert_eq!(dump(&db).0.len(), 1);
    // A batch can open again.
    a.begin_batch(&refs[1..]).unwrap();
    assert!(matches!(
        a.handle(&events[1]).outcome,
        AdapterOutcome::Updated
    ));
    a.commit_batch().unwrap();
    assert_eq!(dump(&db).0[0].1.as_deref(), Some("Ada L."));
}

fn write_stream(dir: &Path, events: &[Event]) {
    std::fs::create_dir_all(dir).unwrap();
    for (i, e) in events.iter().enumerate() {
        std::fs::write(
            dir.join(format!("{i:04}.json")),
            serde_json::to_string(e).unwrap(),
        )
        .unwrap();
    }
}

#[test]
fn failing_event_is_isolated_and_the_rest_lands() {
    let tmp = tempdir().unwrap();
    let mut events = stream();
    events.insert(5, ev(9, "UserBroken", "u9", None));

    let run = |name: &str, max_batch: usize| {
        let db = tmp.path().join(format!("{name}.db"));
        init_users(&db);
        let dir = tmp.path().join(format!("{name}-events"));
        write_stream(&dir, &events);
        let src: Box<dyn EventSource> = Box::new(
            DirectorySource::new("dir", &dir, tmp.path().join(format!("{name}-state"))).unwrap(),
        );
        let report = run_sources_with_adapters(
            &config(&db),
            routing(&["sql-primary"]),
            vec![Box::new(sqlite("sql-primary", &db))],
            vec![src],
            &SourceRunOptions {
                mode: RunMode::Once,
                max_batch,
                retry_budget: 0,
                max_batch_delay: std::time::Duration::ZERO,
                dlq_dir: Some(tmp.path().join(format!("{name}-dlq"))),
            },
            &AtomicBool::new(false),
        )
        .unwrap();
        (db, report)
    };

    let (batched, rb) = run("batched", 1000);
    let (single, _) = run("single", 1);

    assert!(matches!(rb.stopped_reason, StoppedReason::CaughtUp));
    assert_eq!(rb.batches.len(), 1);
    assert!(rb.batches[0].fell_back, "the batch rolled back");
    assert_eq!(rb.events_dlq, 1, "only the broken event is parked");
    assert_eq!(dump(&batched), dump(&single));
    assert_eq!(dump(&batched).0.len(), 2);
}

/// A SQLite adapter whose first `commit_batch` fails (after rolling its
/// batch back), as a store that rejects a commit would.
struct FailFirstCommit {
    inner: SqliteAdapter,
    commits: Cell<u32>,
}

impl StorageAdapter for FailFirstCommit {
    fn kind(&self) -> StorageKind {
        StorageKind::Sql
    }
    fn id(&self) -> &str {
        self.inner.id()
    }
    fn priority(&self) -> u32 {
        self.inner.priority()
    }
    fn prepare(&self) -> Result<(), AdapterError> {
        self.inner.prepare()
    }
    fn begin_batch(&self, events: &[&Event]) -> Result<(), AdapterError> {
        self.inner.begin_batch(events)
    }
    fn commit_batch(&self) -> Result<(), AdapterError> {
        self.commits.set(self.commits.get() + 1);
        if self.commits.get() == 1 {
            self.inner.rollback_batch();
            return Err(AdapterError::WriteFailed("commit rejected".into()));
        }
        self.inner.commit_batch()
    }
    fn rollback_batch(&self) {
        self.inner.rollback_batch()
    }
    fn handle(&self, event: &Event) -> AdapterResult {
        self.inner.handle(event)
    }
}

fn two_sqlite_config(a: &Path, b: &Path) -> KulyraxConfig {
    let mut c = config(a);
    let entry = |id: &str, path: &Path, priority| {
        AdapterConfig::Sqlite(SqliteAdapterConfig {
            id: id.into(),
            priority,
            config: SqliteConfig {
                path: path.to_string_lossy().into(),
            },
            capabilities: None,
            depends_on: vec![],
        })
    };
    c.adapters = vec![entry("sql-a", a, 10), entry("sql-b", b, 20)];
    c
}

#[test]
fn failed_commit_falls_back_without_loss_or_doubling() {
    let tmp = tempdir().unwrap();
    let (db_a, db_b) = (tmp.path().join("a.db"), tmp.path().join("b.db"));
    init_users(&db_a);
    init_users(&db_b);
    let events: Vec<Event> = (1..=4)
        .map(|i| ev(1, "UserCreated", &format!("u{i}"), Some(("name", "x"))))
        .collect();
    let dir = tmp.path().join("events");
    write_stream(&dir, &events);
    let src: Box<dyn EventSource> =
        Box::new(DirectorySource::new("dir", &dir, tmp.path().join("state")).unwrap());

    // `sql-a` commits first (priority 10); `sql-b`'s commit then fails.
    let report = run_sources_with_adapters(
        &two_sqlite_config(&db_a, &db_b),
        routing(&["sql-a", "sql-b"]),
        vec![
            Box::new(sqlite("sql-a", &db_a)),
            Box::new(FailFirstCommit {
                inner: sqlite("sql-b", &db_b),
                commits: Cell::new(0),
            }),
        ],
        vec![src],
        &SourceRunOptions {
            mode: RunMode::Once,
            max_batch: 1000,
            retry_budget: 0,
            max_batch_delay: std::time::Duration::ZERO,
            dlq_dir: None,
        },
        &AtomicBool::new(false),
    )
    .unwrap();

    let b = &report.batches[0];
    assert!(b.committed && b.fell_back, "{b:?}");
    assert_eq!(report.events_failed, 0);
    // The fallback re-delivered `sql-a`'s committed events (skipped by its
    // guards) and created `sql-b`'s.
    assert_eq!((b.created, b.skipped), (4, 4));
    for db in [&db_a, &db_b] {
        let (users, guards) = dump(db);
        assert_eq!(users.len(), 4);
        assert!(guards.iter().all(|g| g.3 == 1), "each entity written once");
    }
}
