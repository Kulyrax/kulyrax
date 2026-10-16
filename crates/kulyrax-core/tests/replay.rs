//! Phase 7 replay integration tests.

use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::event::Event;
use kulyrax_core::execution::ExecutionStatus;
use kulyrax_core::replay::{ReplayContext, ReplayRunOptions, events_from_path};
use kulyrax_core::runtime::config::{
    AdapterConfig, FailurePolicy, FileAdapterConfig, FileConfig, KulyraxConfig, RoutingConfig,
    SqliteAdapterConfig, SqliteConfig,
};

use std::collections::HashMap;
use std::fs;
use std::path::Path;

/// Routing aligned with [user_created_mappings] (sql + doc adapters).
fn rules_user_created() -> HashMap<String, Vec<String>> {
    let mut m = HashMap::new();
    m.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    m
}

fn rules_user_created_and_failing() -> HashMap<String, Vec<String>> {
    let mut m = rules_user_created();
    m.insert(
        "FailingEvent".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    m
}

fn user_created_and_failing_mappings() -> (
    HashMap<String, SqlMapping>,
    HashMap<String, DocumentMapping>,
) {
    let (mut sql, mut doc) = user_created_mappings();
    sql.insert(
        "FailingEvent".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: FailingEvent
table: users
primary_key: id
version: 1
columns:
  id: payload.id
"#,
        )
        .unwrap(),
    );
    doc.insert(
        "FailingEvent".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            r#"
event: FailingEvent
collection: users
version: 1
id: payload.id
document:
  id: payload.id
"#,
        )
        .unwrap(),
    );
    (sql, doc)
}

/// File-backed SQLite so all replay events share one DB (unlike :memory: per handle).
fn sample_config(doc_root: &Path, sqlite_path: &Path) -> KulyraxConfig {
    KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "routing.json".into(),
        },
        adapters: vec![
            AdapterConfig::Sqlite(SqliteAdapterConfig {
                id: "sql-primary".into(),
                priority: 10,
                config: SqliteConfig {
                    path: sqlite_path.to_string_lossy().into(),
                },
                capabilities: None,
                depends_on: vec![],
            }),
            AdapterConfig::File(FileAdapterConfig {
                id: "doc-readmodel".into(),
                priority: 20,
                config: FileConfig {
                    root: doc_root.to_string_lossy().into(),
                },
                capabilities: None,
                depends_on: vec![],
            }),
        ],
        failure_policy: FailurePolicy::FailFast,
        migration_policy: Default::default(),
        sources: Vec::new(),
    }
}

fn user_created_mappings() -> (
    HashMap<String, SqlMapping>,
    HashMap<String, DocumentMapping>,
) {
    let mut sql = HashMap::new();
    sql.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserCreated
table: users
primary_key: id
version: 1
columns:
  id: payload.id
"#,
        )
        .unwrap(),
    );
    let mut doc = HashMap::new();
    doc.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            r#"
event: UserCreated
collection: users
version: 1
id: payload.id
document:
  id: payload.id
"#,
        )
        .unwrap(),
    );
    (sql, doc)
}

fn init_sqlite_users_table(sqlite_path: &Path) {
    let conn = rusqlite::Connection::open(sqlite_path).unwrap();
    conn.execute(
        "CREATE TABLE IF NOT EXISTS users (id TEXT PRIMARY KEY NOT NULL)",
        [],
    )
    .unwrap();
}

#[test]
fn replay_directory_sorted_order_two_user_created() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("replay.db");
    init_sqlite_users_table(&db);
    fs::create_dir_all(tmp.path().join("docs")).unwrap();

    let events_dir = tmp.path().join("events");
    fs::create_dir_all(&events_dir).unwrap();

    fs::write(
        events_dir.join("02_second.json"),
        serde_json::to_string(&Event {
            event_id: "e2".into(),
            event_type: "UserCreated".into(),
            payload: r#"{"id":"u2"}"#.into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        })
        .unwrap(),
    )
    .unwrap();
    fs::write(
        events_dir.join("01_first.json"),
        serde_json::to_string(&Event {
            event_id: "e1".into(),
            event_type: "UserCreated".into(),
            payload: r#"{"id":"u1"}"#.into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        })
        .unwrap(),
    )
    .unwrap();

    let config = sample_config(&tmp.path().join("docs"), &db);
    config.validate().unwrap();
    let (sql, doc) = user_created_mappings();
    let rules = rules_user_created();

    let mut iter = events_from_path(&events_dir).unwrap();
    let mut ctx = ReplayContext::new(&config, rules, sql, doc, HashMap::new(), HashMap::new());
    let report = ctx.run_stream(&mut iter).unwrap();

    assert_eq!(report.events_processed, 2);
    assert_eq!(report.events_succeeded, 2);
    assert_eq!(report.events_failed, 0);
    assert!(!report.stopped_early);
    assert_eq!(report.per_event[0].event_id, "e1");
    assert_eq!(report.per_event[1].event_id, "e2");
}

#[test]
fn replay_ndjson_file() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("replay.db");
    init_sqlite_users_table(&db);
    fs::create_dir_all(tmp.path().join("docs")).unwrap();

    let f = tmp.path().join("events.ndjson");
    let e1 = Event {
        event_id: "n1".into(),
        event_type: "UserCreated".into(),
        payload: r#"{"id":"a"}"#.into(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    };
    let e2 = Event {
        event_id: "n2".into(),
        event_type: "UserCreated".into(),
        payload: r#"{"id":"b"}"#.into(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    };
    fs::write(
        &f,
        format!(
            "{}\n{}\n",
            serde_json::to_string(&e1).unwrap(),
            serde_json::to_string(&e2).unwrap()
        ),
    )
    .unwrap();

    let config = sample_config(&tmp.path().join("docs"), &db);
    config.validate().unwrap();
    let (sql, doc) = user_created_mappings();
    let rules = rules_user_created();

    let mut iter = events_from_path(&f).unwrap();
    let mut ctx = ReplayContext::new(&config, rules, sql, doc, HashMap::new(), HashMap::new());
    let report = ctx.run_stream(&mut iter).unwrap();

    assert_eq!(report.events_processed, 2);
    assert_eq!(report.per_event[0].event_id, "n1");
    assert_eq!(report.per_event[1].event_id, "n2");
}

#[test]
fn replay_fail_fast_stops_after_second_event_failure() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("replay.db");
    init_sqlite_users_table(&db);
    fs::create_dir_all(tmp.path().join("docs")).unwrap();

    let events_dir = tmp.path().join("events");
    fs::create_dir_all(&events_dir).unwrap();

    fs::write(
        events_dir.join("01_ok.json"),
        serde_json::to_string(&Event {
            event_id: "ok1".into(),
            event_type: "UserCreated".into(),
            payload: r#"{"id":"u1"}"#.into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        })
        .unwrap(),
    )
    .unwrap();
    fs::write(
        events_dir.join("02_fail.json"),
        serde_json::to_string(&Event {
            event_id: "bad".into(),
            event_type: "FailingEvent".into(),
            payload: "{}".into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        })
        .unwrap(),
    )
    .unwrap();

    let config = sample_config(&tmp.path().join("docs"), &db);
    config.validate().unwrap();
    let (sql, doc) = user_created_and_failing_mappings();
    let rules = rules_user_created_and_failing();

    let mut iter = events_from_path(&events_dir).unwrap();
    let mut ctx = ReplayContext::new(&config, rules, sql, doc, HashMap::new(), HashMap::new());
    let report = ctx.run_stream(&mut iter).unwrap();

    assert_eq!(report.events_processed, 2, "{:?}", report.per_event);
    assert_eq!(report.events_succeeded, 1);
    assert_eq!(report.events_failed, 1);
    assert!(report.stopped_early);
    assert_eq!(report.per_event[0].status, ExecutionStatus::Succeeded);
    assert_eq!(report.per_event[1].status, ExecutionStatus::Failed);
}

#[test]
fn replay_continue_on_error_processes_all() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("replay.db");
    init_sqlite_users_table(&db);
    fs::create_dir_all(tmp.path().join("docs")).unwrap();

    let events_dir = tmp.path().join("events");
    fs::create_dir_all(&events_dir).unwrap();

    for (name, etype, eid, payload) in [
        ("01.json", "UserCreated", "a", r#"{"id":"x"}"#),
        ("02.json", "FailingEvent", "b", "{}"),
        ("03.json", "UserCreated", "c", r#"{"id":"y"}"#),
    ] {
        fs::write(
            events_dir.join(name),
            serde_json::to_string(&Event {
                event_id: eid.into(),
                event_type: etype.into(),
                payload: payload.into(),
                metadata: HashMap::new(),
                version: 1,
                sequence: 1,
            })
            .unwrap(),
        )
        .unwrap();
    }

    let mut config = sample_config(&tmp.path().join("docs"), &db);
    config.failure_policy = FailurePolicy::ContinueOnError;
    config.validate().unwrap();
    let (sql, doc) = user_created_and_failing_mappings();
    let rules = rules_user_created_and_failing();

    let mut iter = events_from_path(&events_dir).unwrap();
    let mut ctx = ReplayContext::new(&config, rules, sql, doc, HashMap::new(), HashMap::new());
    let report = ctx.run_stream(&mut iter).unwrap();

    assert_eq!(report.events_processed, 3);
    assert_eq!(report.events_succeeded, 2);
    assert_eq!(report.events_failed, 1);
    assert!(!report.stopped_early);
    assert_eq!(report.per_event[2].status, ExecutionStatus::Succeeded);
}

#[test]
fn replay_max_per_event_summaries_caps_success_rows() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("replay.db");
    init_sqlite_users_table(&db);
    fs::create_dir_all(tmp.path().join("docs")).unwrap();

    let events_dir = tmp.path().join("events");
    fs::create_dir_all(&events_dir).unwrap();
    for (i, id) in ["u1", "u2"].iter().enumerate() {
        fs::write(
            events_dir.join(format!("{}.json", i)),
            serde_json::to_string(&Event {
                event_id: format!("e{}", i),
                event_type: "UserCreated".into(),
                payload: format!(r#"{{"id":"{}"}}"#, id),
                metadata: HashMap::new(),
                version: 1,
                sequence: 1,
            })
            .unwrap(),
        )
        .unwrap();
    }

    let config = sample_config(&tmp.path().join("docs"), &db);
    config.validate().unwrap();
    let (sql, doc) = user_created_mappings();
    let rules = rules_user_created();

    let mut iter = events_from_path(&events_dir).unwrap();
    let mut ctx = ReplayContext::new(&config, rules, sql, doc, HashMap::new(), HashMap::new());
    let opts = ReplayRunOptions {
        max_per_event_summaries: Some(1),
        ..Default::default()
    };
    let report = ctx.run_stream_with_options(&mut iter, &opts).unwrap();

    assert_eq!(report.events_processed, 2);
    assert_eq!(report.events_succeeded, 2);
    assert_eq!(report.per_event.len(), 1);
    assert_eq!(report.per_event_summaries_omitted, 1);
}

#[test]
fn replay_validate_routing_fails_unknown_event_type() {
    let tmp = tempfile::tempdir().unwrap();
    let db = tmp.path().join("replay.db");
    init_sqlite_users_table(&db);
    fs::create_dir_all(tmp.path().join("docs")).unwrap();

    let f = tmp.path().join("e.json");
    fs::write(
        &f,
        serde_json::to_string(&Event {
            event_id: "x".into(),
            event_type: "NotInRouting".into(),
            payload: "{}".into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        })
        .unwrap(),
    )
    .unwrap();

    let config = sample_config(&tmp.path().join("docs"), &db);
    config.validate().unwrap();
    let (sql, doc) = user_created_mappings();
    let rules = rules_user_created();

    let mut iter = events_from_path(&f).unwrap();
    let mut ctx = ReplayContext::new(&config, rules, sql, doc, HashMap::new(), HashMap::new());
    let opts = ReplayRunOptions {
        validate_routing: true,
        ..Default::default()
    };
    let report = ctx.run_stream_with_options(&mut iter, &opts).unwrap();

    assert_eq!(report.events_failed, 1);
    assert_eq!(report.per_event[0].status, ExecutionStatus::Failed);
}

/// Same event type can target multiple SQL adapters (e.g. master + replica) with one mapping bundle.
#[test]
fn replay_user_created_writes_two_sqlite_adapters() {
    let tmp = tempfile::tempdir().unwrap();
    let db_master = tmp.path().join("master.db");
    let db_slave = tmp.path().join("slave.db");
    init_sqlite_users_table(&db_master);
    init_sqlite_users_table(&db_slave);

    let config = KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "routing.json".into(),
        },
        adapters: vec![
            AdapterConfig::Sqlite(SqliteAdapterConfig {
                id: "pgsql_master".into(),
                priority: 10,
                config: SqliteConfig {
                    path: db_master.to_string_lossy().into(),
                },
                capabilities: None,
                depends_on: vec![],
            }),
            AdapterConfig::Sqlite(SqliteAdapterConfig {
                id: "pgsql_slave".into(),
                priority: 20,
                config: SqliteConfig {
                    path: db_slave.to_string_lossy().into(),
                },
                capabilities: None,
                depends_on: vec![],
            }),
        ],
        failure_policy: FailurePolicy::FailFast,
        migration_policy: Default::default(),
        sources: Vec::new(),
    };
    config.validate().unwrap();
    let (sql, _) = user_created_mappings();
    let doc = HashMap::new();
    let mut rules = HashMap::new();
    rules.insert(
        "UserCreated".to_string(),
        vec!["pgsql_master".into(), "pgsql_slave".into()],
    );

    let f = tmp.path().join("e.json");
    fs::write(
        &f,
        serde_json::to_string(&Event {
            event_id: "e1".into(),
            event_type: "UserCreated".into(),
            payload: r#"{"id":"u1"}"#.into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        })
        .unwrap(),
    )
    .unwrap();

    let mut ctx = ReplayContext::new(&config, rules, sql, doc, HashMap::new(), HashMap::new());
    let report = ctx.run_stream(events_from_path(&f).unwrap()).unwrap();

    assert_eq!(report.events_succeeded, 1, "{:?}", report.per_event);
    assert_eq!(report.events_failed, 0);

    for db in [&db_master, &db_slave] {
        let c = rusqlite::Connection::open(db).unwrap();
        let n: i64 = c
            .query_row("SELECT COUNT(*) FROM users WHERE id = 'u1'", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(n, 1, "db {:?}", db);
    }
}
