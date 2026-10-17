//! Phase 20.5: in-process gRPC ingestion tests — a `tonic` server + the
//! reference `kulyrax-client`, no external infrastructure. The engine loop runs
//! on a background thread (it is sync); the client runs on its own tokio
//! runtime (`kulyrax_ingest::start` must not be called from an async context).

use kulyrax_adapter_elasticsearch::ElasticsearchAdapter;
use kulyrax_adapter_postgres::PostgresAdapter;
use kulyrax_client::{EventEnvelope, Producer};
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::runtime::config::{
    AdapterCapability, AdapterConfig, CustomAdapterConfig, FailurePolicy, KulyraxConfig,
    MigrationPolicy, RoutingConfig, SqliteAdapterConfig, SqliteConfig,
};
use kulyrax_core::upcast::UpcasterRegistry;
use kulyrax_core::{RunMode, SourceRunOptions, SourceRunReport, StoppedReason};
use serde_json::Value;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;
use tempfile::TempDir;

// --- fixtures --------------------------------------------------------------

fn init_users(db: &Path) {
    rusqlite::Connection::open(db)
        .unwrap()
        .execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)", [])
        .unwrap();
}

fn config(db: &Path) -> KulyraxConfig {
    KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "r.json".into(),
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
    }
}

fn routing() -> HashMap<String, Vec<String>> {
    HashMap::from([("UserUpserted".to_string(), vec!["sql".to_string()])])
}

fn mappings() -> HashMap<String, SqlMapping> {
    HashMap::from([(
        "UserUpserted".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            "event: UserUpserted\ntable: users\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  name: payload.name\n",
        )
        .unwrap(),
    )])
}

fn env(seq: u64, id: &str, name: &str, position: &str) -> EventEnvelope {
    EventEnvelope {
        event_type: "UserUpserted".into(),
        payload: format!(r#"{{"id":"{id}","name":"{name}"}}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
        position: position.into(),
        event_id: String::new(),
    }
}

fn read_user(db: &Path, id: &str) -> Option<(String, i64)> {
    let c = rusqlite::Connection::open(db).ok()?;
    c.query_row(
        "SELECT u.name, s.last_sequence FROM users u
         JOIN conduit_projection_state s
           ON s.target_table = 'users' AND s.entity_key = json_array(u.id)
         WHERE u.id = ?1",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .ok()
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

/// Server + background engine loop over a single `GrpcSource`.
struct Harness {
    server: Option<kulyrax_ingest::GrpcServer>,
    stop: Arc<AtomicBool>,
    loop_thread: Option<JoinHandle<SourceRunReport>>,
    db: PathBuf,
    endpoint: String,
    _dir: TempDir,
}

impl Harness {
    fn start(dlq: Option<PathBuf>) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db");
        init_users(&db);

        let (server, source) = kulyrax_ingest::start("grpc", "tcp://127.0.0.1:0").unwrap();
        let endpoint = format!("http://{}", server.local_addr.unwrap());

        let cfg = config(&db);
        let stop = Arc::new(AtomicBool::new(false));
        let stop2 = Arc::clone(&stop);
        let loop_thread = std::thread::spawn(move || {
            kulyrax_core::run_sources(
                &cfg,
                routing(),
                mappings(),
                HashMap::new(),
                HashMap::new(),
                HashMap::new(),
                vec![Box::new(source)],
                &SourceRunOptions {
                    mode: RunMode::Continuous {
                        poll_interval: Duration::from_millis(20),
                    },
                    max_batch: 64,
                    retry_budget: 1,
                    dlq_dir: dlq,
                },
                &stop2,
            )
            .unwrap()
        });

        Harness {
            server: Some(server),
            stop,
            loop_thread: Some(loop_thread),
            db,
            endpoint,
            _dir: dir,
        }
    }

    fn shutdown(&mut self) -> SourceRunReport {
        self.stop.store(true, Ordering::Relaxed);
        let report = self.loop_thread.take().unwrap().join().unwrap();
        if let Some(s) = self.server.take() {
            s.shutdown();
        }
        report
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        if self.loop_thread.is_some() {
            self.shutdown();
        }
    }
}

/// Send `events`, then wait (up to 5 s) for an `Ack` whose position is `until`.
async fn stream_until(
    endpoint: &str,
    events: Vec<EventEnvelope>,
    until: &str,
) -> kulyrax_client::Ack {
    let mut p = Producer::connect(endpoint.to_string()).await.unwrap();
    for e in events {
        p.send(e).await.unwrap();
    }
    loop {
        let ack = tokio::time::timeout(Duration::from_secs(5), p.next_ack())
            .await
            .expect("ack timeout")
            .expect("stream ended before ack")
            .expect("rpc status");
        if ack.position == until {
            return ack;
        }
    }
}

// --- tests ---------------------------------------------------------------

#[test]
fn grpc_stream_projects() {
    let mut h = Harness::start(None);
    let ack = rt().block_on(stream_until(
        &h.endpoint,
        vec![
            env(1, "u1", "v1", "pos-1"),
            env(2, "u1", "v2", "pos-2"),
            env(3, "u1", "v3", "pos-3"),
        ],
        "pos-3",
    ));
    assert_eq!(ack.position, "pos-3");
    h.shutdown();
    assert_eq!(read_user(&h.db, "u1"), Some(("v3".to_string(), 3)));
}

#[test]
fn grpc_reconnect_resume() {
    let mut h = Harness::start(None);
    rt().block_on(async {
        // session 1: send [1,2], get ack for pos-2, then drop the client.
        stream_until(
            &h.endpoint,
            vec![env(1, "u1", "v1", "pos-1"), env(2, "u1", "v2", "pos-2")],
            "pos-2",
        )
        .await;
        // session 2: resume from pos-1 — resend [1,2] and add [3].
        stream_until(
            &h.endpoint,
            vec![
                env(1, "u1", "v1", "pos-1"),
                env(2, "u1", "v2", "pos-2"),
                env(3, "u1", "v3", "pos-3"),
            ],
            "pos-3",
        )
        .await;
    });
    h.shutdown();
    // The redelivered 1 & 2 were absorbed by the sequence gate — no double apply.
    assert_eq!(read_user(&h.db, "u1"), Some(("v3".to_string(), 3)));
}

#[test]
fn grpc_backpressure_bounds_the_channel() {
    // No engine loop — nothing drains the receive channel, so `send` must
    // eventually block instead of the buffer growing without bound.
    let (server, _source) = kulyrax_ingest::start("grpc", "tcp://127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", server.local_addr.unwrap());

    // ~1 KB payload so the HTTP/2 flow-control window + the bounded channels
    // fill well before this loop count.
    let big = "x".repeat(1024);
    let sent = rt().block_on(async {
        let p = Producer::connect(endpoint).await.unwrap();
        let mut n = 0u32;
        for i in 0..20_000u64 {
            let e = env(i, "x", &big, "");
            match tokio::time::timeout(Duration::from_millis(250), p.send(e)).await {
                Ok(Ok(())) => n += 1,
                _ => break, // send blocked (backpressure) or errored
            }
        }
        n
    });

    assert!(sent < 20_000, "backpressure never engaged — {sent} sent");
    assert!(sent >= 256, "channel bound looks wrong — only {sent} sent");
    drop(server);
}

#[test]
fn grpc_poison_event_acked() {
    let dlq = {
        let d = std::env::temp_dir().join(format!("kulyrax-dlq-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        d
    };
    let mut h = Harness::start(Some(dlq.clone()));

    let mut poison = env(2, "u2", "bad", "pos-2");
    poison.payload = "this is not json".into();

    let ack = rt().block_on(stream_until(
        &h.endpoint,
        vec![
            env(1, "u1", "v1", "pos-1"),
            poison,
            env(3, "u3", "v3", "pos-3"),
        ],
        "pos-3",
    ));
    assert_eq!(
        ack.position, "pos-3",
        "the batch still acks past the poison event"
    );
    assert_eq!(
        ack.dlq_events, 1,
        "dlq_events is the producer's only signal that the position advanced \
         past a batch with a dead-lettered (not actually projected) event"
    );
    assert_eq!(
        ack.ack_protocol_version, 1,
        "a current server must identify itself as dlq_events-capable"
    );

    let report = h.shutdown();
    assert!(report.events_dlq >= 1, "{report:?}");
    assert_eq!(read_user(&h.db, "u1"), Some(("v1".to_string(), 1)));
    assert_eq!(read_user(&h.db, "u3"), Some(("v3".to_string(), 3)));
    assert_eq!(read_user(&h.db, "u2"), None);

    let parked = std::fs::read_dir(&dlq).map(|d| d.count()).unwrap_or(0);
    assert!(parked >= 1, "poison event should be parked in the DLQ");
    let _ = std::fs::remove_dir_all(&dlq);
}

#[test]
fn grpc_vs_directory_converge() {
    // gRPC
    let mut h = Harness::start(None);
    rt().block_on(stream_until(
        &h.endpoint,
        vec![
            env(1, "u1", "a", "p1"),
            env(3, "u1", "c", "p3"),
            env(2, "u1", "b", "p2"),
        ],
        "p3",
    ));
    h.shutdown();
    let grpc_state = read_user(&h.db, "u1");

    // directory
    let dir = tempfile::tempdir().unwrap();
    let ddb = dir.path().join("d.db");
    init_users(&ddb);
    let events = dir.path().join("events");
    std::fs::create_dir_all(&events).unwrap();
    for (i, (seq, name)) in [(1u64, "a"), (3, "c"), (2, "b")].into_iter().enumerate() {
        let e = kulyrax_core::event::Event {
            event_id: format!("e{i}"),
            event_type: "UserUpserted".into(),
            payload: format!(r#"{{"id":"u1","name":"{name}"}}"#),
            metadata: HashMap::new(),
            version: 1,
            sequence: seq,
        };
        std::fs::write(
            events.join(format!("{i:02}.json")),
            serde_json::to_string(&e).unwrap(),
        )
        .unwrap();
    }
    let src: Box<dyn kulyrax_core::EventSource> = Box::new(
        kulyrax_core::source::directory::DirectorySource::new("d", &events, dir.path().join("st"))
            .unwrap(),
    );
    let stop = AtomicBool::new(false);
    kulyrax_core::run_sources(
        &config(&ddb),
        routing(),
        mappings(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        vec![src],
        &SourceRunOptions {
            mode: RunMode::Once,
            max_batch: 64,
            retry_budget: 0,
            dlq_dir: None,
        },
        &stop,
    )
    .unwrap();

    assert_eq!(grpc_state, read_user(&ddb, "u1"));
    assert_eq!(grpc_state, Some(("c".to_string(), 3)));
}

/// The spec's named "critical failure-injection scenario": one real backend
/// (Postgres) succeeds while a different real backend (Elasticsearch)
/// permanently fails the same event, through the *actual* gRPC
/// poll->dispatch->retry->DLQ->Ack path — not a single poison-JSON event
/// against one adapter, like `grpc_poison_event_acked` above. Gated on both
/// `KULYRAX_PG_URL` and `KULYRAX_ELASTICSEARCH_URL`, same skip-if-unset
/// convention as each adapter's own `*_backend.rs` suite.
#[test]
fn grpc_dlq_count_reflects_a_real_cross_backend_failure() {
    let Some(pg_url) = std::env::var("KULYRAX_PG_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return;
    };
    let Some(es_url) = std::env::var("KULYRAX_ELASTICSEARCH_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        eprintln!("SKIP: KULYRAX_ELASTICSEARCH_URL not set");
        return;
    };

    let table = format!("t_cross_{}", std::process::id());
    let index = format!("idx-cross-{}", std::process::id());

    {
        let mut c = postgres::Client::connect(&pg_url, postgres::NoTls).unwrap();
        c.batch_execute(&format!(
            "DROP TABLE IF EXISTS {table}; \
             CREATE TABLE {table} (id text primary key, name text); \
             DELETE FROM conduit_projection_state WHERE target_table = '{table}'"
        ))
        .unwrap();
    }
    let http = reqwest::blocking::Client::new();
    let _ = http
        .delete(format!("{es_url}/{index}"))
        .send()
        .expect("delete old index");
    // Pre-declare `mileage` as numeric-only, so a string value for it is a
    // genuine, deterministic Elasticsearch mapper_parsing_exception (400) —
    // a permanent failure, not a flaky transient one.
    http.put(format!("{es_url}/{index}"))
        .json(&serde_json::json!({"mappings": {"properties": {"mileage": {"type": "long"}}}}))
        .send()
        .expect("create index with strict mapping")
        .error_for_status()
        .expect("index creation must succeed");

    let sql_mappings = HashMap::from([(
        "VehicleUpserted".to_string(),
        serde_yaml::from_str::<SqlMapping>(&format!(
            "event: VehicleUpserted\ntable: {table}\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  name: payload.name\n"
        ))
        .unwrap(),
    )]);
    let doc_mappings = HashMap::from([(
        "VehicleUpserted".to_string(),
        serde_yaml::from_str::<DocumentMapping>(&format!(
            "event: VehicleUpserted\ncollection: {index}\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  id: payload.id\n  name: payload.name\n  mileage: payload.mileage\n"
        ))
        .unwrap(),
    )]);

    let caps = Some(vec![
        AdapterCapability::Write,
        AdapterCapability::Idempotent,
        AdapterCapability::Upsert,
        AdapterCapability::Delete,
    ]);
    let cfg = KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "r.json".into(),
        },
        adapters: vec![
            AdapterConfig::Custom(CustomAdapterConfig {
                type_name: "postgres".into(),
                id: "pg".into(),
                priority: 10,
                raw: serde_yaml::Value::Null,
                capabilities: caps.clone(),
                depends_on: vec![],
            }),
            AdapterConfig::Custom(CustomAdapterConfig {
                type_name: "elasticsearch".into(),
                id: "es".into(),
                priority: 10,
                raw: serde_yaml::Value::Null,
                capabilities: caps,
                depends_on: vec![],
            }),
        ],
        // ContinueOnError, deliberately not the default FailFast: Postgres
        // and Elasticsearch are independent destinations, and this test's
        // whole point is that one's permanent failure must not prevent the
        // other from being attempted (see the same fix applied to
        // examples/automotive-csv-producer/config.yaml, found via this test).
        failure_policy: FailurePolicy::ContinueOnError,
        migration_policy: Default::default(),
        sources: Vec::new(),
    };
    let routing_rules = HashMap::from([(
        "VehicleUpserted".to_string(),
        vec!["pg".to_string(), "es".to_string()],
    )]);

    let (server, source) = kulyrax_ingest::start("grpc", "tcp://127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", server.local_addr.unwrap());
    let stop = Arc::new(AtomicBool::new(false));
    let stop2 = Arc::clone(&stop);
    let pg_url_thread = pg_url.clone();
    let es_url_thread = es_url.clone();
    // Adapters are built here, inside the thread, rather than passed in —
    // `Box<dyn StorageAdapter>` is not `Send` (the trait has no `Send` bound),
    // so a pre-built adapter can't cross the `thread::spawn` boundary; only
    // the owned strings/maps that build it can.
    let loop_thread = std::thread::spawn(move || {
        let pg = PostgresAdapter::new(
            "pg".into(),
            &pg_url_thread,
            2,
            10,
            SqlRuntimeBuilder::new(sql_mappings),
            Arc::new(UpcasterRegistry::new()),
            MigrationPolicy::default(),
        );
        let es = ElasticsearchAdapter::new(
            "es".into(),
            &es_url_thread,
            None,
            10_000,
            10,
            DocumentRuntimeBuilder::new(doc_mappings),
            Arc::new(UpcasterRegistry::new()),
            MigrationPolicy::default(),
        );
        let adapters: Vec<Box<dyn StorageAdapter>> = vec![Box::new(pg), Box::new(es)];

        kulyrax_core::run_sources_with_adapters(
            &cfg,
            routing_rules,
            adapters,
            vec![Box::new(source)],
            &SourceRunOptions {
                mode: RunMode::Continuous {
                    poll_interval: Duration::from_millis(20),
                },
                max_batch: 64,
                retry_budget: 1,
                dlq_dir: Some(
                    std::env::temp_dir().join(format!("kulyrax-dlq-cross-{}", std::process::id())),
                ),
            },
            &stop2,
        )
        .unwrap()
    });

    let vehicle_env = |seq: u64, id: &str, mileage: Value| EventEnvelope {
        event_type: "VehicleUpserted".into(),
        payload: serde_json::json!({"id": id, "name": "Alice", "mileage": mileage}).to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
        position: format!("pos-{seq}"),
        event_id: String::new(),
    };

    let ack = rt().block_on(stream_until(
        &endpoint,
        vec![
            vehicle_env(1, "v1", serde_json::json!(100)), // numeric mileage: both adapters succeed
            vehicle_env(2, "v2", serde_json::json!("not-a-number")), // string into a `long` field: ES permanently rejects it; Postgres never looks at `mileage`
            vehicle_env(3, "v3", serde_json::json!(300)),            // both succeed again
        ],
        "pos-3",
    ));

    assert_eq!(
        ack.dlq_events, 1,
        "exactly v2's Elasticsearch write was dead-lettered; Postgres never failed"
    );
    assert_eq!(ack.ack_protocol_version, 1);

    stop.store(true, Ordering::Relaxed);
    loop_thread.join().unwrap();
    server.shutdown();

    {
        let mut c = postgres::Client::connect(&pg_url, postgres::NoTls).unwrap();
        let rows = c
            .query(&format!("SELECT id FROM {table} ORDER BY id"), &[])
            .unwrap();
        let ids: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        assert_eq!(
            ids,
            vec!["v1".to_string(), "v2".to_string(), "v3".to_string()],
            "Postgres never failed on any of the three events"
        );
    }
    for (id, should_exist) in [("v1", true), ("v2", false), ("v3", true)] {
        let resp = http
            .get(format!("{es_url}/{index}/_doc/{id}"))
            .send()
            .unwrap();
        assert_eq!(
            resp.status() == reqwest::StatusCode::OK,
            should_exist,
            "elasticsearch document presence for {id}"
        );
    }

    let _ = http.delete(format!("{es_url}/{index}")).send();
}

#[test]
fn grpc_graceful_shutdown_commits_in_flight() {
    let mut h = Harness::start(None);
    rt().block_on(stream_until(
        &h.endpoint,
        vec![env(1, "u1", "v1", "pos-1"), env(2, "u1", "v2", "pos-2")],
        "pos-2",
    ));
    let report = h.shutdown();
    assert!(
        matches!(
            report.stopped_reason,
            StoppedReason::Shutdown | StoppedReason::CaughtUp
        ),
        "{report:?}"
    );
    assert_eq!(read_user(&h.db, "u1"), Some(("v2".to_string(), 2)));
}
