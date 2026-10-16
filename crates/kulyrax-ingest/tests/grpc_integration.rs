//! Phase 20.5: in-process gRPC ingestion tests — a `tonic` server + the
//! reference `kulyrax-client`, no external infrastructure. The engine loop runs
//! on a background thread (it is sync); the client runs on its own tokio
//! runtime (`kulyrax_ingest::start` must not be called from an async context).

use kulyrax_client::{EventEnvelope, Producer};
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::runtime::config::{
    AdapterConfig, KulyraxConfig, RoutingConfig, SqliteAdapterConfig, SqliteConfig,
};
use kulyrax_core::{RunMode, SourceRunOptions, SourceRunReport, StoppedReason};

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
