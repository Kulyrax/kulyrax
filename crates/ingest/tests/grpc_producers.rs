//! F-02: with several producers on one ingest endpoint, an acknowledgement must
//! reach only the producer whose events it covers. Its position and counts must
//! describe that producer's events alone. A producer resumes from the last
//! acked position after a disconnect (`ingest.proto`, `Ack.position`), so an ack
//! carrying another producer's position tells it that events it has not seen
//! committed are done.
//!
//! In-process: a `tonic` server and the reference `kulyrax-client`, no external
//! infrastructure.

use kulyrax_client::{Ack, EventEnvelope, Producer};
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::runtime::config::{
    AdapterConfig, KulyraxConfig, RoutingConfig, SqliteAdapterConfig, SqliteConfig,
};
use kulyrax_core::{RunMode, SourceRunOptions, SourceRunReport};

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;
use tempfile::TempDir;

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

fn env(seq: u64, id: &str, position: &str) -> EventEnvelope {
    EventEnvelope {
        event_type: "UserUpserted".into(),
        payload: format!(r#"{{"id":"{id}","name":"n{seq}"}}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
        position: position.into(),
        event_id: String::new(),
    }
}

fn poison(position: &str) -> EventEnvelope {
    EventEnvelope {
        payload: "this is not json".into(),
        ..env(1, "bad", position)
    }
}

struct Harness {
    server: Option<kulyrax_ingest::GrpcServer>,
    stop: Arc<AtomicBool>,
    loop_thread: Option<JoinHandle<SourceRunReport>>,
    endpoint: String,
    _dir: TempDir,
}

impl Harness {
    fn start() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("app.db");
        init_users(&db);
        let dlq: PathBuf = dir.path().join("dlq");
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
                    retry_budget: 0,
                    max_batch_delay: Duration::from_millis(200),
                    dlq_dir: Some(dlq),
                },
                &stop2,
            )
            .unwrap()
        });
        Harness {
            server: Some(server),
            stop,
            loop_thread: Some(loop_thread),
            endpoint,
            _dir: dir,
        }
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(t) = self.loop_thread.take() {
            let _ = t.join();
        }
        if let Some(s) = self.server.take() {
            s.shutdown();
        }
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().unwrap()
}

/// Every ack `p` receives within `wait`.
async fn acks_within(p: &mut Producer, wait: Duration) -> Vec<Ack> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + wait;
    while let Ok(Some(Ok(ack))) = tokio::time::timeout_at(deadline, p.next_ack()).await {
        out.push(ack);
    }
    out
}

/// Acks for `p` until one carries `until` (at most 5 s), then any more within
/// `then`.
async fn acks_until(p: &mut Producer, until: &str, then: Duration) -> Vec<Ack> {
    let mut out = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while let Ok(Some(Ok(ack))) = tokio::time::timeout_at(deadline, p.next_ack()).await {
        let done = ack.position == until;
        out.push(ack);
        if done {
            break;
        }
    }
    out.extend(acks_within(p, then).await);
    out
}

fn positions(acks: &[Ack]) -> Vec<&str> {
    acks.iter().map(|a| a.position.as_str()).collect()
}

/// A producer that has sent nothing is acked for nothing.
#[test]
fn an_idle_producer_receives_no_ack_for_another_producers_events() {
    let h = Harness::start();
    let (a_acks, b_acks) = rt().block_on(async {
        let mut b = Producer::connect(h.endpoint.clone()).await.unwrap();
        let mut a = Producer::connect(h.endpoint.clone()).await.unwrap();
        a.send(env(1, "a1", "a-0001")).await.unwrap();
        a.send(env(2, "a1", "a-0002")).await.unwrap();
        let a_acks = acks_until(&mut a, "a-0002", Duration::from_millis(300)).await;
        let b_acks = acks_within(&mut b, Duration::from_millis(500)).await;
        (a_acks, b_acks)
    });
    assert!(
        positions(&a_acks).contains(&"a-0002"),
        "{:?}",
        positions(&a_acks)
    );
    assert!(
        b_acks.is_empty(),
        "B sent nothing, yet was acked: {:?}",
        positions(&b_acks)
    );
}

/// Two producers interleaved, one sending an event that fails: each sees
/// only its own positions, and the dead-letter count lands on the producer
/// whose event it was.
#[test]
fn interleaved_producers_see_only_their_own_positions_and_dead_letters() {
    let h = Harness::start();
    let (a_acks, b_acks) = rt().block_on(async {
        let mut a = Producer::connect(h.endpoint.clone()).await.unwrap();
        let mut b = Producer::connect(h.endpoint.clone()).await.unwrap();
        a.send(env(1, "a1", "a-0001")).await.unwrap();
        b.send(poison("b-0001")).await.unwrap();
        a.send(env(2, "a1", "a-0002")).await.unwrap();
        b.send(env(1, "b1", "b-0002")).await.unwrap();
        let a_acks = acks_until(&mut a, "a-0002", Duration::from_millis(500)).await;
        let b_acks = acks_until(&mut b, "b-0002", Duration::from_millis(500)).await;
        (a_acks, b_acks)
    });
    for ack in &a_acks {
        assert!(
            ack.position.starts_with("a-"),
            "A got B's position: {:?}",
            positions(&a_acks)
        );
        assert_eq!(
            ack.dlq_events, 0,
            "A was told one of its events was dead-lettered: {a_acks:?}"
        );
    }
    for ack in &b_acks {
        assert!(
            ack.position.starts_with("b-"),
            "B got A's position: {:?}",
            positions(&b_acks)
        );
    }
    let b_dlq: u32 = b_acks.iter().map(|a| a.dlq_events).sum();
    assert_eq!(
        b_dlq, 1,
        "B's poison event is counted on B's acks: {b_acks:?}"
    );
}

/// Producers choosing numeric offsets: B, at offset 0005, must not be told
/// 0009 is committed. Resuming from 0009 after a disconnect, it would never
/// resend its own 0005, which Kulyrax has not seen.
#[test]
fn a_producers_position_is_never_advanced_by_another_producers_events() {
    let h = Harness::start();
    let b_acks = rt().block_on(async {
        let mut b = Producer::connect(h.endpoint.clone()).await.unwrap();
        let mut a = Producer::connect(h.endpoint.clone()).await.unwrap();
        a.send(env(1, "a1", "0009")).await.unwrap();
        let _ = acks_until(&mut a, "0009", Duration::ZERO).await;
        acks_within(&mut b, Duration::from_millis(500)).await
    });
    assert!(
        !positions(&b_acks).contains(&"0009"),
        "B was told offset 0009 is committed: {:?}",
        positions(&b_acks)
    );
}
