//! Redis backend integration scenarios.
//!
//! **Gated on `KULYRAX_REDIS_URL`** (`redis://[:password@]host:port[/db]`).
//! When it is unset every test prints a skip line and returns `Ok` — the
//! hermetic file-backed KV suite stays the default and CI without a Redis
//! server stays green. Point it at a throwaway Redis (`docker run --rm -p
//! 6379:6379 redis:7`) to actually exercise these.
//!
//! Consolidated into one file, mirroring `pg_backend.rs` (Phase 19.6) —
//! every scenario shares the same skip guard and fresh-namespace helper,
//! rather than the five files the phase doc sketches.

use kulyrax_adapter_redis::RedisAdapter;
use kulyrax_core::adapter::AdapterOutcome;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::keyvalue::mapping::KvMapping;
use kulyrax_core::adapter::keyvalue::runtime::KvRuntimeBuilder;
use kulyrax_core::adapter::keyvalue::store::FileKvStore;
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

type R = Result<(), Box<dyn std::error::Error>>;

fn redis_url() -> Option<String> {
    std::env::var("KULYRAX_REDIS_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

macro_rules! require_redis {
    ($url:ident) => {
        let Some($url) = redis_url() else {
            eprintln!("SKIP: KULYRAX_REDIS_URL not set");
            return Ok(());
        };
    };
}

static NS_SEQ: AtomicU32 = AtomicU32::new(0);

/// A unique namespace per test invocation, so the shared keyspace stays
/// isolated (the Redis analog of `pg_backend.rs`'s `fresh_table`). The run
/// token keeps a rerun against the same server off an earlier run's guards.
fn fresh_namespace() -> String {
    format!(
        "t{}-{}",
        run_token(),
        NS_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// The process id and this run's start time. The process id alone repeats:
/// Windows reuses ids soon, and a namespace that met an earlier run's guards
/// made its events stale.
fn run_token() -> &'static str {
    static TOKEN: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    TOKEN.get_or_init(|| {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{}x{}", std::process::id(), nanos % 1_000_000_000_000)
    })
}

fn redis_conn(url: &str) -> Result<redis::Connection, redis::RedisError> {
    redis::Client::open(url)?.get_connection()
}

fn redis_get_raw(url: &str, key: &str) -> Option<String> {
    use redis::Commands;
    let mut conn = redis_conn(url).ok()?;
    conn.get(key).ok()?
}

fn redis_adapter(url: &str, mappings: HashMap<String, KvMapping>) -> RedisAdapter {
    RedisAdapter::new(
        "redis".into(),
        url,
        4,
        10,
        KvRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn kv_mapping(yaml: &str) -> KvMapping {
    kulyrax_core::yaml::from_str(yaml).unwrap()
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
fn redis_upsert_out_of_order() -> R {
    require_redis!(url);
    let ns = fresh_namespace();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        kv_mapping(&format!(
            "event: U\nnamespace: {ns}\nversion: 1\nkey: payload.id\non_existing: replace\nvalue:\n  state: payload.state\n"
        )),
    );
    let a = redis_adapter(&url, m);

    for seq in [3u64, 1, 2] {
        let r = a.handle(&ev(
            "U",
            seq,
            serde_json::json!({ "id": "u1", "state": format!("s{seq}") }),
        ));
        assert!(
            !matches!(r.outcome, AdapterOutcome::Failed(_)),
            "{:?}",
            r.outcome
        );
    }

    let raw = redis_get_raw(&url, &format!("{ns}:u1")).expect("value key must exist");
    let value: serde_json::Value = serde_json::from_str(&raw)?;
    assert_eq!(
        value["state"], "s3",
        "highest sequence wins regardless of arrival order"
    );
    Ok(())
}

#[test]
fn redis_delete_lifecycle() -> R {
    require_redis!(url);
    let ns = fresh_namespace();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        kv_mapping(&format!(
            "event: U\nnamespace: {ns}\nversion: 1\nkey: payload.id\non_existing: replace\nvalue:\n  state: payload.state\n"
        )),
    );
    m.insert(
        "D".into(),
        kv_mapping(&format!(
            "event: D\nnamespace: {ns}\nversion: 1\nkey: payload.id\noperation: delete\nvalue: {{}}\n"
        )),
    );
    let a = redis_adapter(&url, m);

    a.handle(&ev("U", 1, serde_json::json!({ "id": "u1", "state": "a" })));
    a.handle(&ev("U", 2, serde_json::json!({ "id": "u1", "state": "b" })));
    let d = a.handle(&ev("D", 3, serde_json::json!({ "id": "u1" })));
    assert!(
        matches!(d.outcome, AdapterOutcome::Deleted),
        "{:?}",
        d.outcome
    );

    assert!(
        redis_get_raw(&url, &format!("{ns}:u1")).is_none(),
        "value key must be gone after delete"
    );
    let guard_raw =
        redis_get_raw(&url, &format!("__kulyrax:guard:{ns}:u1")).expect("guard key must exist");
    let guard: serde_json::Value = serde_json::from_str(&guard_raw)?;
    assert_eq!(guard["last_sequence"], 3);
    assert_eq!(guard["deleted"], true);
    Ok(())
}

#[test]
fn redis_facet_disjoint() -> R {
    require_redis!(url);
    let ns = fresh_namespace();

    let mut m = HashMap::new();
    m.insert(
        "C".into(),
        kv_mapping(&format!(
            "event: C\nnamespace: {ns}\nversion: 1\nkey: payload.id\nvalue:\n  name: payload.name\n"
        )),
    );
    m.insert(
        "Email".into(),
        kv_mapping(&format!(
            "event: Email\nnamespace: {ns}\nversion: 1\nkey: payload.id\nfacet: contact\nvalue:\n  email: payload.email\n"
        )),
    );
    m.insert(
        "Login".into(),
        kv_mapping(&format!(
            "event: Login\nnamespace: {ns}\nversion: 1\nkey: payload.id\nfacet: activity\nvalue:\n  last_login: payload.at\n"
        )),
    );
    let a = redis_adapter(&url, m);

    a.handle(&ev(
        "C",
        1,
        serde_json::json!({ "id": "u1", "name": "Ada" }),
    ));
    // Two disjoint facets, updated out of order relative to each other —
    // each gates on its own lane, so neither supersedes the other.
    let login = a.handle(&ev(
        "Login",
        5,
        serde_json::json!({ "id": "u1", "at": "2026-01-02" }),
    ));
    let email = a.handle(&ev(
        "Email",
        2,
        serde_json::json!({ "id": "u1", "email": "ada@example.com" }),
    ));
    assert!(!matches!(login.outcome, AdapterOutcome::Failed(_)));
    assert!(!matches!(email.outcome, AdapterOutcome::Failed(_)));

    let raw = redis_get_raw(&url, &format!("{ns}:u1")).expect("value key must exist");
    let value: serde_json::Value = serde_json::from_str(&raw)?;
    assert_eq!(value["name"], "Ada");
    assert_eq!(value["email"], "ada@example.com");
    assert_eq!(value["last_login"], "2026-01-02");
    Ok(())
}

/// Two threads racing writes to the same entity must never lose a write or
/// corrupt the guard — one thread's `EXEC` succeeds, the other's WATCH
/// conflicts, retries against the freshly-committed guard, and resolves to
/// either `Updated` or `Skipped(StaleSequence)` depending on which sequence
/// actually won (Phase 22.3's optimistic-retry guarantee).
#[test]
fn redis_concurrent_writers() -> R {
    require_redis!(url);
    let ns = fresh_namespace();

    let mapping_yaml = format!(
        "event: U\nnamespace: {ns}\nversion: 1\nkey: payload.id\non_existing: replace\nvalue:\n  state: payload.state\n"
    );
    let mut m = HashMap::new();
    m.insert("U".into(), kv_mapping(&mapping_yaml));
    let a = Arc::new(redis_adapter(&url, m));

    // Seed the entity so both racing writers hit the `Update` (replace) path.
    a.handle(&ev(
        "U",
        1,
        serde_json::json!({ "id": "u1", "state": "seed" }),
    ));

    let handles: Vec<_> = [10u64, 11u64]
        .into_iter()
        .map(|seq| {
            let a = Arc::clone(&a);
            std::thread::spawn(move || {
                a.handle(&ev(
                    "U",
                    seq,
                    serde_json::json!({ "id": "u1", "state": format!("s{seq}") }),
                ))
                .outcome
            })
        })
        .collect();

    for h in handles {
        let outcome = h.join().expect("writer thread must not panic");
        assert!(
            !matches!(outcome, AdapterOutcome::Failed(_)),
            "no writer may fail under bounded optimistic retry: {outcome:?}"
        );
    }

    // Highest sequence wins regardless of which thread's EXEC landed first.
    let guard_raw =
        redis_get_raw(&url, &format!("__kulyrax:guard:{ns}:u1")).expect("guard key must exist");
    let guard: serde_json::Value = serde_json::from_str(&guard_raw)?;
    assert_eq!(guard["last_sequence"], 11);
    let raw = redis_get_raw(&url, &format!("{ns}:u1")).expect("value key must exist");
    let value: serde_json::Value = serde_json::from_str(&raw)?;
    assert_eq!(value["state"], "s11");
    Ok(())
}

/// The same event stream, projected into the file-backed store and into
/// Redis, must converge to identical logical value and guard state — Redis
/// is behaviourally interchangeable with the file-backed KV adapter.
#[test]
fn kv_cross_backend() -> R {
    require_redis!(url);
    let ns = fresh_namespace();
    let dir = tempfile::tempdir()?;

    let mapping_yaml = format!(
        "event: U\nnamespace: {ns}\nversion: 1\nkey: payload.id\non_existing: replace\nvalue:\n  state: payload.state\n"
    );
    let mut m_file = HashMap::new();
    m_file.insert("U".into(), kv_mapping(&mapping_yaml));
    let file_store = FileKvStore::new(
        "file".into(),
        dir.path().to_path_buf(),
        10,
        KvRuntimeBuilder::new(m_file),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let mut m_redis = HashMap::new();
    m_redis.insert("U".into(), kv_mapping(&mapping_yaml));
    let redis_store = redis_adapter(&url, m_redis);

    for (seq, state) in [(1u64, "a"), (3, "c"), (2, "b")] {
        let payload = serde_json::json!({ "id": "u1", "state": state });
        file_store.handle(&ev("U", seq, payload.clone()));
        redis_store.handle(&ev("U", seq, payload));
    }

    let file_value: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        dir.path().join(&ns).join("u1.json"),
    )?)?;
    let redis_raw = redis_get_raw(&url, &format!("{ns}:u1")).expect("value key must exist");
    let redis_value: serde_json::Value = serde_json::from_str(&redis_raw)?;
    assert_eq!(
        file_value, redis_value,
        "both backends converge on the same value"
    );

    let file_guard: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        dir.path()
            .join(".kulyrax")
            .join("kv")
            .join(&ns)
            .join("u1.guard.json"),
    )?)?;
    let redis_guard_raw =
        redis_get_raw(&url, &format!("__kulyrax:guard:{ns}:u1")).expect("guard key must exist");
    let redis_guard: serde_json::Value = serde_json::from_str(&redis_guard_raw)?;
    assert_eq!(
        file_guard["last_sequence"], redis_guard["last_sequence"],
        "both backends converge on the same guard sequence"
    );
    assert_eq!(file_guard["deleted"], redis_guard["deleted"]);
    Ok(())
}

// ---------------------------------------------------------------------------
// One MULTI/EXEC per batch
// ---------------------------------------------------------------------------

/// Upsert, delete and facet mappings on `ns`, under event types `Up` / `Del`
/// / `Mail`.
fn batch_mappings(ns: &str) -> HashMap<String, KvMapping> {
    HashMap::from([
        (
            "Up".to_string(),
            kv_mapping(&format!(
                "event: Up\nnamespace: {ns}\nversion: 1\nkey: payload.id\non_existing: replace\nvalue:\n  name: payload.name\n"
            )),
        ),
        (
            "Del".to_string(),
            kv_mapping(&format!(
                "event: Del\nnamespace: {ns}\nversion: 1\nkey: payload.id\noperation: delete\nvalue: {{}}\n"
            )),
        ),
        (
            "Mail".to_string(),
            kv_mapping(&format!(
                "event: Mail\nnamespace: {ns}\nversion: 1\nkey: payload.id\nfacet: contact\nvalue:\n  email: payload.email\n"
            )),
        ),
    ])
}

/// One entity's lifecycle, a second deleted before it exists, and one written
/// many times, all meant for one batch.
fn batch_stream() -> Vec<Event> {
    use serde_json::json;
    let e = |kind: &str, seq: u64, payload: serde_json::Value| Event {
        event_id: format!("evt-{kind}-{}-{seq}", payload["id"].as_str().unwrap_or("")),
        ..ev(kind, seq, payload)
    };
    let mut events = vec![
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
    ];
    for seq in 1..=20 {
        events.push(e(
            "Up",
            seq,
            json!({ "id": "hot", "name": format!("h{seq}") }),
        ));
    }
    events
}

type KvState = Vec<(String, Option<serde_json::Value>, Option<serde_json::Value>)>;

/// Value and guard of each entity the batch stream touches.
fn ns_state(url: &str, ns: &str) -> KvState {
    let json = |raw: Option<String>| raw.and_then(|s| serde_json::from_str(&s).ok());
    ["hot", "u1", "u2"]
        .iter()
        .map(|key| {
            (
                key.to_string(),
                json(redis_get_raw(url, &format!("{ns}:{key}"))),
                json(redis_get_raw(url, &format!("__kulyrax:guard:{ns}:{key}"))),
            )
        })
        .collect()
}

#[test]
fn redis_batch_matches_one_event_at_a_time() -> R {
    require_redis!(url);
    let (nb, ns) = (fresh_namespace(), fresh_namespace());
    let events = batch_stream();
    let refs: Vec<&Event> = events.iter().collect();

    let batched = redis_adapter(&url, batch_mappings(&nb));
    batched.begin_batch(&refs).map_err(|e| e.to_string())?;
    let batch_outcomes: Vec<String> = events
        .iter()
        .map(|e| format!("{:?}", batched.handle(e).outcome))
        .collect();
    let while_open = ns_state(&url, &nb);
    kulyrax_core::execution::stages::take();
    batched.commit_batch().map_err(|e| e.to_string())?;
    let t = kulyrax_core::execution::stages::take();
    assert_eq!(t.commit.calls, 1, "one MULTI/EXEC for the batch");
    assert!(
        while_open
            .iter()
            .all(|(_, v, g)| v.is_none() && g.is_none()),
        "nothing is sent before the commit: {while_open:?}"
    );

    let single = redis_adapter(&url, batch_mappings(&ns));
    let single_outcomes: Vec<String> = events
        .iter()
        .map(|e| format!("{:?}", single.handle(e).outcome))
        .collect();

    assert_eq!(batch_outcomes, single_outcomes);
    assert!(
        !batch_outcomes.iter().any(|o| o.starts_with("Failed")),
        "{batch_outcomes:?}"
    );
    let state = ns_state(&url, &nb);
    assert_eq!(state, ns_state(&url, &ns));
    let names: Vec<Option<serde_json::Value>> = state
        .iter()
        .map(|(_, v, _)| v.as_ref().map(|v| v["name"].clone()))
        .collect();
    assert_eq!(
        names,
        vec![Some("h20".into()), Some("c".into()), Some("g".into())]
    );
    Ok(())
}

#[test]
fn redis_batch_rollback_leaves_nothing() -> R {
    require_redis!(url);
    let ns = fresh_namespace();
    let a = redis_adapter(&url, batch_mappings(&ns));
    let events = batch_stream();
    let refs: Vec<&Event> = events.iter().collect();

    a.begin_batch(&refs).map_err(|e| e.to_string())?;
    for e in &events {
        assert!(a.handle(e).is_success());
    }
    a.rollback_batch();
    assert!(
        ns_state(&url, &ns)
            .iter()
            .all(|(_, v, g)| v.is_none() && g.is_none()),
        "nothing written"
    );

    // Outside a batch, each event is written on its own again.
    let r = a.handle(&events[0]);
    assert!(
        matches!(r.outcome, AdapterOutcome::Created),
        "{:?}",
        r.outcome
    );
    Ok(())
}

/// Another writer changes an entity after the batch read it: `EXEC` aborts,
/// nothing of the batch lands, and the per-event fallback applies it on top.
#[test]
fn redis_batch_conflict_aborts_the_whole_batch() -> R {
    require_redis!(url);
    use serde_json::json;
    let ns = fresh_namespace();
    let a = redis_adapter(&url, batch_mappings(&ns));
    let other = redis_adapter(&url, batch_mappings(&ns));
    assert!(
        other
            .handle(&ev("Up", 1, json!({ "id": "u1", "name": "seed" })))
            .is_success()
    );

    let events = vec![
        ev("Up", 5, json!({ "id": "u1", "name": "batch" })),
        ev("Up", 1, json!({ "id": "new", "name": "n" })),
    ];
    let refs: Vec<&Event> = events.iter().collect();
    a.begin_batch(&refs).map_err(|e| e.to_string())?;
    for e in &events {
        assert!(a.handle(e).is_success());
    }
    assert!(
        other
            .handle(&ev("Up", 3, json!({ "id": "u1", "name": "other" })))
            .is_success()
    );
    let err = a
        .commit_batch()
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("another writer changed a guard"), "{err}");
    assert_eq!(
        redis_get_raw(&url, &format!("{ns}:new")),
        None,
        "nothing of the batch lands"
    );

    let outcomes: Vec<String> = events
        .iter()
        .map(|e| format!("{:?}", a.handle(e).outcome))
        .collect();
    assert_eq!(outcomes, vec!["Updated", "Created"]);
    let u1: serde_json::Value =
        serde_json::from_str(&redis_get_raw(&url, &format!("{ns}:u1")).unwrap_or_default())?;
    assert_eq!(u1["name"], "batch");
    Ok(())
}

/// Keys are `<namespace>:<key>` and guards live under `__kulyrax`: a
/// namespace with `:` or that prefix could reach another namespace's keys.
#[test]
fn redis_namespaces_that_could_meet_other_keys_are_refused() -> R {
    require_redis!(url);
    for ns in ["a:b", "__kulyrax_guard", ""] {
        let mut m = HashMap::new();
        m.insert(
            "U".into(),
            kv_mapping(&format!(
                "event: U\nnamespace: '{ns}'\nversion: 1\nkey: payload.id\non_existing: replace\nvalue:\n  state: payload.state\n"
            )),
        );
        let err = redis_adapter(&url, m)
            .prepare()
            .err()
            .map(|e| e.to_string());
        assert!(
            err.as_deref()
                .is_some_and(|e| e.contains("namespaces must be")),
            "{ns:?}: {err:?}"
        );
    }
    Ok(())
}
