//! Elasticsearch backend integration scenarios.
//!
//! **Gated on `KULYRAX_ELASTICSEARCH_URL`** (`http://host:port`). When unset
//! every test prints a skip line and returns `Ok` — CI without a running
//! Elasticsearch stays green. Point it at a throwaway single-node cluster
//! (`docker run --rm -p 9200:9200 -e "discovery.type=single-node" -e
//! "xpack.security.enabled=false" docker.elastic.co/elasticsearch/elasticsearch:8.15.0`)
//! to actually exercise these. Mirrors `mongo_backend.rs`'s structure — same
//! skip guard, same fresh-index-per-test isolation.

use kulyrax_adapter_elasticsearch::ElasticsearchAdapter;
use kulyrax_core::adapter::AdapterOutcome;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::document::file::FileDocumentAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

type R = Result<(), Box<dyn std::error::Error>>;

fn es_url() -> Option<String> {
    std::env::var("KULYRAX_ELASTICSEARCH_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

macro_rules! require_es {
    ($url:ident) => {
        let Some($url) = es_url() else {
            eprintln!("SKIP: KULYRAX_ELASTICSEARCH_URL not set");
            return Ok(());
        };
    };
}

static INDEX_SEQ: AtomicU32 = AtomicU32::new(0);

/// A unique index name per test invocation, not yet created — the
/// Elasticsearch analog of `mongo_backend.rs`'s `fresh_collection`.
fn fresh_index_name() -> String {
    format!(
        "t{}-{}",
        std::process::id(),
        INDEX_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

/// A unique, empty index, created with its primary shard active. Left to
/// the first write, the tests' indexes were all auto-created at once, and on
/// a small single-node cluster that took past the adapter's 10 s timeout or
/// answered reads with 503 while the shards started.
fn fresh_index() -> String {
    let idx = fresh_index_name();
    if let Some(url) = es_url() {
        create_index(&url, &idx);
    }
    idx
}

fn create_index(url: &str, idx: &str) {
    let created = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .and_then(|c| {
            c.put(format!("{url}/{idx}?wait_for_active_shards=1&timeout=60s"))
                .json(&serde_json::json!({ "settings": { "number_of_replicas": 0 } }))
                .send()
        })
        .map(|r| r.status());
    assert!(
        matches!(&created, Ok(s) if s.is_success()),
        "create index {idx}: {created:?}"
    );
}

fn es_adapter(url: &str, mappings: HashMap<String, DocumentMapping>) -> ElasticsearchAdapter {
    ElasticsearchAdapter::new(
        "es".into(),
        url,
        None,
        10_000,
        10,
        DocumentRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn doc_mapping(yaml: &str) -> DocumentMapping {
    serde_yaml::from_str(yaml).unwrap()
}

fn ev(event_type: &str, seq: u64, payload: Value) -> Event {
    Event {
        event_id: format!("evt-{event_type}-{seq}"),
        event_type: event_type.into(),
        payload: payload.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

/// Fetch a document's `_source` directly over HTTP — a plain GET, bypassing
/// the adapter, to verify what actually landed.
fn fetch(url: &str, index: &str, id: &str) -> Result<Option<Value>, Box<dyn std::error::Error>> {
    let resp = reqwest::blocking::get(format!("{url}/{index}/_doc/{id}"))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let body: Value = resp.json()?;
    Ok(Some(body["_source"].clone()))
}

// ---------------------------------------------------------------------------

#[test]
fn es_upsert_out_of_order() -> R {
    require_es!(url);
    let idx = fresh_index();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        doc_mapping(&format!(
            "event: U\ncollection: {idx}\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  id: payload.id\n  state: payload.state\n"
        )),
    );
    let a = es_adapter(&url, m);

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

    let found = fetch(&url, &idx, "u1")?.expect("document must exist");
    assert_eq!(
        found["state"], "s3",
        "highest sequence wins regardless of arrival order"
    );
    Ok(())
}

#[test]
fn es_delete_lifecycle() -> R {
    require_es!(url);
    let idx = fresh_index();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        doc_mapping(&format!(
            "event: U\ncollection: {idx}\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  id: payload.id\n  state: payload.state\n"
        )),
    );
    m.insert(
        "D".into(),
        doc_mapping(&format!(
            "event: D\ncollection: {idx}\nversion: 1\nid: payload.id\noperation: delete\ndocument: {{}}\n"
        )),
    );
    let a = es_adapter(&url, m);

    a.handle(&ev("U", 1, serde_json::json!({ "id": "u1", "state": "a" })));
    a.handle(&ev("U", 2, serde_json::json!({ "id": "u1", "state": "b" })));
    let d = a.handle(&ev("D", 3, serde_json::json!({ "id": "u1" })));
    assert!(
        matches!(d.outcome, AdapterOutcome::Deleted),
        "{:?}",
        d.outcome
    );

    // Elasticsearch can't drop the document outright without losing the
    // guard it needs to survive its own deletion (same as every other
    // backend's tombstone) — it clears the content fields instead.
    let found = fetch(&url, &idx, "u1")?.expect("document (now a tombstone) must still exist");
    assert!(
        found.get("state").is_none(),
        "content fields are cleared on delete"
    );
    let guard = &found["__kulyrax_guard"];
    assert_eq!(guard["last_sequence"], 3);
    assert_eq!(guard["deleted"], true);
    Ok(())
}

#[test]
fn es_facet_disjoint() -> R {
    require_es!(url);
    let idx = fresh_index();

    let mut m = HashMap::new();
    m.insert(
        "C".into(),
        doc_mapping(&format!(
            "event: C\ncollection: {idx}\nversion: 1\nid: payload.id\ndocument:\n  id: payload.id\n  name: payload.name\n"
        )),
    );
    m.insert(
        "Email".into(),
        doc_mapping(&format!(
            "event: Email\ncollection: {idx}\nversion: 1\nid: payload.id\nfacet: contact\ndocument:\n  email: payload.email\n"
        )),
    );
    m.insert(
        "Login".into(),
        doc_mapping(&format!(
            "event: Login\ncollection: {idx}\nversion: 1\nid: payload.id\nfacet: activity\ndocument:\n  last_login: payload.at\n"
        )),
    );
    let a = es_adapter(&url, m);

    a.handle(&ev(
        "C",
        1,
        serde_json::json!({ "id": "u1", "name": "Ada" }),
    ));
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

    let found = fetch(&url, &idx, "u1")?.expect("document must exist");
    assert_eq!(found["name"], "Ada");
    assert_eq!(found["email"], "ada@example.com");
    assert_eq!(found["last_login"], "2026-01-02");
    Ok(())
}

/// Two threads racing writes to the same entity must never lose a write or
/// corrupt the guard — one thread's PUT wins the optimistic-concurrency
/// check, the other's gets a 409, retries against the freshly-written
/// document, and resolves to either `Updated` or `Skipped(StaleSequence)`
/// depending on which sequence actually won.
#[test]
fn es_concurrent_writers() -> R {
    require_es!(url);
    let idx = fresh_index();

    let mapping_yaml = format!(
        "event: U\ncollection: {idx}\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  id: payload.id\n  state: payload.state\n"
    );
    let mut m = HashMap::new();
    m.insert("U".into(), doc_mapping(&mapping_yaml));
    let a = Arc::new(es_adapter(&url, m));

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
            "no writer may fail under bounded conflict retry: {outcome:?}"
        );
    }

    let found = fetch(&url, &idx, "u1")?.expect("document must exist");
    assert_eq!(found["__kulyrax_guard"]["last_sequence"], 11);
    assert_eq!(found["state"], "s11");
    Ok(())
}

/// The same event stream, projected into the file-backed store and into
/// Elasticsearch, must converge to identical logical document and guard
/// state — Elasticsearch is behaviourally interchangeable with the
/// file-backed document adapter.
#[test]
fn doc_cross_backend_elasticsearch() -> R {
    require_es!(url);
    let idx = fresh_index();
    let dir = tempfile::tempdir()?;

    let mapping_yaml = format!(
        "event: U\ncollection: {idx}\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  id: payload.id\n  state: payload.state\n"
    );
    let mut m_file = HashMap::new();
    m_file.insert("U".into(), doc_mapping(&mapping_yaml));
    let file_store = FileDocumentAdapter::new(
        "file".into(),
        dir.path().to_path_buf(),
        10,
        DocumentRuntimeBuilder::new(m_file),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let mut m_es = HashMap::new();
    m_es.insert("U".into(), doc_mapping(&mapping_yaml));
    let es_store = es_adapter(&url, m_es);

    for (seq, state) in [(1u64, "a"), (3, "c"), (2, "b")] {
        let payload = serde_json::json!({ "id": "u1", "state": state });
        file_store.handle(&ev("U", seq, payload.clone()));
        es_store.handle(&ev("U", seq, payload));
    }

    let file_value: Value = serde_json::from_str(&std::fs::read_to_string(
        dir.path().join(&idx).join("u1.json"),
    )?)?;
    let es_value = fetch(&url, &idx, "u1")?.expect("document must exist");
    assert_eq!(file_value["state"], es_value["state"]);
    assert_eq!(file_value["id"], es_value["id"]);

    let file_guard: Value = serde_json::from_str(&std::fs::read_to_string(
        dir.path()
            .join(".conduit")
            .join("entities")
            .join(&idx)
            .join("u1.done"),
    )?)?;
    assert_eq!(
        file_guard["last_sequence"], es_value["__kulyrax_guard"]["last_sequence"],
        "both backends converge on the same guard sequence"
    );
    assert_eq!(
        file_guard["deleted"],
        es_value["__kulyrax_guard"]["deleted"]
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// One _bulk request per batch
// ---------------------------------------------------------------------------

/// Upsert, delete and facet mappings on `idx`, under event types `Up` /
/// `Del` / `Mail`.
fn batch_mappings(idx: &str) -> HashMap<String, DocumentMapping> {
    HashMap::from([
        (
            "Up".to_string(),
            doc_mapping(&format!(
                "event: Up\ncollection: {idx}\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  id: payload.id\n  name: payload.name\n"
            )),
        ),
        (
            "Del".to_string(),
            doc_mapping(&format!(
                "event: Del\ncollection: {idx}\nversion: 1\nid: payload.id\noperation: delete\ndocument: {{}}\n"
            )),
        ),
        (
            "Mail".to_string(),
            doc_mapping(&format!(
                "event: Mail\ncollection: {idx}\nversion: 1\nid: payload.id\nfacet: contact\ndocument:\n  email: payload.email\n"
            )),
        ),
    ])
}

/// One entity's lifecycle, a second deleted before it exists, and one written
/// many times, all meant for one batch.
fn batch_stream() -> Vec<Event> {
    use serde_json::json;
    let e = |kind: &str, seq: u64, payload: Value| Event {
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

/// Every document of `idx`, `_source` by id, read in real time.
fn index_state(url: &str, idx: &str) -> Result<Vec<(String, Value)>, Box<dyn std::error::Error>> {
    let mut docs = Vec::new();
    for id in ["hot", "u1", "u2"] {
        if let Some(mut source) = fetch(url, idx, id)? {
            // Event ids carry no index name, so guards compare as they are.
            if let Some(guard) = source.get_mut("__kulyrax_guard") {
                guard.as_object_mut().map(|g| g.remove("processed_at"));
            }
            docs.push((id.to_string(), source));
        }
    }
    Ok(docs)
}

#[test]
fn es_batch_matches_one_event_at_a_time() -> R {
    require_es!(url);
    let (ib, is) = (fresh_index(), fresh_index());
    let events = batch_stream();
    let refs: Vec<&Event> = events.iter().collect();

    let batched = es_adapter(&url, batch_mappings(&ib));
    kulyrax_core::execution::stages::take();
    batched.begin_batch(&refs).map_err(|e| e.to_string())?;
    let t = kulyrax_core::execution::stages::take();
    assert_eq!(t.read.calls, 1, "documents loaded in one _mget");
    let batch_outcomes: Vec<String> = events
        .iter()
        .map(|e| format!("{:?}", batched.handle(e).outcome))
        .collect();
    let read_while_open = fetch(&url, &ib, "u1")?;
    batched.commit_batch().map_err(|e| e.to_string())?;
    let t = kulyrax_core::execution::stages::take();
    assert_eq!(t.commit.calls, 1, "documents sent in one _bulk");
    assert_eq!(read_while_open, None, "nothing is sent before the commit");

    let single = es_adapter(&url, batch_mappings(&is));
    let single_outcomes: Vec<String> = events
        .iter()
        .map(|e| format!("{:?}", single.handle(e).outcome))
        .collect();

    assert_eq!(batch_outcomes, single_outcomes);
    assert!(
        !batch_outcomes.iter().any(|o| o.starts_with("Failed")),
        "{batch_outcomes:?}"
    );
    let state = index_state(&url, &ib)?;
    assert_eq!(state, index_state(&url, &is)?);
    let names: Vec<(&str, &Value)> = state
        .iter()
        .map(|(id, s)| (id.as_str(), &s["name"]))
        .collect();
    assert_eq!(
        names,
        vec![
            ("hot", &Value::from("h20")),
            ("u1", &Value::from("c")),
            ("u2", &Value::from("g")),
        ]
    );
    Ok(())
}

#[test]
fn es_batch_rollback_sends_nothing() -> R {
    require_es!(url);
    // Not created: `begin_batch`'s `_mget` must read a missing index as empty.
    let idx = fresh_index_name();
    let a = es_adapter(&url, batch_mappings(&idx));
    let events = batch_stream();
    let refs: Vec<&Event> = events.iter().collect();

    a.begin_batch(&refs).map_err(|e| e.to_string())?;
    for e in &events {
        assert!(a.handle(e).is_success());
    }
    a.rollback_batch();
    assert_eq!(index_state(&url, &idx)?, vec![], "nothing written");

    // Outside a batch, each event is written on its own again.
    create_index(&url, &idx);
    let r = a.handle(&events[0]);
    assert!(
        matches!(r.outcome, AdapterOutcome::Created),
        "{:?}",
        r.outcome
    );
    assert_eq!(index_state(&url, &idx)?.len(), 1);
    Ok(())
}

/// Another writer changes a document after the batch loaded it: the commit
/// fails on that document's version check (409) instead of overwriting it,
/// and the per-event path the run loop then falls back to applies the
/// batch on top of the newer write.
#[test]
fn es_batch_conflict_fails_the_commit() -> R {
    require_es!(url);
    use serde_json::json;
    let idx = fresh_index();
    let a = es_adapter(&url, batch_mappings(&idx));
    let other = es_adapter(&url, batch_mappings(&idx));
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
    assert!(err.contains("1 of 2 documents failed"), "{err}");
    assert!(err.contains("version_conflict"), "{err}");
    assert_eq!(
        fetch(&url, &idx, "u1")?.map(|d| d["name"].clone()),
        Some(json!("other"))
    );

    // The fallback: one event at a time. `new` already landed and is skipped.
    let outcomes: Vec<String> = events
        .iter()
        .map(|e| format!("{:?}", a.handle(e).outcome))
        .collect();
    assert_eq!(outcomes, vec!["Updated", "Skipped(StaleSequence)"]);
    assert_eq!(
        fetch(&url, &idx, "u1")?.map(|d| d["name"].clone()),
        Some(json!("batch"))
    );
    Ok(())
}
