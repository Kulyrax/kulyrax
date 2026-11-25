//! MongoDB backend integration scenarios.
//!
//! **Gated on `KULYRAX_MONGO_URL`** (`mongodb://[user:pass@]host[:port]`).
//! Every test works in this binary's own database, `kx_run_mongo_backend`
//! (see `mongo_db`). When unset every test prints a skip line and returns
//! `Ok` — the hermetic file-backed document suite stays the default and CI
//! without a MongoDB replica set stays green. Point it at a throwaway
//! single-node replica set (`docker run --rm -p 27017:27017 mongo:7 --replSet
//! rs0`, then `mongosh --eval 'rs.initiate()'`) to actually exercise these —
//! transactions require a replica set; a standalone `mongod` cannot run them.
//!
//! Consolidated into one file, mirroring `pg_backend.rs` (Phase 19.6) and
//! `redis_backend.rs` (Phase 22.5) — every scenario shares the same skip
//! guard and fresh-collection helper, rather than the five files the phase
//! doc sketches.

use kulyrax_adapter_mongodb::MongoDbAdapter;
use kulyrax_core::adapter::AdapterOutcome;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::document::file::FileDocumentAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::event::Event;
use kulyrax_core::execution::stages;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use mongodb::bson::doc;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

type R = Result<(), Box<dyn std::error::Error>>;

fn mongo_url() -> Option<String> {
    std::env::var("KULYRAX_MONGO_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

/// This binary's own database, `kx_run_mongo_backend`, dropped once per run
/// and checked empty before any test uses it, so no test reads or writes the
/// shared database's guard collection. The harness has no
/// end-of-binary hook, so the database is dropped at the start of the next
/// run. Call it only after `require_mongo!`.
fn mongo_db() -> String {
    static DB: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    DB.get_or_init(|| {
        let db = format!("kx_run_{}", env!("CARGO_CRATE_NAME"));
        let url = mongo_url().unwrap_or_else(|| panic!("KULYRAX_MONGO_URL is not set"));
        let client = mongo_client(&url).unwrap_or_else(|e| panic!("connect to MongoDB: {e}"));
        let handle = client.database(&db);
        handle
            .drop()
            .run()
            .unwrap_or_else(|e| panic!("drop the test database {db}: {e}"));
        // Fail loudly on leftovers: the fresh database holds nothing.
        let leftovers = handle
            .list_collection_names()
            .run()
            .unwrap_or_else(|e| panic!("inspect the test database {db}: {e}"));
        assert!(
            leftovers.is_empty(),
            "the fresh test database {db} is not empty: {leftovers:?}"
        );
        db
    })
    .clone()
}

macro_rules! require_mongo {
    ($url:ident) => {
        let Some($url) = mongo_url() else {
            eprintln!("SKIP: KULYRAX_MONGO_URL not set");
            return Ok(());
        };
    };
}

static COLL_SEQ: AtomicU32 = AtomicU32::new(0);

/// A unique collection per test invocation, so the shared database stays
/// isolated (the Mongo analog of `pg_backend.rs`'s `fresh_table`). The
/// process id keeps a rerun against the same server off an earlier run's
/// guards.
fn fresh_collection() -> String {
    format!(
        "t{}-{}",
        std::process::id(),
        COLL_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

fn mongo_client(url: &str) -> Result<mongodb::sync::Client, mongodb::error::Error> {
    mongodb::sync::Client::with_uri_str(url)
}

fn mongo_adapter(
    url: &str,
    database: &str,
    mappings: HashMap<String, DocumentMapping>,
) -> MongoDbAdapter {
    prepared(unprepared_mongo_adapter(url, database, mappings))
}

/// Prepare an adapter the way every write path does before projecting.
fn prepared<A: StorageAdapter>(adapter: A) -> A {
    adapter.prepare().expect("prepare adapter");
    adapter
}

fn unprepared_mongo_adapter(
    url: &str,
    database: &str,
    mappings: HashMap<String, DocumentMapping>,
) -> MongoDbAdapter {
    MongoDbAdapter::new(
        "mongo".into(),
        url,
        database.into(),
        4,
        10,
        DocumentRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn doc_mapping(yaml: &str) -> DocumentMapping {
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
fn mongo_upsert_out_of_order() -> R {
    require_mongo!(url);
    let db = mongo_db();
    let coll = fresh_collection();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        doc_mapping(&format!(
            "event: U\ncollection: {coll}\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  id: payload.id\n  state: payload.state\n"
        )),
    );
    let a = mongo_adapter(&url, &db, m);

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

    let client = mongo_client(&url)?;
    let target = client
        .database(&db)
        .collection::<mongodb::bson::Document>(&coll);
    let found = target
        .find_one(doc! { "_id": "u1" })
        .run()?
        .expect("document must exist");
    assert_eq!(
        found.get_str("state")?,
        "s3",
        "highest sequence wins regardless of arrival order"
    );
    Ok(())
}

#[test]
fn mongo_delete_lifecycle() -> R {
    require_mongo!(url);
    let db = mongo_db();
    let coll = fresh_collection();

    let mut m = HashMap::new();
    m.insert(
        "U".into(),
        doc_mapping(&format!(
            "event: U\ncollection: {coll}\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  id: payload.id\n  state: payload.state\n"
        )),
    );
    m.insert(
        "D".into(),
        doc_mapping(&format!(
            "event: D\ncollection: {coll}\nversion: 1\nid: payload.id\noperation: delete\ndocument: {{}}\n"
        )),
    );
    let a = mongo_adapter(&url, &db, m);

    a.handle(&ev("U", 1, serde_json::json!({ "id": "u1", "state": "a" })));
    a.handle(&ev("U", 2, serde_json::json!({ "id": "u1", "state": "b" })));
    let d = a.handle(&ev("D", 3, serde_json::json!({ "id": "u1" })));
    assert!(
        matches!(d.outcome, AdapterOutcome::Deleted),
        "{:?}",
        d.outcome
    );

    let client = mongo_client(&url)?;
    let db_handle = client.database(&db);
    let target = db_handle.collection::<mongodb::bson::Document>(&coll);
    assert!(
        target.find_one(doc! { "_id": "u1" }).run()?.is_none(),
        "document must be gone after delete"
    );
    let guard_coll = db_handle.collection::<mongodb::bson::Document>("kulyrax_projection_state");
    let guard = guard_coll
        .find_one(doc! { "target": &coll, "entity_key": "u1" })
        .run()?
        .expect("guard document must exist");
    assert_eq!(guard.get_i64("last_sequence")?, 3);
    assert!(guard.get_bool("deleted")?);
    Ok(())
}

#[test]
fn mongo_facet_disjoint() -> R {
    require_mongo!(url);
    let db = mongo_db();
    let coll = fresh_collection();

    let mut m = HashMap::new();
    m.insert(
        "C".into(),
        doc_mapping(&format!(
            "event: C\ncollection: {coll}\nversion: 1\nid: payload.id\ndocument:\n  id: payload.id\n  name: payload.name\n"
        )),
    );
    m.insert(
        "Email".into(),
        doc_mapping(&format!(
            "event: Email\ncollection: {coll}\nversion: 1\nid: payload.id\nfacet: contact\ndocument:\n  email: payload.email\n"
        )),
    );
    m.insert(
        "Login".into(),
        doc_mapping(&format!(
            "event: Login\ncollection: {coll}\nversion: 1\nid: payload.id\nfacet: activity\ndocument:\n  last_login: payload.at\n"
        )),
    );
    let a = mongo_adapter(&url, &db, m);

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

    let client = mongo_client(&url)?;
    let target = client
        .database(&db)
        .collection::<mongodb::bson::Document>(&coll);
    let found = target
        .find_one(doc! { "_id": "u1" })
        .run()?
        .expect("document must exist");
    assert_eq!(found.get_str("name")?, "Ada");
    assert_eq!(found.get_str("email")?, "ada@example.com");
    assert_eq!(found.get_str("last_login")?, "2026-01-02");
    Ok(())
}

/// Two threads racing writes to the same entity must never lose a write or
/// corrupt the guard — one thread's transaction commits, the other's aborts
/// with a `TransientTransactionError`, retries against the freshly-committed
/// guard, and resolves to either `Updated` or `Skipped(StaleSequence)`
/// depending on which sequence actually won (Phase 23.3's ACID guarantee).
#[test]
fn mongo_concurrent_writers() -> R {
    require_mongo!(url);
    let db = mongo_db();
    let coll = fresh_collection();

    let mapping_yaml = format!(
        "event: U\ncollection: {coll}\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  id: payload.id\n  state: payload.state\n"
    );
    let mut m = HashMap::new();
    m.insert("U".into(), doc_mapping(&mapping_yaml));
    let a = Arc::new(mongo_adapter(&url, &db, m));

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
            "no writer may fail under bounded transaction retry: {outcome:?}"
        );
    }

    let client = mongo_client(&url)?;
    let db_handle = client.database(&db);
    let guard_coll = db_handle.collection::<mongodb::bson::Document>("kulyrax_projection_state");
    let guard = guard_coll
        .find_one(doc! { "target": &coll, "entity_key": "u1" })
        .run()?
        .expect("guard document must exist");
    assert_eq!(guard.get_i64("last_sequence")?, 11);
    let target = db_handle.collection::<mongodb::bson::Document>(&coll);
    let found = target
        .find_one(doc! { "_id": "u1" })
        .run()?
        .expect("document must exist");
    assert_eq!(found.get_str("state")?, "s11");
    Ok(())
}

/// The same event stream, projected into the file-backed store and into
/// MongoDB, must converge to identical logical document and guard state —
/// MongoDB is behaviourally interchangeable with the file-backed document
/// adapter.
#[test]
fn doc_cross_backend() -> R {
    require_mongo!(url);
    let db = mongo_db();
    let coll = fresh_collection();
    let dir = tempfile::tempdir()?;

    let mapping_yaml = format!(
        "event: U\ncollection: {coll}\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  id: payload.id\n  state: payload.state\n"
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

    let mut m_mongo = HashMap::new();
    m_mongo.insert("U".into(), doc_mapping(&mapping_yaml));
    let mongo_store = mongo_adapter(&url, &db, m_mongo);

    for (seq, state) in [(1u64, "a"), (3, "c"), (2, "b")] {
        let payload = serde_json::json!({ "id": "u1", "state": state });
        file_store.handle(&ev("U", seq, payload.clone()));
        mongo_store.handle(&ev("U", seq, payload));
    }

    let file_value: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        dir.path().join(&coll).join("u1.json"),
    )?)?;

    let client = mongo_client(&url)?;
    let db_handle = client.database(&db);
    let target = db_handle.collection::<mongodb::bson::Document>(&coll);
    let mongo_doc = target
        .find_one(doc! { "_id": "u1" })
        .run()?
        .expect("document must exist");
    assert_eq!(file_value["state"], mongo_doc.get_str("state")?);
    assert_eq!(file_value["id"], mongo_doc.get_str("id")?);

    let file_guard: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        dir.path()
            .join(".kulyrax")
            .join("entities")
            .join(&coll)
            .join("u1.done"),
    )?)?;
    let guard_coll = db_handle.collection::<mongodb::bson::Document>("kulyrax_projection_state");
    let mongo_guard = guard_coll
        .find_one(doc! { "target": &coll, "entity_key": "u1" })
        .run()?
        .expect("guard document must exist");
    assert_eq!(
        file_guard["last_sequence"].as_u64(),
        Some(mongo_guard.get_i64("last_sequence")? as u64),
        "both backends converge on the same guard sequence"
    );
    assert_eq!(file_guard["deleted"], mongo_guard.get_bool("deleted")?);
    Ok(())
}

fn prep_mapping(coll: &str) -> HashMap<String, DocumentMapping> {
    let mut m = HashMap::new();
    m.insert(
        "Prep".into(),
        doc_mapping(&format!(
            "event: Prep\ncollection: {coll}\nversion: 1\nid: payload.id\ndocument:\n  id: payload.id\n"
        )),
    );
    m
}

/// Index creation is an explicit `prepare` step: before it, `handle` fails and
/// writes nothing.
#[test]
fn mongo_handle_before_prepare_fails_and_writes_nothing() -> R {
    require_mongo!(url);
    let db = mongo_db();
    let coll = fresh_collection();
    let a = unprepared_mongo_adapter(&url, &db, prep_mapping(&coll));

    let r = a.handle(&ev("Prep", 1, serde_json::json!({ "id": "u1" })));
    match &r.outcome {
        AdapterOutcome::Failed(e) => assert!(e.to_string().contains("not prepared"), "{e}"),
        other => panic!("expected a failure, got {other:?}"),
    }
    let written = mongo_client(&url)?
        .database(&db)
        .collection::<mongodb::bson::Document>(&coll)
        .count_documents(doc! {})
        .run()?;
    assert_eq!(written, 0);
    Ok(())
}

#[test]
fn mongo_prepare_does_the_setup_and_events_do_not() -> R {
    require_mongo!(url);
    let db = mongo_db();
    let coll = fresh_collection();
    let a = unprepared_mongo_adapter(&url, &db, prep_mapping(&coll));

    stages::take();
    a.prepare().map_err(|e| e.to_string())?;
    for (seq, id) in [(1u64, "u1"), (1, "u2"), (1, "u3")] {
        let r = a.handle(&ev("Prep", seq, serde_json::json!({ "id": id })));
        assert!(
            matches!(r.outcome, AdapterOutcome::Created),
            "{:?}",
            r.outcome
        );
    }
    assert_eq!(
        stages::take().schema.calls,
        1,
        "only prepare creates the index"
    );
    Ok(())
}

#[test]
fn mongo_prepare_twice_is_idempotent() -> R {
    require_mongo!(url);
    let db = mongo_db();
    let a = mongo_adapter(&url, &db, HashMap::new()); // prepared once by the helper
    a.prepare().map_err(|e| e.to_string())?;
    let guard_indexes = mongo_client(&url)?
        .database(&db)
        .collection::<mongodb::bson::Document>("kulyrax_projection_state")
        .list_indexes()
        .run()?
        .filter_map(Result::ok)
        .filter(|ix| ix.keys == doc! { "target": 1, "entity_key": 1 })
        .count();
    assert_eq!(guard_indexes, 1);
    Ok(())
}

// ---------------------------------------------------------------------------
// One transaction per batch
// ---------------------------------------------------------------------------

/// Upsert, delete and facet mappings on `coll`, under event types `Up` /
/// `Del` / `Mail`.
fn batch_mappings(coll: &str) -> HashMap<String, DocumentMapping> {
    HashMap::from([
        (
            "Up".to_string(),
            doc_mapping(&format!(
                "event: Up\ncollection: {coll}\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  name: payload.name\n"
            )),
        ),
        (
            "Del".to_string(),
            doc_mapping(&format!(
                "event: Del\ncollection: {coll}\nversion: 1\nid: payload.id\noperation: delete\ndocument: {{}}\n"
            )),
        ),
        (
            "Mail".to_string(),
            doc_mapping(&format!(
                "event: Mail\ncollection: {coll}\nversion: 1\nid: payload.id\nfacet: contact\ndocument:\n  email: payload.email\n"
            )),
        ),
    ])
}

fn bev(kind: &str, seq: u64, payload: serde_json::Value) -> Event {
    Event {
        event_id: format!("evt-{kind}-{}-{seq}", payload["id"].as_str().unwrap_or("")),
        ..ev(kind, seq, payload)
    }
}

/// Written before the batch: `pre` and `u3` exist when it starts.
fn seed_stream() -> Vec<Event> {
    use serde_json::json;
    vec![
        bev("Up", 1, json!({ "id": "pre", "name": "stored" })),
        bev("Up", 1, json!({ "id": "u3", "name": "old" })),
    ]
}

/// The ways an entity's writes collapse in a batch: a replace with facets on
/// top, a delete, a facet on a document stored before the batch, a facet
/// after a delete (skipped), and one key written many times.
fn batch_stream() -> Vec<Event> {
    use serde_json::json;
    let mut events = vec![
        bev("Up", 1, json!({ "id": "u1", "name": "a" })),
        bev("Mail", 2, json!({ "id": "u1", "email": "a@x" })),
        bev("Up", 3, json!({ "id": "u1", "name": "b" })),
        bev("Del", 4, json!({ "id": "u1" })),
        bev("Mail", 5, json!({ "id": "u1", "email": "gone@x" })),
        bev("Up", 6, json!({ "id": "u1", "name": "c" })),
        bev("Mail", 7, json!({ "id": "u1", "email": "c@x" })),
        bev("Up", 3, json!({ "id": "u1", "name": "b" })),
        bev("Del", 1, json!({ "id": "u2" })),
        bev("Up", 1, json!({ "id": "u2", "name": "stale" })),
        bev("Up", 2, json!({ "id": "u2", "name": "g" })),
        bev("Mail", 2, json!({ "id": "pre", "email": "pre@x" })),
        bev("Del", 2, json!({ "id": "u3" })),
        bev("Mail", 3, json!({ "id": "u3", "email": "back@x" })),
    ];
    for seq in 1..=20 {
        events.push(bev(
            "Up",
            seq,
            json!({ "id": "hot", "name": format!("h{seq}") }),
        ));
    }
    events
}

type MongoState = Vec<(
    String,
    Option<mongodb::bson::Document>,
    Option<mongodb::bson::Document>,
)>;

/// Target document and guard (without its `_id` and `target`, which differ
/// by collection) of every entity the batch stream touches.
fn coll_state(url: &str, db: &str, coll: &str) -> Result<MongoState, Box<dyn std::error::Error>> {
    let db = mongo_client(url)?.database(db);
    let mut state = Vec::new();
    for id in ["hot", "pre", "u1", "u2", "u3"] {
        let target = db
            .collection::<mongodb::bson::Document>(coll)
            .find_one(doc! { "_id": id })
            .run()?;
        let guard = db
            .collection::<mongodb::bson::Document>("kulyrax_projection_state")
            .find_one(doc! { "target": coll, "entity_key": id })
            .run()?
            .map(|mut g| {
                g.remove("_id");
                g.remove("target");
                g
            });
        state.push((id.to_string(), target, guard));
    }
    Ok(state)
}

#[test]
fn mongo_batch_matches_one_event_at_a_time() -> R {
    require_mongo!(url);
    let db = mongo_db();
    let (cb, cs) = (fresh_collection(), fresh_collection());
    let events = batch_stream();
    let refs: Vec<&Event> = events.iter().collect();

    let batched = mongo_adapter(&url, &db, batch_mappings(&cb));
    for e in seed_stream() {
        assert!(batched.handle(&e).is_success());
    }
    let before = coll_state(&url, &db, &cb)?;
    stages::take();
    batched.begin_batch(&refs).map_err(|e| e.to_string())?;
    let batch_outcomes: Vec<String> = events
        .iter()
        .map(|e| format!("{:?}", batched.handle(e).outcome))
        .collect();
    assert_eq!(
        coll_state(&url, &db, &cb)?,
        before,
        "nothing is visible before the commit"
    );
    batched.commit_batch().map_err(|e| e.to_string())?;
    let t = stages::take();
    assert_eq!(t.commit.calls, 1, "one transaction for the batch");

    let single = mongo_adapter(&url, &db, batch_mappings(&cs));
    for e in seed_stream() {
        assert!(single.handle(&e).is_success());
    }
    let single_outcomes: Vec<String> = events
        .iter()
        .map(|e| format!("{:?}", single.handle(e).outcome))
        .collect();

    assert_eq!(batch_outcomes, single_outcomes);
    assert!(
        !batch_outcomes.iter().any(|o| o.starts_with("Failed")),
        "{batch_outcomes:?}"
    );
    let state = coll_state(&url, &db, &cb)?;
    assert_eq!(state, coll_state(&url, &db, &cs)?);
    let doc_of = |id: &str| {
        state
            .iter()
            .find(|(k, _, _)| k == id)
            .and_then(|(_, d, _)| d.clone())
    };
    assert_eq!(
        doc_of("pre"),
        Some(doc! { "_id": "pre", "name": "stored", "email": "pre@x" }),
        "a facet keeps the stored fields"
    );
    assert_eq!(
        doc_of("u3"),
        None,
        "a facet does not re-create a deleted entity"
    );
    assert_eq!(doc_of("hot"), Some(doc! { "_id": "hot", "name": "h20" }));
    Ok(())
}

#[test]
fn mongo_batch_rollback_leaves_nothing() -> R {
    require_mongo!(url);
    let db = mongo_db();
    let coll = fresh_collection();
    let a = mongo_adapter(&url, &db, batch_mappings(&coll));
    let events = batch_stream();
    let refs: Vec<&Event> = events.iter().collect();

    a.begin_batch(&refs).map_err(|e| e.to_string())?;
    for e in &events {
        assert!(a.handle(e).is_success());
    }
    a.rollback_batch();
    assert!(
        coll_state(&url, &db, &coll)?
            .iter()
            .all(|(_, d, g)| d.is_none() && g.is_none()),
        "nothing written"
    );

    // Outside a batch, each event commits on its own again.
    let r = a.handle(&events[0]);
    assert!(
        matches!(r.outcome, AdapterOutcome::Created),
        "{:?}",
        r.outcome
    );
    Ok(())
}

/// Another writer changes a guard the batch read: the batch's commit fails
/// with a write conflict, nothing of it lands, and the per-event fallback
/// applies it on top.
#[test]
fn mongo_batch_conflict_fails_the_commit() -> R {
    require_mongo!(url);
    use serde_json::json;
    let db = mongo_db();
    let coll = fresh_collection();
    let a = mongo_adapter(&url, &db, batch_mappings(&coll));
    let other = mongo_adapter(&url, &db, batch_mappings(&coll));
    assert!(
        other
            .handle(&ev("Up", 1, json!({ "id": "u1", "name": "seed" })))
            .is_success()
    );

    let events = vec![
        bev("Up", 1, json!({ "id": "new", "name": "n" })),
        bev("Up", 5, json!({ "id": "u1", "name": "batch" })),
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
    assert!(a.commit_batch().is_err(), "the commit must not overwrite");
    let target = mongo_client(&url)?
        .database(&db)
        .collection::<mongodb::bson::Document>(&coll);
    assert_eq!(
        target.find_one(doc! { "_id": "new" }).run()?,
        None,
        "nothing of the batch lands"
    );

    let outcomes: Vec<String> = events
        .iter()
        .map(|e| format!("{:?}", a.handle(e).outcome))
        .collect();
    assert_eq!(outcomes, vec!["Created", "Updated"]);
    assert_eq!(
        target
            .find_one(doc! { "_id": "u1" })
            .run()?
            .and_then(|d| d.get_str("name").ok().map(str::to_string)),
        Some("batch".to_string())
    );
    Ok(())
}
