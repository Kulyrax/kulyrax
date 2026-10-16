//! Phase 23.5: MongoDB backend integration scenarios.
//!
//! **Gated on `KULYRAX_MONGO_URL`** (`mongodb://[user:pass@]host[:port]`,
//! optionally `KULYRAX_MONGO_DB` for the database name, default
//! `kulyrax_test`). When unset every test prints a skip line and returns
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

use kulyrax_core::adapter::AdapterOutcome;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::document::file::FileDocumentAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::mongo::MongoDbAdapter;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::event::Event;
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

fn mongo_db() -> String {
    std::env::var("KULYRAX_MONGO_DB").unwrap_or_else(|_| "kulyrax_test".into())
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
/// isolated (the Mongo analog of `pg_backend.rs`'s `fresh_table`).
fn fresh_collection() -> String {
    format!("t{}", COLL_SEQ.fetch_add(1, Ordering::Relaxed))
}

fn mongo_client(url: &str) -> Result<mongodb::sync::Client, mongodb::error::Error> {
    mongodb::sync::Client::with_uri_str(url)
}

fn mongo_adapter(
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
    serde_yaml::from_str(yaml).unwrap()
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
    let guard_coll = db_handle.collection::<mongodb::bson::Document>("conduit_projection_state");
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
    let guard_coll = db_handle.collection::<mongodb::bson::Document>("conduit_projection_state");
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
            .join(".conduit")
            .join("entities")
            .join(&coll)
            .join("u1.done"),
    )?)?;
    let guard_coll = db_handle.collection::<mongodb::bson::Document>("conduit_projection_state");
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
