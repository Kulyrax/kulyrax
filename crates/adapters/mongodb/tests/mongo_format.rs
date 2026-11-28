//! Step 5's format marker on MongoDB (decision 2026-11-28-format-versions):
//! `{_id: "guards", version}` in `kulyrax_format`, beside the guard
//! collection, written by an upsert with `$setOnInsert` so concurrent first
//! starts leave one; a newer or unrecognised one is refused before the guard
//! collection is touched.
//!
//! The refusal of a user who may not write the marker (`Unauthorized`) is not
//! tested here: the local and CI MongoDB run without authorization.
//!
//! **Gated on `KULYRAX_MONGO_URL`.** Each test uses a database of its own.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use kulyrax_adapter_mongodb::MongoDbAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::{AdapterError, StorageAdapter};
use kulyrax_core::format::FormatProblem;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;
use mongodb::bson::{Bson, Document, doc};

type R = Result<(), Box<dyn std::error::Error>>;

static SEQ: AtomicU32 = AtomicU32::new(0);

fn fresh_db(client: &mongodb::sync::Client) -> Result<String, Box<dyn std::error::Error>> {
    let db = format!(
        "kx_format_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    client.database(&db).drop().run()?;
    assert!(
        collections(client, &db).is_empty(),
        "the fresh test database {db} is not empty"
    );
    Ok(db)
}

fn collections(client: &mongodb::sync::Client, db: &str) -> Vec<String> {
    let mut names = client
        .database(db)
        .list_collection_names()
        .run()
        .unwrap_or_default();
    names.sort();
    names
}

fn adapter(url: &str, db: &str) -> MongoDbAdapter {
    let m: DocumentMapping = kulyrax_core::yaml::from_str(
        "event: U\ncollection: users\nversion: 1\nid: payload.id\ndocument:\n  v: payload.v\n",
    )
    .expect("mapping");
    MongoDbAdapter::new(
        "mongo".into(),
        url,
        db.into(),
        4,
        10,
        DocumentRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn markers(client: &mongodb::sync::Client, db: &str) -> Vec<Document> {
    client
        .database(db)
        .collection::<Document>("kulyrax_format")
        .find(doc! {})
        .run()
        .map(|c| c.filter_map(Result::ok).collect())
        .unwrap_or_default()
}

fn connect() -> Option<(String, mongodb::sync::Client)> {
    let url = std::env::var("KULYRAX_MONGO_URL").ok()?;
    let client = mongodb::sync::Client::with_uri_str(&url).expect("connect");
    Some((url, client))
}

#[test]
fn a_new_database_gets_one_marker_beside_the_guards() -> R {
    let Some((url, client)) = connect() else {
        eprintln!("SKIP: KULYRAX_MONGO_URL not set");
        return Ok(());
    };
    let db = fresh_db(&client)?;
    adapter(&url, &db).prepare()?;
    adapter(&url, &db).prepare()?;
    assert_eq!(
        markers(&client, &db),
        [doc! { "_id": "guards", "version": 1 }]
    );
    assert!(collections(&client, &db).contains(&"kulyrax_projection_state".to_string()));
    client.database(&db).drop().run()?;
    Ok(())
}

/// A database with guards and no marker (0.12.x): format 1, stamped, guards
/// kept.
#[test]
fn a_0_12_database_is_stamped_as_format_1() -> R {
    let Some((url, client)) = connect() else {
        eprintln!("SKIP: KULYRAX_MONGO_URL not set");
        return Ok(());
    };
    let db = fresh_db(&client)?;
    let guards = client
        .database(&db)
        .collection::<Document>("kulyrax_projection_state");
    guards
        .insert_one(doc! { "target": "users", "entity_key": "u1", "last_sequence": 7_i64 })
        .run()?;
    adapter(&url, &db).prepare()?;
    assert_eq!(
        markers(&client, &db),
        [doc! { "_id": "guards", "version": 1 }]
    );
    assert_eq!(guards.count_documents(doc! {}).run()?, 1);
    client.database(&db).drop().run()?;
    Ok(())
}

#[test]
fn a_newer_or_unknown_marker_is_refused_before_the_guards() -> R {
    let Some((url, client)) = connect() else {
        eprintln!("SKIP: KULYRAX_MONGO_URL not set");
        return Ok(());
    };
    for (version, newer) in [(Bson::Int32(2), true), (Bson::String("two".into()), false)] {
        let db = fresh_db(&client)?;
        client
            .database(&db)
            .collection::<Document>("kulyrax_format")
            .insert_one(doc! { "_id": "guards", "version": version.clone() })
            .run()?;
        let err = adapter(&url, &db).prepare().expect_err("refused");
        let AdapterError::Format(e) = err else {
            panic!("not a format refusal: {err}");
        };
        assert_eq!(
            matches!(
                e.problem,
                FormatProblem::Newer {
                    found: 2,
                    supported: 1
                }
            ),
            newer,
            "{e}"
        );
        assert!(e.store.contains(&db), "{}", e.store);
        assert_eq!(
            collections(&client, &db),
            ["kulyrax_format"],
            "guards untouched"
        );
        client.database(&db).drop().run()?;
    }
    Ok(())
}

/// Change 3: concurrent first starts leave one marker, one version.
#[test]
fn concurrent_first_starts_leave_one_marker() -> R {
    let Some((url, client)) = connect() else {
        eprintln!("SKIP: KULYRAX_MONGO_URL not set");
        return Ok(());
    };
    let db = fresh_db(&client)?;
    let results: Vec<Result<(), String>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|_| s.spawn(|| adapter(&url, &db).prepare().map_err(|e| e.to_string())))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err("panicked".into())))
            .collect()
    });
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(
        markers(&client, &db),
        [doc! { "_id": "guards", "version": 1 }]
    );
    client.database(&db).drop().run()?;
    Ok(())
}

/// Mappings built in code (not loaded from files) that target Kulyrax's own
/// collections, or its fields, are refused at `prepare`, before any
/// connection: no server needed.
#[test]
fn prepare_refuses_a_mapping_on_kulyrax_collections() {
    for (collection, field) in [
        ("kulyrax_format", "v"),
        ("conduit_projection_state", "v"),
        ("users", "__kulyrax_guard"),
    ] {
        let m: DocumentMapping = kulyrax_core::yaml::from_str(&format!(
            "event: U\ncollection: {collection}\nversion: 1\nid: payload.id\ndocument:\n  {field}: payload.v\n"
        ))
        .expect("mapping");
        let a = MongoDbAdapter::new(
            "mongo".into(),
            "mongodb://127.0.0.1:1/?serverSelectionTimeoutMS=100",
            "app".into(),
            1,
            10,
            DocumentRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
            Arc::new(UpcasterRegistry::new()),
            MigrationPolicy::default(),
        );
        let err = a.prepare().expect_err("refused").to_string();
        assert!(err.contains("Kulyrax keeps for itself"), "{err}");
    }
}
