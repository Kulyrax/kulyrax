//! A MongoDB database last used by Kulyrax 0.12 (guard collection
//! `conduit_projection_state`) opens under this version: renamed in place
//! once, every guard lane kept, a tombstone collection left under the old
//! name that rejects every document.
//!
//! **Gated on `KULYRAX_MONGO_URL`.** Each test uses a database of its own.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use kulyrax_adapter_mongodb::MongoDbAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;
use mongodb::bson::{Document, doc};

type R = Result<(), Box<dyn std::error::Error>>;

static SEQ: AtomicU32 = AtomicU32::new(0);

fn fresh_db() -> String {
    format!(
        "kx_rename_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

fn adapter(url: &str, db: &str) -> MongoDbAdapter {
    let m: DocumentMapping = serde_yaml::from_str(
        "event: U\ncollection: users\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  v: payload.v\n",
    )
    .unwrap();
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

fn ev(id: &str, seq: u64) -> Event {
    Event {
        event_id: format!("e-{id}-{seq}"),
        event_type: "U".into(),
        payload: serde_json::json!({ "id": id, "v": seq }).to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn stale(o: &AdapterOutcome) -> bool {
    matches!(o, AdapterOutcome::Skipped(SkipReason::StaleSequence))
}

fn rename(client: &mongodb::sync::Client, db: &str, from: &str, to: &str) -> R {
    client
        .database("admin")
        .run_command(doc! {
            "renameCollection": format!("{db}.{from}"),
            "to": format!("{db}.{to}"),
        })
        .run()?;
    Ok(())
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

/// Lanes written by this version, then put back under the 0.12 name.
fn as_0_12_1(client: &mongodb::sync::Client, url: &str, db: &str) -> R {
    let a = adapter(url, db);
    a.prepare()?;
    let first = a.handle(&ev("u1", 5)).outcome;
    assert!(matches!(first, AdapterOutcome::Created), "{first:?}");
    assert!(matches!(
        a.handle(&ev("u2", 3)).outcome,
        AdapterOutcome::Created
    ));
    drop(a);
    rename(
        client,
        db,
        "kulyrax_projection_state",
        "conduit_projection_state",
    )
}

#[test]
fn a_0_12_1_mongodb_database_opens_with_every_lane() -> R {
    let Ok(url) = std::env::var("KULYRAX_MONGO_URL") else {
        eprintln!("SKIP: KULYRAX_MONGO_URL not set");
        return Ok(());
    };
    let client = mongodb::sync::Client::with_uri_str(&url)?;
    let db = fresh_db();
    as_0_12_1(&client, &url, &db)?;

    let a = adapter(&url, &db);
    a.prepare()?;
    assert!(stale(&a.handle(&ev("u1", 2)).outcome));
    assert!(stale(&a.handle(&ev("u2", 1)).outcome));
    assert!(matches!(
        a.handle(&ev("u1", 9)).outcome,
        AdapterOutcome::Updated
    ));
    let names = collections(&client, &db);
    assert_eq!(
        names,
        [
            "conduit_projection_state",
            "kulyrax_projection_state",
            "users"
        ]
    );
    // The tombstone rejects a 0.12-shaped guard write.
    let write = client
        .database(&db)
        .collection::<Document>("conduit_projection_state")
        .insert_one(doc! { "target": "users", "entity_key": "u9", "last_sequence": 1 })
        .run();
    assert!(write.is_err(), "{write:?}");
    drop(a);

    // A second start is a no-op.
    let a = adapter(&url, &db);
    a.prepare()?;
    assert!(stale(&a.handle(&ev("u1", 3)).outcome));
    assert_eq!(collections(&client, &db), names);
    client.database(&db).drop().run()?;
    Ok(())
}

#[test]
fn both_mongodb_guard_collections_are_refused() -> R {
    let Ok(url) = std::env::var("KULYRAX_MONGO_URL") else {
        eprintln!("SKIP: KULYRAX_MONGO_URL not set");
        return Ok(());
    };
    let client = mongodb::sync::Client::with_uri_str(&url)?;
    let db = fresh_db();
    as_0_12_1(&client, &url, &db)?;
    client
        .database(&db)
        .create_collection("kulyrax_projection_state")
        .run()?;
    let err = adapter(&url, &db).prepare().err().map(|e| e.to_string());
    assert!(
        err.as_deref()
            .is_some_and(|e| e.contains("conduit_projection_state")
                && e.contains("kulyrax_projection_state")),
        "{err:?}"
    );
    client.database(&db).drop().run()?;
    Ok(())
}

/// Killed between the two renames: the guards are under the new name and the
/// tombstone still under its temporary one. The next start finishes.
#[test]
fn a_tombstone_left_under_its_temporary_name_is_moved_into_place() -> R {
    let Ok(url) = std::env::var("KULYRAX_MONGO_URL") else {
        eprintln!("SKIP: KULYRAX_MONGO_URL not set");
        return Ok(());
    };
    let client = mongodb::sync::Client::with_uri_str(&url)?;
    let db = fresh_db();
    {
        let a = adapter(&url, &db);
        a.prepare()?;
        assert!(matches!(
            a.handle(&ev("u1", 5)).outcome,
            AdapterOutcome::Created
        ));
    }
    client
        .database(&db)
        .create_collection("kulyrax_tombstone_tmp")
        .run()?;
    let a = adapter(&url, &db);
    a.prepare()?;
    assert!(stale(&a.handle(&ev("u1", 2)).outcome));
    assert_eq!(
        collections(&client, &db),
        [
            "conduit_projection_state",
            "kulyrax_projection_state",
            "users"
        ]
    );
    client.database(&db).drop().run()?;
    Ok(())
}

/// Several adapters (or processes) start on one 0.12 database at once: every
/// one migrates or finds it migrated, and the lanes survive.
#[test]
fn concurrent_starts_all_migrate() -> R {
    let Ok(url) = std::env::var("KULYRAX_MONGO_URL") else {
        eprintln!("SKIP: KULYRAX_MONGO_URL not set");
        return Ok(());
    };
    let client = mongodb::sync::Client::with_uri_str(&url)?;
    let db = fresh_db();
    as_0_12_1(&client, &url, &db)?;
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
    let a = adapter(&url, &db);
    a.prepare()?;
    assert!(stale(&a.handle(&ev("u1", 2)).outcome));
    assert_eq!(
        collections(&client, &db),
        [
            "conduit_projection_state",
            "kulyrax_projection_state",
            "users"
        ]
    );
    client.database(&db).drop().run()?;
    Ok(())
}
