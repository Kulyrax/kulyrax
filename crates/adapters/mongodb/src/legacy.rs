//! Kulyrax 0.12 kept guards in the collection `conduit_projection_state`. The
//! first `prepare` renames it to `kulyrax_projection_state` and leaves a
//! tombstone collection under the old name (`kulyrax_core::adapter::legacy`).

use mongodb::bson::doc;
use mongodb::sync::{Client, Database};

use kulyrax_core::adapter::document::adapter::DocumentError;
use kulyrax_core::adapter::legacy::{
    LEGACY_GUARD_TABLE as LEGACY_GUARD_COLLECTION, LEGACY_TOMBSTONE_TEXT as TOMBSTONE_TEXT,
};

use crate::{GUARD_COLLECTION, mongo_err};

/// The tombstone collection, created under this name first and renamed into
/// the old guard collection's name once the guards have moved.
const TOMBSTONE_TMP: &str = "kulyrax_tombstone_tmp";

/// Rename the 0.12 guard collection to
/// [`GUARD_COLLECTION`] (`renameCollection` is atomic and keeps the indexes)
/// and leave a tombstone under the old name: a collection whose validator
/// rejects every document, so a 0.12 process fails instead of starting on an
/// empty guard collection.
///
/// The tombstone is created as [`TOMBSTONE_TMP`] before the rename and moved
/// into place after it. A kill between the steps leaves one of: the
/// temporary beside the old collection (start over), or the temporary beside
/// the new one (finish the last rename).
///
/// MongoDB has no lock to serialise several processes (or adapters) migrating
/// one database. When a create or rename fails because another migration
/// moved something meanwhile, the state is read again and the migration
/// continues from there, up to [`ATTEMPTS`] times.
pub(crate) fn migrate_guard_collection(
    client: &Client,
    db: &Database,
) -> Result<(), DocumentError> {
    let mut raced = None;
    for _ in 0..ATTEMPTS {
        match attempt(client, db)? {
            None => return Ok(()),
            Some(e) => raced = Some(e),
        }
    }
    Err(raced.unwrap_or_else(|| {
        DocumentError::WriteFailed("guard collection migration did not settle".to_string())
    }))
}

/// Migrations read the state again at most this many times.
const ATTEMPTS: usize = 5;

/// One pass. `Err`: refused (both names exist) or the state could not be read.
/// `Ok(Some(_))`: a create or rename failed, probably because a concurrent
/// migration got there first; read the state again. `Ok(None)`: done.
fn attempt(client: &Client, db: &Database) -> Result<Option<DocumentError>, DocumentError> {
    let names = db.list_collection_names().run().map_err(mongo_err)?;
    let has = |n: &str| names.iter().any(|c| c == n);
    if has(LEGACY_GUARD_COLLECTION) && !is_tombstone(db, LEGACY_GUARD_COLLECTION)? {
        if has(GUARD_COLLECTION) {
            return Err(DocumentError::WriteFailed(format!(
                "database {} has both {LEGACY_GUARD_COLLECTION} (Kulyrax 0.12) and \
                 {GUARD_COLLECTION}; a 0.12 process wrote here after the upgrade, or the old \
                 collection was restored. Keep the one you want, drop or rename the other, \
                 and start again",
                db.name()
            )));
        }
        if !has(TOMBSTONE_TMP)
            && let Err(e) = db
                .create_collection(TOMBSTONE_TMP)
                .validator(doc! {
                    "$jsonSchema": { "description": TOMBSTONE_TEXT, "not": {} }
                })
                .validation_level(mongodb::options::ValidationLevel::Strict)
                .validation_action(mongodb::options::ValidationAction::Error)
                .run()
        {
            return Ok(Some(mongo_err(e)));
        }
        kulyrax_core::crash::crash_point("guard-tombstone-created");
        if let Err(e) = rename(client, db, LEGACY_GUARD_COLLECTION, GUARD_COLLECTION) {
            return Ok(Some(e));
        }
        kulyrax_core::crash::crash_point("guard-table-renamed");
        if let Err(e) = rename(client, db, TOMBSTONE_TMP, LEGACY_GUARD_COLLECTION) {
            return Ok(Some(e));
        }
    } else if has(TOMBSTONE_TMP) {
        // Either killed between the two renames (no tombstone yet: move the
        // temporary into place), or a concurrent start made a second
        // temporary after the migration finished (drop it: it is only ever an
        // empty collection that rejects every document).
        let step = if has(LEGACY_GUARD_COLLECTION) {
            db.collection::<mongodb::bson::Document>(TOMBSTONE_TMP)
                .drop()
                .run()
                .map_err(mongo_err)
        } else {
            rename(client, db, TOMBSTONE_TMP, LEGACY_GUARD_COLLECTION)
        };
        if let Err(e) = step {
            return Ok(Some(e));
        }
    }
    Ok(None)
}

fn rename(client: &Client, db: &Database, from: &str, to: &str) -> Result<(), DocumentError> {
    client
        .database("admin")
        .run_command(doc! {
            "renameCollection": format!("{}.{from}", db.name()),
            "to": format!("{}.{to}", db.name()),
            "dropTarget": false,
        })
        .run()
        .map(|_| ())
        .map_err(mongo_err)
}

/// Whether `name` is the tombstone (its validator carries [`TOMBSTONE_TEXT`]).
fn is_tombstone(db: &Database, name: &str) -> Result<bool, DocumentError> {
    let specs = db
        .list_collections()
        .filter(doc! { "name": name })
        .run()
        .map_err(mongo_err)?;
    for spec in specs {
        let spec = spec.map_err(mongo_err)?;
        let description = spec
            .options
            .validator
            .as_ref()
            .and_then(|v| v.get_document("$jsonSchema").ok())
            .and_then(|s| s.get_str("description").ok());
        if description == Some(TOMBSTONE_TEXT) {
            return Ok(true);
        }
    }
    Ok(false)
}
