//! Phase 23.2–23.3: the MongoDB document backend.
//!
//! Same `DocumentMapping` / `decide()` / guard model as the file-backed
//! store — this file supplies only what a driver genuinely owns: a `Client`
//! (already internally pooled — no r2d2 needed, unlike Postgres/Redis), the
//! `{database}.{collection}` target and a dedicated `conduit_projection_state`
//! guard collection, and a client-session transaction wrapping the guard
//! write and the target-document write together (they're two different
//! documents in two different collections, so a single atomic Mongo
//! operation can't cover both).

use std::sync::{Arc, Mutex};

use mongodb::bson::{Document, doc, from_document, to_document};
use mongodb::sync::{Client, ClientSession, Database};
use serde_json::Value;

use super::adapter::DocumentError;
use super::exec::{self, DocumentBackend, DocumentOutcome, DocumentPlan, ProjectionGuard};
use super::runtime::DocumentRuntimeBuilder;
use crate::adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter};
use crate::event::Event;
use crate::routing::StorageKind;
use crate::runtime::config::MigrationPolicy;
use crate::upcast::UpcasterRegistry;

/// Dedicated collection for guard documents — kept separate from the user's
/// projected documents, same principle as the SQL guard table.
const GUARD_COLLECTION: &str = "conduit_projection_state";

/// Bounded retry budget for a transaction aborted by a concurrent writer
/// (Phase 23.3) — the same shape as Phase 22.3's Redis retry loop.
const MAX_ATTEMPTS: u32 = 5;

/// Any Mongo error from inside the transaction (a read, a write, or the
/// final commit) that carries a retryable label becomes `WriteConflict`
/// rather than a hard failure — a write conflict most often surfaces from
/// the individual operation that lost the race, not only from
/// `commit_transaction()`.
fn mongo_err(e: mongodb::error::Error) -> DocumentError {
    if is_retryable(&e) {
        DocumentError::WriteConflict
    } else {
        DocumentError::WriteFailed(e.to_string())
    }
}

/// A MongoDB transaction error that means "a concurrent writer touched the
/// same guard document — retry the whole transaction", per MongoDB's own
/// documented transaction-retry contract. `UnknownTransactionCommitResult`
/// technically only requires retrying the *commit*, but retrying the whole
/// transaction is also safe here (`decide()` is itself sequence-gated and
/// idempotent) and keeps one retry path instead of two.
fn is_retryable(e: &mongodb::error::Error) -> bool {
    e.contains_label("TransientTransactionError")
        || e.contains_label("UnknownTransactionCommitResult")
}

/// MongoDB document adapter (Phase 23). Like Postgres (Phase 19), genuinely
/// ACID: two concurrent writers to the same entity serialize through
/// MongoDB's own transaction conflict detection, not an optimistic
/// WATCH/MULTI/EXEC race (Redis, Phase 22).
pub struct MongoDbAdapter {
    id: String,
    priority: u32,
    /// `Err` (a stored message) when the URL didn't parse — every `handle()`
    /// then fails cleanly rather than the factory being fallible.
    client: Result<Client, String>,
    database: String,
    builder: DocumentRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
    /// Guarded so the unique compound index is created at most once per
    /// adapter instance — `create_index` cannot run inside a transaction.
    guard_index_ensured: Mutex<bool>,
}

impl MongoDbAdapter {
    /// Build the client from a connection URL
    /// (`mongodb://[user:pass@]host[:port]/[?options]`). Infallible — a bad
    /// URL is stored and surfaced per-event, keeping `build_adapters_from_config`
    /// free of a `Result`. `Client` connects lazily (no handshake at
    /// construction), matching Postgres/Redis's "errors surface on the first
    /// real write" pattern.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        url: &str,
        database: String,
        pool_size: u32,
        priority: u32,
        builder: DocumentRuntimeBuilder,
        upcasters: Arc<UpcasterRegistry>,
        migration_policy: MigrationPolicy,
    ) -> Self {
        // MongoDB's own pool lives inside `Client`; `maxPoolSize` is a
        // connection-string option rather than a separate r2d2 pool, so it's
        // applied by appending the query parameter rather than building
        // `ClientOptions` (whose `parse` is async — the adapter path stays
        // synchronous end to end).
        let separator = if url.contains('?') { '&' } else { '?' };
        let url_with_pool = format!("{url}{separator}maxPoolSize={}", pool_size.max(1));
        let client =
            Client::with_uri_str(&url_with_pool).map_err(|e| format!("invalid mongodb url: {e}"));
        Self {
            id,
            priority,
            client,
            database,
            builder,
            upcasters,
            migration_policy,
            guard_index_ensured: Mutex::new(false),
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Document, error)
    }

    /// Create the guard collection's unique compound index on
    /// `(target, entity_key)` at most once — `createIndex` cannot run inside
    /// a multi-document transaction, so this always runs outside one, before
    /// the retry loop starts. Idempotent server-side (MongoDB no-ops an
    /// identical index spec), so a lost race just means two harmless calls.
    fn ensure_guard_index(&self, db: &Database) -> Result<(), DocumentError> {
        let mut ensured = self
            .guard_index_ensured
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if *ensured {
            return Ok(());
        }
        let model = mongodb::IndexModel::builder()
            .keys(doc! { "target": 1, "entity_key": 1 })
            .options(
                mongodb::options::IndexOptions::builder()
                    .unique(true)
                    .build(),
            )
            .build();
        db.collection::<Document>(GUARD_COLLECTION)
            .create_index(model)
            .run()
            .map_err(mongo_err)?;
        *ensured = true;
        Ok(())
    }
}

impl StorageAdapter for MongoDbAdapter {
    fn kind(&self) -> StorageKind {
        StorageKind::Document
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn priority(&self) -> u32 {
        self.priority
    }

    fn handle(&self, event: &Event) -> AdapterResult {
        let projection = match self.builder.build(event, &self.upcasters) {
            Ok(p) => p,
            Err(DocumentError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return match self.migration_policy {
                    MigrationPolicy::Strict => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::Document,
                        reason,
                        from_version,
                        to_version,
                    ),
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::Document,
                        SkipReason::UnsupportedVersion,
                        from_version,
                        to_version,
                    ),
                };
            }
            Err(e) => return self.failure(AdapterError::WriteFailed(e.to_string())),
        };

        let source_version = projection.source_version;
        let projected_version = projection.projected_version;
        let plan = DocumentPlan {
            collection: projection.collection,
            entity_id: projection.entity_id,
            document: projection.document,
            on_existing: projection.on_existing,
            operation: projection.operation,
            permanent: projection.permanent,
            facet: projection.facet,
        };

        let client = match &self.client {
            Ok(c) => c,
            Err(e) => return self.failure(AdapterError::WriteFailed(e.clone())),
        };
        let db = client.database(&self.database);
        if let Err(e) = self.ensure_guard_index(&db) {
            return self.failure(AdapterError::WriteFailed(e.to_string()));
        }

        // Phase 23.3: a client-session transaction wraps the read-decide-write
        // cycle; a concurrent writer's commit causes ours to abort with a
        // retryable error label. Retry the whole transaction, bounded, then
        // surface a batch failure like any other transient error (retried by
        // the run loop — Phase 17.5).
        let mut attempts = 0;
        let result = loop {
            attempts += 1;

            let mut session = match client.start_session().run() {
                Ok(s) => s,
                Err(e) => break Err(mongo_err(e)),
            };
            if let Err(e) = session.start_transaction().run() {
                break Err(mongo_err(e));
            }

            let outcome = {
                let mut backend = MongoBackend {
                    session: &mut session,
                    db: db.clone(),
                };
                exec::project(&mut backend, &plan, event)
            };

            let attempt_result = match outcome {
                Ok(o) => match session.commit_transaction().run() {
                    Ok(()) => Ok(o),
                    Err(e) => Err(mongo_err(e)),
                },
                Err(e) => {
                    // Best-effort — the transaction is already unusable
                    // either way; ignore an abort failure.
                    let _ = session.abort_transaction().run();
                    Err(e)
                }
            };

            match attempt_result {
                Err(DocumentError::WriteConflict) if attempts < MAX_ATTEMPTS => continue,
                other => break other,
            }
        };

        match result {
            Ok(DocumentOutcome::Created) => AdapterResult::created_versioned(
                self.id.clone(),
                StorageKind::Document,
                source_version,
                projected_version,
            ),
            Ok(DocumentOutcome::Updated) => AdapterResult::updated_versioned(
                self.id.clone(),
                StorageKind::Document,
                source_version,
                projected_version,
            ),
            Ok(DocumentOutcome::Deleted) => AdapterResult::deleted_versioned(
                self.id.clone(),
                StorageKind::Document,
                source_version,
                projected_version,
            ),
            Ok(DocumentOutcome::Skipped(reason)) => AdapterResult::skipped_versioned(
                self.id.clone(),
                StorageKind::Document,
                reason,
                source_version,
                projected_version,
            ),
            Err(DocumentError::WriteConflict) => self.failure(AdapterError::WriteFailed(format!(
                "mongodb write conflict: exceeded {MAX_ATTEMPTS} attempts against a contended entity"
            ))),
            Err(e) => self.failure(AdapterError::WriteFailed(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// MongoBackend — the driver
// ---------------------------------------------------------------------------

struct MongoBackend<'a> {
    session: &'a mut ClientSession,
    db: Database,
}

impl MongoBackend<'_> {
    fn guard_collection(&self) -> mongodb::sync::Collection<Document> {
        self.db.collection(GUARD_COLLECTION)
    }

    fn target_collection(&self, name: &str) -> mongodb::sync::Collection<Document> {
        self.db.collection(name)
    }

    /// The guard's BSON form, with `target`/`entity_key` added — `ProjectionGuard`
    /// itself carries neither (they're the caller's identity, not guard state),
    /// but `replace_one` overwrites the whole document with exactly what it's
    /// given, so they must be in the replacement or a second write collides
    /// with every other entity on the unique index (both fields absent = null).
    fn guard_doc(
        collection: &str,
        entity_id: &str,
        guard: &ProjectionGuard,
    ) -> Result<Document, DocumentError> {
        let mut doc = to_document(guard)
            .map_err(|e| DocumentError::WriteFailed(format!("failed to serialize guard: {e}")))?;
        doc.insert("target", collection);
        doc.insert("entity_key", entity_id);
        Ok(doc)
    }
}

impl DocumentBackend for MongoBackend<'_> {
    fn read_guard(
        &mut self,
        collection: &str,
        entity_id: &str,
    ) -> Result<Option<ProjectionGuard>, DocumentError> {
        let found = self
            .guard_collection()
            .find_one(doc! { "target": collection, "entity_key": entity_id })
            .session(&mut *self.session)
            .run()
            .map_err(mongo_err)?;
        match found {
            None => Ok(None),
            Some(doc) => from_document(doc)
                .map(Some)
                .map_err(|e| DocumentError::WriteFailed(format!("corrupt guard document: {e}"))),
        }
    }

    fn write(
        &mut self,
        collection: &str,
        entity_id: &str,
        document: Option<&Value>,
        facet: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError> {
        if let Some(document) = document {
            let target = self.target_collection(collection);
            if facet.is_empty() {
                let mut bson_doc = to_document(document).map_err(|e| {
                    DocumentError::WriteFailed(format!("failed to serialize document: {e}"))
                })?;
                bson_doc.insert("_id", entity_id);
                target
                    .replace_one(doc! { "_id": entity_id }, bson_doc)
                    .upsert(true)
                    .session(&mut *self.session)
                    .run()
                    .map_err(mongo_err)?;
            } else {
                // Phase 23.3: a targeted server-side `$set` on the facet's own
                // top-level fields — no read-merge-write round trip needed.
                let facet_obj = document.as_object().ok_or_else(|| {
                    DocumentError::BuildFailed("facet mapping must resolve to a JSON object".into())
                })?;
                let set_doc = to_document(facet_obj).map_err(|e| {
                    DocumentError::WriteFailed(format!("failed to serialize facet: {e}"))
                })?;
                target
                    .update_one(doc! { "_id": entity_id }, doc! { "$set": set_doc })
                    .upsert(true)
                    .session(&mut *self.session)
                    .run()
                    .map_err(mongo_err)?;
            }
        }

        let guard_doc = Self::guard_doc(collection, entity_id, guard)?;
        self.guard_collection()
            .replace_one(
                doc! { "target": collection, "entity_key": entity_id },
                guard_doc,
            )
            .upsert(true)
            .session(&mut *self.session)
            .run()
            .map_err(mongo_err)?;
        Ok(())
    }

    fn delete(
        &mut self,
        collection: &str,
        entity_id: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError> {
        self.target_collection(collection)
            .delete_one(doc! { "_id": entity_id })
            .session(&mut *self.session)
            .run()
            .map_err(mongo_err)?;

        let guard_doc = Self::guard_doc(collection, entity_id, guard)?;
        self.guard_collection()
            .replace_one(
                doc! { "target": collection, "entity_key": entity_id },
                guard_doc,
            )
            .upsert(true)
            .session(&mut *self.session)
            .run()
            .map_err(mongo_err)?;
        Ok(())
    }
}
