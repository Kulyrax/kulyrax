//! MongoDB document backend over core's `DocumentBackend` seam. Registers as
//! `type: mongodb` via [`factory`].
//!
//! Documents go to `{database}.{collection}`; guards go to a separate
//! `conduit_projection_state` collection. The guard and the target document
//! live in different collections, so no single atomic Mongo operation covers
//! both: each write runs in a client-session transaction.

use std::sync::Arc;

use mongodb::bson::{Document, doc, from_document, to_document};
use mongodb::sync::{Client, ClientSession, Database};
use serde::Deserialize;
use serde_json::Value;

use kulyrax_core::adapter::document::adapter::DocumentError;
use kulyrax_core::adapter::document::exec::{
    self, DocumentBackend, DocumentOutcome, DocumentPlan, ProjectionGuard,
};
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::{AdapterError, AdapterResult, Prepared, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::execution::stages::{Stage, timed};
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::AdapterBuildContext;
use kulyrax_core::runtime::config::{
    ConfigError, CustomAdapterConfig, MigrationPolicy, expand_env,
};
use kulyrax_core::upcast::UpcasterRegistry;

/// The `config:` block of a `type: mongodb` adapter entry.
#[derive(Debug, Clone, Deserialize)]
pub struct MongoDbConfig {
    /// `mongodb://[user:pass@]host[:port][/?options]`; `${ENV_VAR}` references
    /// are expanded.
    pub url: String,
    pub database: String,
    /// Becomes the `maxPoolSize` connection-string option (default 4).
    #[serde(default)]
    pub pool_size: Option<u32>,
}

/// Registry factory for `type: mongodb`.
pub fn factory(
    entry: &CustomAdapterConfig,
    ctx: &AdapterBuildContext,
) -> Result<Box<dyn StorageAdapter>, ConfigError> {
    let id = entry.id.clone();
    let cfg: MongoDbConfig = entry.backend_config()?;
    let url = expand_env(&cfg.url);
    let invalid = |reason: String| {
        ConfigError::FactoryConfigInvalid(format!("mongodb adapter {id:?}: {reason}"))
    };
    if url.trim().is_empty() {
        return Err(invalid("`url` is empty".into()));
    }
    if cfg.database.trim().is_empty() {
        return Err(invalid("`database` is empty".into()));
    }
    if Client::with_uri_str(&url).is_err() {
        return Err(invalid(format!("`url` does not parse: {url}")));
    }
    Ok(Box::new(MongoDbAdapter::new(
        id.clone(),
        &url,
        cfg.database,
        cfg.pool_size.unwrap_or(4),
        entry.priority,
        DocumentRuntimeBuilder::new(ctx.mappings.document.clone()),
        Arc::clone(&ctx.upcasters),
        ctx.migration_policy,
    )))
}

/// Dedicated collection for guard documents — kept separate from the user's
/// projected documents, same principle as the SQL guard table.
const GUARD_COLLECTION: &str = "conduit_projection_state";

/// Retry budget for a transaction aborted by a concurrent writer.
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

/// Two concurrent writers to the same entity serialize through MongoDB's own
/// transaction conflict detection.
pub struct MongoDbAdapter {
    id: String,
    priority: u32,
    /// `Err` when the URL did not parse; every `handle()` then fails cleanly.
    client: Result<Client, String>,
    database: String,
    builder: DocumentRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
    /// Set by `prepare` once the guard collection's unique index exists.
    prepared: Prepared,
}

impl MongoDbAdapter {
    /// `Client` connects lazily, so an unreachable server surfaces on the
    /// first write rather than here.
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
            prepared: Prepared::default(),
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Document, error)
    }

    /// Create the guard collection's unique compound index on
    /// `(target, entity_key)`. `createIndex` cannot run inside a
    /// multi-document transaction, so `prepare` runs it on its own.
    /// Idempotent server-side (MongoDB no-ops an identical index spec).
    fn ensure_guard_index(&self, db: &Database) -> Result<(), DocumentError> {
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

    fn prepare(&self) -> Result<(), AdapterError> {
        let client = self
            .client
            .as_ref()
            .map_err(|e| AdapterError::WriteFailed(e.clone()))?;
        self.prepared
            .prepare(|| {
                timed(Stage::Schema, || {
                    self.ensure_guard_index(&client.database(&self.database))
                })
            })
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))
    }

    fn handle(&self, event: &Event) -> AdapterResult {
        if let Err(e) = self.prepared.require() {
            return self.failure(AdapterError::WriteFailed(e));
        }
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

        // A concurrent writer's commit aborts ours with a retryable error
        // label. Retry the whole transaction; once the budget is spent, the
        // run loop retries the batch like any transient error.
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
                Ok(o) => match timed(Stage::Commit, || session.commit_transaction().run()) {
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
                // A server-side `$set` on the facet's own top-level fields
                // avoids a read-merge-write round trip.
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

#[cfg(test)]
mod tests {
    use super::*;

    fn build(url: &str, database: &str) -> Result<Box<dyn StorageAdapter>, ConfigError> {
        let raw = serde_yaml::to_value(serde_json::json!({
            "type": "mongodb",
            "id": "m1",
            "priority": 10,
            "config": { "url": url, "database": database },
        }))
        .map_err(|e| ConfigError::FactoryConfigInvalid(e.to_string()))?;
        let entry = CustomAdapterConfig {
            type_name: "mongodb".into(),
            id: "m1".into(),
            priority: 10,
            raw,
            capabilities: None,
            depends_on: Vec::new(),
        };
        let ctx = AdapterBuildContext {
            mappings: Default::default(),
            upcasters: Arc::new(UpcasterRegistry::new()),
            migration_policy: MigrationPolicy::default(),
        };
        factory(&entry, &ctx)
    }

    fn error_of(url: &str, database: &str) -> String {
        build(url, database)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default()
    }

    #[test]
    fn factory_rejects_an_empty_url() {
        assert!(error_of("", "db").contains("`url` is empty"));
    }

    #[test]
    fn factory_rejects_an_empty_database() {
        assert!(error_of("mongodb://127.0.0.1:1", " ").contains("`database` is empty"));
    }

    #[test]
    fn factory_rejects_an_unparseable_url() {
        let err = error_of("not a url", "db");
        assert!(err.contains("`url` does not parse"), "{err}");
    }

    #[test]
    fn factory_builds_without_a_server_because_the_client_connects_lazily() {
        let adapter = build("mongodb://127.0.0.1:1", "db");
        assert_eq!(adapter.map(|a| a.kind()).ok(), Some(StorageKind::Document));
    }
}
