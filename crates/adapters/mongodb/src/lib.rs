//! MongoDB document backend over core's `DocumentBackend` seam. Registers as
//! `type: mongodb` via [`factory`].
//!
//! Documents go to `{database}.{collection}`; guards go to a separate
//! `kulyrax_projection_state` collection. The guard and the target document
//! live in different collections, so no single atomic Mongo operation covers
//! both: each write runs in a client-session transaction.
//!
//! A batch (`begin_batch` … `commit_batch`) is one transaction. It opens at
//! `begin_batch`, which reads every guard the batch needs inside it (one
//! `$in` query per collection), so the transaction's snapshot covers them:
//! a concurrent writer's change to one of those guards still surfaces as a
//! write conflict at commit, as one event at a time. Events are decided in
//! memory; each touched entity's writes collapse to one document operation
//! and one guard write, applied and committed by `commit_batch`.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

mod legacy;

use mongodb::bson::{Document, doc, from_document, to_document};
use mongodb::sync::{Client, ClientSession, Database};
use serde::Deserialize;
use serde_json::{Map, Value};

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
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct MongoDbConfig {
    /// The adapter's id. In YAML it is the entry's `id:`, not a key of the
    /// `config:` block.
    #[serde(skip)]
    pub id: String,
    /// The adapter's priority. In YAML it is the entry's `priority:`.
    #[serde(skip)]
    pub priority: u32,
    /// `mongodb://[user:pass@]host[:port][/?options]`; `${ENV_VAR}` references
    /// are expanded.
    pub url: String,
    pub database: String,
    /// Becomes the `maxPoolSize` connection-string option (default 4).
    #[serde(default)]
    pub pool_size: Option<u32>,
}

impl MongoDbConfig {
    /// Id empty, priority 0, the default pool size.
    pub fn new(url: impl Into<String>, database: impl Into<String>) -> Self {
        Self {
            id: String::new(),
            priority: 0,
            url: url.into(),
            database: database.into(),
            pool_size: None,
        }
    }

    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    pub fn with_pool_size(mut self, pool_size: u32) -> Self {
        self.pool_size = Some(pool_size);
        self
    }
}

/// Shows the URL with its password and secret query parameters hidden.
impl std::fmt::Debug for MongoDbConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MongoDbConfig")
            .field("id", &self.id)
            .field("priority", &self.priority)
            .field("url", &kulyrax_core::redact::redact_url(&self.url))
            .field("database", &self.database)
            .field("pool_size", &self.pool_size)
            .finish()
    }
}

/// Registry factory for `type: mongodb`.
pub fn factory(
    entry: &CustomAdapterConfig,
    ctx: &AdapterBuildContext,
) -> Result<Box<dyn StorageAdapter>, ConfigError> {
    let cfg: MongoDbConfig = entry.backend_config()?;
    let cfg = cfg.with_id(entry.id.clone()).with_priority(entry.priority);
    Ok(Box::new(MongoDbAdapter::from_config(&cfg, ctx)?))
}

impl MongoDbAdapter {
    /// Build the adapter, with the run's mappings, upcasters and migration
    /// policy from `ctx`. Fails on the checks below; an unreachable server
    /// surfaces on the first write.
    pub fn from_config(
        cfg: &MongoDbConfig,
        ctx: &AdapterBuildContext,
    ) -> Result<Self, ConfigError> {
        let id = cfg.id.clone();
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
            return Err(invalid(format!(
                "`url` does not parse: {}",
                kulyrax_core::redact::redact_url(&url)
            )));
        }
        Ok(MongoDbAdapter::new(
            id.clone(),
            &url,
            cfg.database.clone(),
            cfg.pool_size.unwrap_or(4),
            cfg.priority,
            DocumentRuntimeBuilder::new(ctx.mappings.document.clone()),
            Arc::clone(&ctx.upcasters),
            ctx.migration_policy,
        ))
    }
}

/// Dedicated collection for guard documents — kept separate from the user's
/// projected documents, same principle as the SQL guard table.
const GUARD_COLLECTION: &str = "kulyrax_projection_state";

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
    /// The open batch, from `begin_batch` to `commit_batch` / `rollback_batch`.
    batch: Mutex<Option<MongoBatch>>,
}

/// An event's plan and its observed / target schema versions.
struct Planned {
    plan: DocumentPlan,
    source_version: u32,
    projected_version: u32,
}

/// Entity ids per `$in` guard query.
const GUARD_READ_CHUNK: usize = 1_000;

/// One batch: its session, with the transaction open, and every entity it
/// has read or written, by `(collection, entity_id)`, in first-touch order.
struct MongoBatch {
    session: ClientSession,
    entities: HashMap<(String, String), BatchEntity>,
    order: Vec<(String, String)>,
}

#[derive(Default)]
struct BatchEntity {
    guard: Option<ProjectionGuard>,
    guard_dirty: bool,
    doc: DocOp,
}

/// What the batch does to an entity's target document, collapsed over all
/// its events into the one operation that leaves the same document.
#[derive(Default)]
enum DocOp {
    #[default]
    Untouched,
    /// `replace_one`, upserting: the whole document.
    Replace(Map<String, Value>),
    /// `update_one` with `$set`, upserting: facet fields onto whatever the
    /// stored document holds.
    Merge(Map<String, Value>),
    /// `delete_one`.
    Delete,
}

impl DocOp {
    /// The operation after a facet's `$set` of `fields` on top of `self`.
    fn merged(self, fields: &Map<String, Value>) -> DocOp {
        match self {
            DocOp::Untouched => DocOp::Merge(fields.clone()),
            DocOp::Merge(mut m) => {
                m.extend(fields.clone());
                DocOp::Merge(m)
            }
            DocOp::Replace(mut d) => {
                d.extend(fields.clone());
                DocOp::Replace(d)
            }
            // An upserting `$set` after a delete creates a document of the
            // facet's fields alone.
            DocOp::Delete => DocOp::Replace(fields.clone()),
        }
    }
}

fn poisoned() -> AdapterError {
    AdapterError::WriteFailed("MongoDB batch lock poisoned by an earlier panic".to_string())
}

fn parse_guard(doc: Document) -> Result<ProjectionGuard, DocumentError> {
    from_document(doc)
        .map_err(|e| DocumentError::WriteFailed(format!("corrupt guard document: {e}")))
}

impl MongoBatch {
    /// Read the guards of `ids` in `collection`, inside the batch's
    /// transaction.
    fn load(
        &mut self,
        db: &Database,
        collection: &str,
        ids: &[String],
    ) -> Result<(), DocumentError> {
        let guards = db.collection::<Document>(GUARD_COLLECTION);
        for chunk in ids.chunks(GUARD_READ_CHUNK) {
            let mut found: HashMap<String, ProjectionGuard> = HashMap::new();
            let mut cursor = guards
                .find(doc! { "target": collection, "entity_key": { "$in": chunk } })
                .session(&mut self.session)
                .run()
                .map_err(mongo_err)?;
            while let Some(doc) = cursor.next(&mut self.session) {
                let doc = doc.map_err(mongo_err)?;
                let key = doc.get_str("entity_key").map(str::to_string).map_err(|e| {
                    DocumentError::WriteFailed(format!("corrupt guard document: {e}"))
                })?;
                found.insert(key, parse_guard(doc)?);
            }
            for id in chunk {
                let key = (collection.to_string(), id.clone());
                let entity = BatchEntity {
                    guard: found.remove(id),
                    ..BatchEntity::default()
                };
                if self.entities.insert(key.clone(), entity).is_none() {
                    self.order.push(key);
                }
            }
        }
        Ok(())
    }

    /// Apply every touched entity's collapsed writes, then commit.
    fn commit(&mut self, db: &Database) -> Result<(), DocumentError> {
        for key in &self.order {
            let Some(e) = self.entities.get(key) else {
                continue;
            };
            let (collection, entity_id) = key;
            let target = db.collection::<Document>(collection);
            match &e.doc {
                DocOp::Untouched => {}
                DocOp::Replace(fields) => {
                    let mut bson_doc = to_document(fields).map_err(|e| {
                        DocumentError::WriteFailed(format!("failed to serialize document: {e}"))
                    })?;
                    bson_doc.insert("_id", entity_id.as_str());
                    target
                        .replace_one(doc! { "_id": entity_id }, bson_doc)
                        .upsert(true)
                        .session(&mut self.session)
                        .run()
                        .map_err(mongo_err)?;
                }
                DocOp::Merge(fields) => {
                    let set_doc = to_document(fields).map_err(|e| {
                        DocumentError::WriteFailed(format!("failed to serialize facet: {e}"))
                    })?;
                    target
                        .update_one(doc! { "_id": entity_id }, doc! { "$set": set_doc })
                        .upsert(true)
                        .session(&mut self.session)
                        .run()
                        .map_err(mongo_err)?;
                }
                DocOp::Delete => {
                    target
                        .delete_one(doc! { "_id": entity_id })
                        .session(&mut self.session)
                        .run()
                        .map_err(mongo_err)?;
                }
            }
            if e.guard_dirty
                && let Some(guard) = &e.guard
            {
                db.collection::<Document>(GUARD_COLLECTION)
                    .replace_one(
                        doc! { "target": collection, "entity_key": entity_id },
                        MongoBackend::guard_doc(collection, entity_id, guard)?,
                    )
                    .upsert(true)
                    .session(&mut self.session)
                    .run()
                    .map_err(mongo_err)?;
            }
        }
        timed(Stage::Commit, || self.session.commit_transaction().run()).map_err(mongo_err)
    }
}

/// An event inside an open batch: reads and writes the batch's entities in
/// memory. A guard `begin_batch` did not read is read on first use.
struct BatchBackend<'a> {
    batch: &'a mut MongoBatch,
    db: &'a Database,
}

impl BatchBackend<'_> {
    fn entity(
        &mut self,
        collection: &str,
        entity_id: &str,
    ) -> Result<&mut BatchEntity, DocumentError> {
        let key = (collection.to_string(), entity_id.to_string());
        if !self.batch.entities.contains_key(&key) {
            self.batch
                .load(self.db, collection, &[entity_id.to_string()])?;
        }
        self.batch.entities.get_mut(&key).ok_or_else(|| {
            DocumentError::WriteFailed(format!("{collection}/{entity_id} was not loaded"))
        })
    }
}

impl DocumentBackend for BatchBackend<'_> {
    fn read_guard(
        &mut self,
        collection: &str,
        entity_id: &str,
    ) -> Result<Option<ProjectionGuard>, DocumentError> {
        Ok(self.entity(collection, entity_id)?.guard.clone())
    }

    fn write(
        &mut self,
        collection: &str,
        entity_id: &str,
        document: Option<&Value>,
        facet: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError> {
        let e = self.entity(collection, entity_id)?;
        if let Some(document) = document {
            // Both need a JSON object, as `MongoBackend::write`'s BSON
            // conversion does.
            let Some(fields) = document.as_object() else {
                return Err(if facet.is_empty() {
                    DocumentError::WriteFailed(
                        "failed to serialize document: not a JSON object".into(),
                    )
                } else {
                    DocumentError::BuildFailed("facet mapping must resolve to a JSON object".into())
                });
            };
            e.doc = if facet.is_empty() {
                DocOp::Replace(fields.clone())
            } else {
                std::mem::take(&mut e.doc).merged(fields)
            };
        }
        e.guard = Some(guard.clone());
        e.guard_dirty = true;
        Ok(())
    }

    fn delete(
        &mut self,
        collection: &str,
        entity_id: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError> {
        let e = self.entity(collection, entity_id)?;
        e.doc = DocOp::Delete;
        e.guard = Some(guard.clone());
        e.guard_dirty = true;
        Ok(())
    }
}

impl MongoDbAdapter {
    /// `Client` connects lazily, so an unreachable server surfaces on the
    /// first write rather than here.
    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
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
            batch: Mutex::new(None),
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Document, error)
    }

    fn client(&self) -> Result<&Client, AdapterError> {
        self.client
            .as_ref()
            .map_err(|e| AdapterError::WriteFailed(e.clone()))
    }

    /// Build `event`'s plan. `Err` is the finished result to return instead:
    /// a skip or failure for a version with no upcaster chain (per the
    /// migration policy), or a failure to build.
    fn plan(&self, event: &Event) -> Result<Planned, AdapterResult> {
        let projection = match self.builder.build(event, &self.upcasters) {
            Ok(p) => p,
            Err(DocumentError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return Err(match self.migration_policy {
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::Document,
                        SkipReason::UnsupportedVersion,
                        from_version,
                        to_version,
                    ),
                    // `Strict`, and any policy this adapter does not know
                    // (`MigrationPolicy` is `#[non_exhaustive]`): refuse.
                    _ => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::Document,
                        reason,
                        from_version,
                        to_version,
                    ),
                });
            }
            Err(e) => return Err(self.failure(AdapterError::WriteFailed(e.to_string()))),
        };
        Ok(Planned {
            source_version: projection.source_version,
            projected_version: projection.projected_version,
            plan: DocumentPlan::from(projection),
        })
    }

    fn result(
        &self,
        planned: &Planned,
        outcome: Result<DocumentOutcome, DocumentError>,
    ) -> AdapterResult {
        let (id, kind) = (self.id.clone(), StorageKind::Document);
        let (sv, pv) = (planned.source_version, planned.projected_version);
        match outcome {
            Ok(DocumentOutcome::Created) => AdapterResult::created_versioned(id, kind, sv, pv),
            Ok(DocumentOutcome::Updated) => AdapterResult::updated_versioned(id, kind, sv, pv),
            Ok(DocumentOutcome::Deleted) => AdapterResult::deleted_versioned(id, kind, sv, pv),
            Ok(DocumentOutcome::Skipped(reason)) => {
                AdapterResult::skipped_versioned(id, kind, reason, sv, pv)
            }
            // `DocumentOutcome` is `#[non_exhaustive]`: an outcome this
            // adapter does not know is a failure, never a silent success.
            Ok(other) => self.failure(AdapterError::WriteFailed(format!(
                "mongodb: unknown document outcome {other:?}"
            ))),
            Err(DocumentError::WriteConflict) => self.failure(AdapterError::WriteFailed(format!(
                "mongodb write conflict: exceeded {MAX_ATTEMPTS} attempts against a contended entity"
            ))),
            Err(e) => self.failure(AdapterError::WriteFailed(e.to_string())),
        }
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
                    let db = client.database(&self.database);
                    legacy::migrate_guard_collection(client, &db)?;
                    self.ensure_guard_index(&db)
                })
            })
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))
    }

    /// Start the batch's transaction and read, inside it, the guard of every
    /// entity its events touch.
    fn begin_batch(&self, events: &[&Event]) -> Result<(), AdapterError> {
        self.prepared.require().map_err(AdapterError::WriteFailed)?;
        let mut batch = self.batch.lock().map_err(|_| poisoned())?;
        if batch.is_some() {
            return Err(AdapterError::WriteFailed(
                "a batch is already open on this adapter".to_string(),
            ));
        }
        // Events that fail to plan are left to `handle`, which reports them.
        let mut by_collection: Vec<(String, Vec<String>)> = Vec::new();
        let mut seen = HashSet::new();
        for event in events {
            if let Ok(p) = self.plan(event)
                && seen.insert((p.plan.collection.clone(), p.plan.entity_id.clone()))
            {
                match by_collection
                    .iter_mut()
                    .find(|(c, _)| *c == p.plan.collection)
                {
                    Some((_, ids)) => ids.push(p.plan.entity_id),
                    None => by_collection.push((p.plan.collection, vec![p.plan.entity_id])),
                }
            }
        }
        let client = self.client()?;
        let db = client.database(&self.database);
        let opened = (|| {
            let mut session = client.start_session().run().map_err(mongo_err)?;
            session.start_transaction().run().map_err(mongo_err)?;
            let mut open = MongoBatch {
                session,
                entities: HashMap::new(),
                order: Vec::new(),
            };
            let loaded = timed(Stage::Read, || {
                by_collection
                    .iter()
                    .try_for_each(|(collection, ids)| open.load(&db, collection, ids))
            });
            if let Err(e) = loaded {
                let _ = open.session.abort_transaction().run();
                return Err(e);
            }
            Ok(open)
        })();
        *batch = Some(opened.map_err(|e| AdapterError::WriteFailed(e.to_string()))?);
        Ok(())
    }

    fn commit_batch(&self) -> Result<(), AdapterError> {
        let mut batch = self.batch.lock().map_err(|_| poisoned())?;
        let Some(mut open) = batch.take() else {
            return Ok(());
        };
        let db = self.client()?.database(&self.database);
        open.commit(&db).map_err(|e| {
            // Best-effort: the transaction is unusable either way.
            let _ = open.session.abort_transaction().run();
            AdapterError::WriteFailed(e.to_string())
        })
    }

    fn rollback_batch(&self) {
        let Ok(mut batch) = self.batch.lock() else {
            return;
        };
        if let Some(mut open) = batch.take() {
            let _ = open.session.abort_transaction().run();
        }
    }

    fn handle(&self, event: &Event) -> AdapterResult {
        if let Err(e) = self.prepared.require() {
            return self.failure(AdapterError::WriteFailed(e));
        }
        let planned = match self.plan(event) {
            Ok(p) => p,
            Err(result) => return result,
        };
        let plan = &planned.plan;

        let client = match self.client() {
            Ok(c) => c,
            Err(e) => return self.failure(e),
        };
        let db = client.database(&self.database);

        let mut batch = match self.batch.lock() {
            Ok(b) => b,
            Err(_) => return self.failure(poisoned()),
        };
        if let Some(open) = batch.as_mut() {
            let outcome = exec::project(
                &mut BatchBackend {
                    batch: open,
                    db: &db,
                },
                plan,
                event,
            );
            return self.result(&planned, outcome);
        }
        drop(batch);

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
                exec::project(&mut backend, plan, event)
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
        self.result(&planned, result)
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
        let raw = serde_json::to_value(serde_json::json!({
            "type": "mongodb",
            "id": "m1",
            "priority": 10,
            "config": { "url": url, "database": database },
        }))
        .map_err(|e| ConfigError::FactoryConfigInvalid(e.to_string()))?;
        let entry = CustomAdapterConfig::new("mongodb", "m1", 10, raw);
        let ctx = AdapterBuildContext::new(
            Default::default(),
            Arc::new(UpcasterRegistry::new()),
            MigrationPolicy::default(),
        );
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
