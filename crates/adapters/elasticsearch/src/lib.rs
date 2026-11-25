//! Elasticsearch document backend for Kulyrax — a [`StorageAdapter`] /
//! [`DocumentBackend`] extension crate, following the same pattern as
//! `kulyrax-adapter-postgres` (Phase 28) and the in-tree MongoDB adapter.
//!
//! **Storage shape.** Elasticsearch has no multi-document transactions, so
//! unlike MongoDB (a separate guard collection, tied together with a
//! client-session transaction) this adapter keeps the guard and the
//! projected document in **one** Elasticsearch document, under a reserved
//! top-level key (`__kulyrax_guard`, matching the `__ConduitGuard` /
//! `__KulyraxGuard` naming convention the Neo4j adapter already uses for the
//! same purpose). A single index/update call is then atomic by construction
//! — there is no second document to keep in sync. The cost: a "delete" can't
//! remove the Elasticsearch document outright (that would destroy the guard
//! it needs to survive its own deletion, exactly like every other backend's
//! tombstone), so it clears the projected fields and leaves the document
//! behind with `__kulyrax_guard.deleted = true`.
//!
//! **Concurrency.** Elasticsearch's native optimistic-concurrency-control
//! fields (`_seq_no` / `_primary_term`, returned by every read and accepted
//! by every write as `if_seq_no` / `if_primary_term`) play the same role
//! MongoDB's transaction-abort label and Neo4j's conditional `SET` play: a
//! concurrent writer's commit makes ours fail (HTTP 409), mapped to
//! `DocumentError::WriteConflict` and retried from scratch, bounded, by the
//! same loop shape as the Mongo adapter. A document that doesn't exist yet
//! races on `op_type=create` instead (fails if another writer created it
//! first).
//!
//! **Batches.** Between `begin_batch` and `commit_batch`, every event is
//! decided in memory against the batch's documents, loaded up front with one
//! `_mget`. `commit_batch` then sends each touched document's final state in
//! one `_bulk` request (split past [`BULK_MAX_BYTES`]): `index` with the
//! `if_seq_no` / `if_primary_term` the batch read, or `create` for a
//! document that did not exist. `_bulk` is not atomic, but each document
//! carries its own guard, so a document that landed before another failed is
//! consistent, and the per-event retry that follows a failed commit skips it.
//! A concurrent writer's change surfaces as a 409 item, which fails the
//! commit the same way.
//!
//! Registers itself on a [`kulyrax_core::KulyraxRuntime`] under `type:
//! elasticsearch` via [`factory`] — see `kulyrax-backends` for the wiring
//! pattern, or `docs/writing-a-backend.md` for the extension pattern this
//! crate is itself a worked example of.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::Deserialize;
use serde_json::{Map, Value, json};

use kulyrax_core::adapter::document::adapter::DocumentError;
use kulyrax_core::adapter::document::exec::{self, ProjectionGuard};
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::document::{DocumentBackend, DocumentOutcome, DocumentPlan};
use kulyrax_core::adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::execution::stages::{Stage, timed};
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::AdapterBuildContext;
use kulyrax_core::runtime::config::{
    ConfigError, CustomAdapterConfig, MigrationPolicy, expand_env,
};
use kulyrax_core::upcast::UpcasterRegistry;

/// Bounded retry budget for an optimistic-concurrency conflict (HTTP 409) —
/// the same shape and limit as the Mongo adapter's transaction-retry loop.
const MAX_ATTEMPTS: u32 = 5;

/// The reserved top-level field every document in an index this adapter
/// writes carries alongside the projected content fields. Double-underscore
/// prefixed, matching the Neo4j adapter's `__ConduitGuard`/`__KulyraxGuard`
/// label convention for the same "this is ours, not the user's mapped data"
/// signal.
const GUARD_FIELD: &str = "__kulyrax_guard";

/// Documents per `_mget` request.
const MGET_CHUNK: usize = 1_000;

/// Largest `_bulk` body sent at once, well under Elasticsearch's default
/// `http.max_content_length` of 100 MB.
const BULK_MAX_BYTES: usize = 8 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Clone, Deserialize)]
pub struct ElasticsearchConfig {
    /// `http(s)://host:port`. May contain `${ENV_VAR}` references (the same
    /// pattern every other networked backend's `url` field uses).
    pub url: String,
    /// HTTP basic-auth username, if the cluster requires it.
    #[serde(default)]
    pub username: Option<String>,
    /// HTTP basic-auth password, if the cluster requires it. May contain
    /// `${ENV_VAR}` references — never put a literal secret in a mapping file.
    #[serde(default)]
    pub password: Option<String>,
    /// Request timeout in milliseconds (default 10_000).
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// Shows the URL redacted and the password as `***`.
impl std::fmt::Debug for ElasticsearchConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ElasticsearchConfig")
            .field("url", &kulyrax_core::redact::redact_url(&self.url))
            .field("username", &self.username)
            .field(
                "password",
                &self
                    .password
                    .as_deref()
                    .map(kulyrax_core::redact::redact_secret),
            )
            .field("timeout_ms", &self.timeout_ms)
            .finish()
    }
}

fn es_err(e: reqwest::Error) -> DocumentError {
    DocumentError::WriteFailed(format!("elasticsearch request failed: {e}"))
}

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

pub struct ElasticsearchAdapter {
    id: String,
    priority: u32,
    client: reqwest::blocking::Client,
    base_url: String,
    auth: Option<(String, String)>,
    builder: DocumentRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
    /// The open batch, from `begin_batch` to `commit_batch` / `rollback_batch`.
    batch: Mutex<Option<EsBatch>>,
}

/// An event's plan and its observed / target schema versions.
struct Planned {
    plan: DocumentPlan,
    source_version: u32,
    projected_version: u32,
}

/// Every document an open batch has read or written, keyed by
/// `(index, id)`, in first-touch order so the `_bulk` body is deterministic.
#[derive(Default)]
struct EsBatch {
    docs: HashMap<(String, String), BatchDoc>,
    order: Vec<(String, String)>,
}

struct BatchDoc {
    /// `(_seq_no, _primary_term)` of the stored document when the batch read
    /// it; `None` if it did not exist.
    stored: Option<(i64, i64)>,
    /// The document's current body, guard included; `None` while absent.
    body: Option<Map<String, Value>>,
    /// Written by this batch, so `commit_batch` must send it.
    dirty: bool,
}

fn poisoned() -> AdapterError {
    AdapterError::WriteFailed("Elasticsearch batch lock poisoned by an earlier panic".to_string())
}

impl ElasticsearchAdapter {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: String,
        url: &str,
        auth: Option<(String, String)>,
        timeout_ms: u64,
        priority: u32,
        builder: DocumentRuntimeBuilder,
        upcasters: Arc<UpcasterRegistry>,
        migration_policy: MigrationPolicy,
    ) -> Self {
        let client = reqwest::blocking::Client::builder()
            .timeout(std::time::Duration::from_millis(timeout_ms))
            .build()
            // A client-builder failure here is a process/TLS-backend
            // misconfiguration, not a per-event condition `handle()` could
            // meaningfully retry — fall back to an unconfigured client,
            // which then fails every request with a clear reqwest error
            // instead of panicking (Zero Panic Rule).
            .unwrap_or_else(|_| reqwest::blocking::Client::new());
        Self {
            id,
            priority,
            client,
            base_url: url.trim_end_matches('/').to_string(),
            auth,
            builder,
            upcasters,
            migration_policy,
            batch: Mutex::new(None),
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Document, error)
    }

    fn with_auth(&self, b: reqwest::blocking::RequestBuilder) -> reqwest::blocking::RequestBuilder {
        match &self.auth {
            Some((u, p)) => b.basic_auth(u, Some(p)),
            None => b,
        }
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
                });
            }
            Err(e) => return Err(self.failure(AdapterError::WriteFailed(e.to_string()))),
        };
        // `.` and `..` are dot segments: a URL parser resolves `/_doc/..` (even
        // percent-encoded) to another endpoint, so these ids cannot be addressed.
        // Refused here, on the batch and the per-event path alike.
        if matches!(projection.entity_id.as_str(), "." | "..") {
            return Err(self.failure(AdapterError::WriteFailed(format!(
                "elasticsearch cannot address a document with id {:?}: it is a URL dot segment",
                projection.entity_id
            ))));
        }
        Ok(Planned {
            source_version: projection.source_version,
            projected_version: projection.projected_version,
            plan: DocumentPlan {
                collection: projection.collection,
                entity_id: projection.entity_id,
                document: projection.document,
                on_existing: projection.on_existing,
                operation: projection.operation,
                permanent: projection.permanent,
                facet: projection.facet,
            },
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
            Err(DocumentError::WriteConflict) => self.failure(AdapterError::WriteFailed(format!(
                "elasticsearch write conflict: exceeded {MAX_ATTEMPTS} attempts against a contended entity"
            ))),
            Err(e) => self.failure(AdapterError::WriteFailed(e.to_string())),
        }
    }

    /// Load `keys` into `batch` with `_mget`, `MGET_CHUNK` documents per
    /// request. A document in an index that does not exist yet is absent.
    fn load(&self, batch: &mut EsBatch, keys: &[(String, String)]) -> Result<(), DocumentError> {
        for chunk in keys.chunks(MGET_CHUNK) {
            let docs: Vec<Value> = chunk
                .iter()
                .map(|(index, id)| json!({ "_index": index, "_id": id }))
                .collect();
            let resp = self
                .with_auth(self.client.post(format!("{}/_mget", self.base_url)))
                .json(&json!({ "docs": docs }))
                .send()
                .map_err(es_err)?;
            if !resp.status().is_success() {
                return Err(DocumentError::WriteFailed(format!(
                    "elasticsearch _mget returned {}: {}",
                    resp.status(),
                    resp.text().unwrap_or_default()
                )));
            }
            let body: Value = resp.json().map_err(es_err)?;
            let items = body
                .get("docs")
                .and_then(Value::as_array)
                .filter(|items| items.len() == chunk.len())
                .ok_or_else(|| {
                    DocumentError::WriteFailed(format!(
                        "elasticsearch _mget returned no `docs` for {} documents",
                        chunk.len()
                    ))
                })?;
            for (key, item) in chunk.iter().zip(items) {
                let doc = mget_item(item)?;
                batch.insert(key.clone(), doc);
            }
        }
        Ok(())
    }

    /// Send every document `batch` wrote, in `_bulk` requests of at most
    /// [`BULK_MAX_BYTES`].
    fn flush(&self, batch: &EsBatch) -> Result<(), DocumentError> {
        let mut body = String::new();
        let mut items = 0usize;
        for key in &batch.order {
            let Some(doc) = batch.docs.get(key).filter(|d| d.dirty) else {
                continue;
            };
            let (index, id) = key;
            let action = match doc.stored {
                Some((seq_no, primary_term)) => json!({ "index": {
                    "_index": index, "_id": id,
                    "if_seq_no": seq_no, "if_primary_term": primary_term,
                }}),
                None => json!({ "create": { "_index": index, "_id": id } }),
            };
            let source = Value::Object(doc.body.clone().unwrap_or_default());
            body.push_str(&action.to_string());
            body.push('\n');
            body.push_str(&source.to_string());
            body.push('\n');
            items += 1;
            if body.len() >= BULK_MAX_BYTES {
                self.bulk(std::mem::take(&mut body), items)?;
                items = 0;
            }
        }
        if items > 0 {
            self.bulk(body, items)?;
        }
        Ok(())
    }

    fn bulk(&self, body: String, items: usize) -> Result<(), DocumentError> {
        let resp = self
            .with_auth(self.client.post(format!("{}/_bulk", self.base_url)))
            .header(reqwest::header::CONTENT_TYPE, "application/x-ndjson")
            .body(body)
            .send()
            .map_err(es_err)?;
        if !resp.status().is_success() {
            return Err(DocumentError::WriteFailed(format!(
                "elasticsearch _bulk returned {}: {}",
                resp.status(),
                resp.text().unwrap_or_default()
            )));
        }
        let reply: Value = resp.json().map_err(es_err)?;
        bulk_errors(&reply, items)
    }
}

impl EsBatch {
    fn insert(&mut self, key: (String, String), doc: BatchDoc) {
        if !self.docs.contains_key(&key) {
            self.order.push(key.clone());
        }
        self.docs.insert(key, doc);
    }
}

/// One `_mget` item as a batch document.
fn mget_item(item: &Value) -> Result<BatchDoc, DocumentError> {
    let absent = BatchDoc {
        stored: None,
        body: None,
        dirty: false,
    };
    if let Some(error) = item.get("error") {
        return if error.get("type").and_then(Value::as_str) == Some("index_not_found_exception") {
            Ok(absent)
        } else {
            Err(DocumentError::WriteFailed(format!(
                "elasticsearch _mget item failed: {error}"
            )))
        };
    }
    if item.get("found").and_then(Value::as_bool) != Some(true) {
        return Ok(absent);
    }
    let token = |field: &str| {
        item.get(field)
            .and_then(Value::as_i64)
            .ok_or_else(|| DocumentError::WriteFailed(format!("missing {field} in _mget response")))
    };
    Ok(BatchDoc {
        stored: Some((token("_seq_no")?, token("_primary_term")?)),
        body: Some(
            item.get("_source")
                .and_then(Value::as_object)
                .cloned()
                .unwrap_or_default(),
        ),
        dirty: false,
    })
}

/// `Err` naming the first failed item of a `_bulk` reply, if any failed.
fn bulk_errors(reply: &Value, items: usize) -> Result<(), DocumentError> {
    let results = reply
        .get("items")
        .and_then(Value::as_array)
        .filter(|r| r.len() == items)
        .ok_or_else(|| {
            DocumentError::WriteFailed(format!(
                "elasticsearch _bulk reply has no result for each of {items} documents"
            ))
        })?;
    if reply.get("errors").and_then(Value::as_bool) != Some(true) {
        return Ok(());
    }
    let failed: Vec<&Value> = results
        .iter()
        .filter_map(|r| r.as_object().and_then(|o| o.values().next()))
        .filter(|r| r.get("error").is_some())
        .collect();
    let first = failed.first().map(|r| r.to_string()).unwrap_or_default();
    Err(DocumentError::WriteFailed(format!(
        "elasticsearch _bulk: {} of {items} documents failed; first: {first}",
        failed.len()
    )))
}

impl StorageAdapter for ElasticsearchAdapter {
    fn kind(&self) -> StorageKind {
        StorageKind::Document
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn priority(&self) -> u32 {
        self.priority
    }

    /// Load every document the batch's events will read, with `_mget`.
    fn begin_batch(&self, events: &[&Event]) -> Result<(), AdapterError> {
        let mut batch = self.batch.lock().map_err(|_| poisoned())?;
        if batch.is_some() {
            return Err(AdapterError::WriteFailed(
                "a batch is already open on this adapter".to_string(),
            ));
        }
        // Events that fail to plan are left to `handle`, which reports them.
        let mut keys: Vec<(String, String)> = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for event in events {
            if let Ok(p) = self.plan(event) {
                let key = (p.plan.collection, p.plan.entity_id);
                if seen.insert(key.clone()) {
                    keys.push(key);
                }
            }
        }
        let mut open = EsBatch::default();
        timed(Stage::Read, || self.load(&mut open, &keys))
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))?;
        *batch = Some(open);
        Ok(())
    }

    fn commit_batch(&self) -> Result<(), AdapterError> {
        let mut batch = self.batch.lock().map_err(|_| poisoned())?;
        let Some(open) = batch.take() else {
            return Ok(());
        };
        timed(Stage::Commit, || self.flush(&open))
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))
    }

    fn rollback_batch(&self) {
        if let Ok(mut batch) = self.batch.lock() {
            batch.take();
        }
    }

    fn handle(&self, event: &Event) -> AdapterResult {
        let planned = match self.plan(event) {
            Ok(p) => p,
            Err(result) => return result,
        };
        let plan = &planned.plan;

        let mut batch = match self.batch.lock() {
            Ok(b) => b,
            Err(_) => return self.failure(poisoned()),
        };
        if let Some(open) = batch.as_mut() {
            let outcome = exec::project(
                &mut BatchBackend {
                    adapter: self,
                    batch: open,
                },
                plan,
                event,
            );
            return self.result(&planned, outcome);
        }
        drop(batch);

        // Phase 29: the same bounded-retry-on-conflict shape as the Mongo
        // adapter, driven by Elasticsearch's own optimistic-concurrency
        // fields instead of a transaction-abort label. A fresh `EsBackend`
        // each attempt mirrors a fresh `MongoBackend`/session per attempt.
        let mut attempts = 0;
        let result = loop {
            attempts += 1;
            let mut backend = EsBackend {
                client: &self.client,
                base_url: &self.base_url,
                auth: self.auth.clone(),
                found: None,
            };
            let outcome = exec::project(&mut backend, plan, event);
            match outcome {
                Err(DocumentError::WriteConflict) if attempts < MAX_ATTEMPTS => continue,
                other => break other,
            }
        };
        self.result(&planned, result)
    }
}

// ---------------------------------------------------------------------------
// BatchBackend — an event inside an open batch
// ---------------------------------------------------------------------------

/// Reads and writes the batch's documents in memory; `commit_batch` sends
/// them. A document `begin_batch` did not load is fetched on first read.
struct BatchBackend<'a> {
    adapter: &'a ElasticsearchAdapter,
    batch: &'a mut EsBatch,
}

impl BatchBackend<'_> {
    fn doc(&mut self, index: &str, id: &str) -> Result<&mut BatchDoc, DocumentError> {
        let key = (index.to_string(), id.to_string());
        // Inside `exec::project`'s own `read` timing, so not timed again.
        if !self.batch.docs.contains_key(&key) {
            self.adapter.load(self.batch, std::slice::from_ref(&key))?;
        }
        self.batch
            .docs
            .get_mut(&key)
            .ok_or_else(|| DocumentError::WriteFailed(format!("{index}/{id} was not loaded")))
    }
}

impl DocumentBackend for BatchBackend<'_> {
    fn read_guard(
        &mut self,
        collection: &str,
        entity_id: &str,
    ) -> Result<Option<ProjectionGuard>, DocumentError> {
        match &self.doc(collection, entity_id)?.body {
            None => Ok(None),
            Some(body) => parse_guard(body).map(Some),
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
        let doc = self.doc(collection, entity_id)?;
        let body = written_body(doc.body.as_ref(), document, facet, guard)?;
        doc.body = Some(body);
        doc.dirty = true;
        Ok(())
    }

    fn delete(
        &mut self,
        collection: &str,
        entity_id: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError> {
        let body = deleted_body(guard)?;
        let doc = self.doc(collection, entity_id)?;
        doc.body = Some(body);
        doc.dirty = true;
        Ok(())
    }
}

/// The guard stored in a document body.
fn parse_guard(body: &Map<String, Value>) -> Result<ProjectionGuard, DocumentError> {
    let guard_value = body.get(GUARD_FIELD).cloned().unwrap_or(Value::Null);
    serde_json::from_value(guard_value)
        .map_err(|e| DocumentError::WriteFailed(format!("corrupt guard field: {e}")))
}

fn guard_value(guard: &ProjectionGuard) -> Result<Value, DocumentError> {
    serde_json::to_value(guard)
        .map_err(|e| DocumentError::WriteFailed(format!("failed to serialize guard: {e}")))
}

/// The body a write leaves, starting from the document's previous body
/// (`prev`), so a guard-only bump (`document: None`) or a facet merge doesn't
/// lose sibling top-level fields this write doesn't touch.
fn written_body(
    prev: Option<&Map<String, Value>>,
    document: Option<&Value>,
    facet: &str,
    guard: &ProjectionGuard,
) -> Result<Map<String, Value>, DocumentError> {
    let mut body = prev.cloned().unwrap_or_default();
    if let Some(document) = document {
        if facet.is_empty() {
            // A full replace of every content field, same as Mongo's
            // `replace_one` — but the guard lives in this same document,
            // so start a fresh map rather than merging onto stale content.
            body = document.as_object().cloned().ok_or_else(|| {
                DocumentError::BuildFailed("document mapping must resolve to a JSON object".into())
            })?;
        } else {
            let facet_obj = document.as_object().ok_or_else(|| {
                DocumentError::BuildFailed("facet mapping must resolve to a JSON object".into())
            })?;
            for (k, v) in facet_obj {
                body.insert(k.clone(), v.clone());
            }
        }
    }
    body.insert(GUARD_FIELD.to_string(), guard_value(guard)?);
    Ok(body)
}

/// The body a delete leaves: the guard alone. It must survive the entity's
/// own deletion (every other backend's tombstone does the same), so this
/// clears the content fields rather than issuing a real Elasticsearch
/// DELETE, which would take the guard down with it.
fn deleted_body(guard: &ProjectionGuard) -> Result<Map<String, Value>, DocumentError> {
    let mut body = Map::new();
    body.insert(GUARD_FIELD.to_string(), guard_value(guard)?);
    Ok(body)
}

// ---------------------------------------------------------------------------
// EsBackend — the driver
// ---------------------------------------------------------------------------

/// What a prior `read_guard` in this same attempt found, carried forward so
/// `write`/`delete` can issue the matching optimistic-concurrency write. The
/// `DocumentBackend` trait has no slot for a driver's own CAS token, so (like
/// `MongoBackend` holding `&mut ClientSession`) this struct holds it instead,
/// scoped to one `project()` attempt.
struct Found {
    seq_no: i64,
    primary_term: i64,
    /// The document's other top-level fields, besides `__kulyrax_guard` —
    /// needed so a facet merge or a guard-only bump doesn't clobber content
    /// fields this write doesn't touch (Elasticsearch's own partial-update
    /// API merges top-level keys, but this adapter still needs the previous
    /// body on hand to decide what "untouched" means when clearing content
    /// on delete).
    body: Map<String, Value>,
}

struct EsBackend<'a> {
    client: &'a reqwest::blocking::Client,
    base_url: &'a str,
    auth: Option<(String, String)>,
    /// Populated by `read_guard`, consumed by `write`/`delete`.
    found: Option<Found>,
}

impl EsBackend<'_> {
    fn doc_url(&self, index: &str, id: &str) -> String {
        format!(
            "{}/{}/_doc/{}",
            self.base_url,
            urlencoding(index),
            urlencoding(id)
        )
    }

    fn with_auth(&self, b: reqwest::blocking::RequestBuilder) -> reqwest::blocking::RequestBuilder {
        match &self.auth {
            Some((u, p)) => b.basic_auth(u, Some(p)),
            None => b,
        }
    }

    /// GET the document; `None` on 404, the parsed body + CAS token on 200.
    fn get(&self, index: &str, id: &str) -> Result<Option<Found>, DocumentError> {
        let resp = self
            .with_auth(self.client.get(self.doc_url(index, id)))
            .send()
            .map_err(es_err)?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(DocumentError::WriteFailed(format!(
                "elasticsearch GET {} returned {}",
                self.doc_url(index, id),
                resp.status()
            )));
        }
        let body: Value = resp.json().map_err(es_err)?;
        let seq_no = body
            .get("_seq_no")
            .and_then(Value::as_i64)
            .ok_or_else(|| DocumentError::WriteFailed("missing _seq_no in GET response".into()))?;
        let primary_term = body
            .get("_primary_term")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                DocumentError::WriteFailed("missing _primary_term in GET response".into())
            })?;
        let body = body
            .get("_source")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        Ok(Some(Found {
            seq_no,
            primary_term,
            body,
        }))
    }

    /// PUT the whole document body, with the concurrency-control query
    /// params matching whether a prior read in this attempt found one.
    fn put(&self, index: &str, id: &str, body: &Map<String, Value>) -> Result<(), DocumentError> {
        let mut url = self.doc_url(index, id);
        match &self.found {
            Some(f) => {
                url.push_str(&format!(
                    "?if_seq_no={}&if_primary_term={}",
                    f.seq_no, f.primary_term
                ));
            }
            None => url.push_str("?op_type=create"),
        }
        let resp = self
            .with_auth(self.client.put(url))
            .json(body)
            .send()
            .map_err(es_err)?;
        if resp.status() == reqwest::StatusCode::CONFLICT {
            return Err(DocumentError::WriteConflict);
        }
        if !resp.status().is_success() {
            return Err(DocumentError::WriteFailed(format!(
                "elasticsearch PUT {} returned {}: {}",
                self.doc_url(index, id),
                resp.status(),
                resp.text().unwrap_or_default()
            )));
        }
        Ok(())
    }
}

impl DocumentBackend for EsBackend<'_> {
    fn read_guard(
        &mut self,
        collection: &str,
        entity_id: &str,
    ) -> Result<Option<ProjectionGuard>, DocumentError> {
        let found = self.get(collection, entity_id)?;
        match found {
            None => {
                self.found = None;
                Ok(None)
            }
            Some(found) => {
                let guard = parse_guard(&found.body)?;
                self.found = Some(found);
                Ok(Some(guard))
            }
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
        // Start from whatever this same attempt's `read_guard` saw.
        let prev = self.found.as_ref().map(|f| &f.body);
        let body = written_body(prev, document, facet, guard)?;
        self.put(collection, entity_id, &body)
    }

    fn delete(
        &mut self,
        collection: &str,
        entity_id: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError> {
        self.put(collection, entity_id, &deleted_body(guard)?)
    }
}

/// Minimal path-segment escaping — index/entity ids here are producer-chosen
/// strings, not necessarily URL-safe. `reqwest::Url::parse` on the raw
/// formatted string would percent-escape too late (it needs a well-formed
/// URL to parse first), so this is applied to each segment going in instead.
fn urlencoding(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Registry factory for `type: elasticsearch`.
pub fn factory(
    entry: &CustomAdapterConfig,
    ctx: &AdapterBuildContext,
) -> Result<Box<dyn StorageAdapter>, ConfigError> {
    let id = entry.id.clone();
    let cfg: ElasticsearchConfig = entry.backend_config()?;

    let url = expand_env(&cfg.url);
    if url.trim().is_empty() {
        return Err(ConfigError::FactoryConfigInvalid(format!(
            "elasticsearch adapter {id:?}: `url` is empty"
        )));
    }
    if reqwest::Url::parse(&url).is_err() {
        return Err(ConfigError::FactoryConfigInvalid(format!(
            "elasticsearch adapter {id:?}: `url` does not parse: {}",
            kulyrax_core::redact::redact_url(&url)
        )));
    }
    let auth = match (&cfg.username, &cfg.password) {
        (Some(u), Some(p)) => Some((u.clone(), expand_env(p))),
        _ => None,
    };

    Ok(Box::new(ElasticsearchAdapter::new(
        id,
        &url,
        auth,
        cfg.timeout_ms.unwrap_or(10_000),
        entry.priority,
        DocumentRuntimeBuilder::new(ctx.mappings.document.clone()),
        Arc::clone(&ctx.upcasters),
        ctx.migration_policy,
    )))
}
