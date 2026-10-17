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
//! Registers itself on a [`kulyrax_core::KulyraxRuntime`] under `type:
//! elasticsearch` via [`factory`] — see `kulyrax-backends` for the wiring
//! pattern, or `docs/writing-a-backend.md` for the extension pattern this
//! crate is itself a worked example of.

use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value};

use kulyrax_core::adapter::document::adapter::DocumentError;
use kulyrax_core::adapter::document::exec::{self, ProjectionGuard};
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::document::{DocumentBackend, DocumentOutcome, DocumentPlan};
use kulyrax_core::adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
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

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize)]
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
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Document, error)
    }
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
            let outcome = exec::project(&mut backend, &plan, event);
            match outcome {
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
                "elasticsearch write conflict: exceeded {MAX_ATTEMPTS} attempts against a contended entity"
            ))),
            Err(e) => self.failure(AdapterError::WriteFailed(e.to_string())),
        }
    }
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

    fn guard_value(guard: &ProjectionGuard) -> Result<Value, DocumentError> {
        serde_json::to_value(guard)
            .map_err(|e| DocumentError::WriteFailed(format!("failed to serialize guard: {e}")))
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
                let guard_value = found.body.get(GUARD_FIELD).cloned().unwrap_or(Value::Null);
                let guard: ProjectionGuard = serde_json::from_value(guard_value)
                    .map_err(|e| DocumentError::WriteFailed(format!("corrupt guard field: {e}")))?;
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
        // Start from whatever this same attempt's `read_guard` saw, so a
        // guard-only bump (`document: None`) or a facet merge doesn't lose
        // sibling top-level fields this write doesn't touch.
        let mut body = self
            .found
            .as_ref()
            .map(|f| f.body.clone())
            .unwrap_or_default();

        if let Some(document) = document {
            if facet.is_empty() {
                // A full replace of every content field, same as Mongo's
                // `replace_one` — but the guard lives in this same document,
                // so start a fresh map rather than merging onto stale content.
                body = document.as_object().cloned().ok_or_else(|| {
                    DocumentError::BuildFailed(
                        "document mapping must resolve to a JSON object".into(),
                    )
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

        body.insert(GUARD_FIELD.to_string(), Self::guard_value(guard)?);
        self.put(collection, entity_id, &body)
    }

    fn delete(
        &mut self,
        collection: &str,
        entity_id: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError> {
        // The guard must survive the entity's own deletion (every other
        // backend's tombstone does the same), so this clears the content
        // fields rather than issuing a real Elasticsearch DELETE, which
        // would take the guard down with it.
        let mut body = Map::new();
        body.insert(GUARD_FIELD.to_string(), Self::guard_value(guard)?);
        self.put(collection, entity_id, &body)
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
            "elasticsearch adapter {id:?}: `url` does not parse: {url}"
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
