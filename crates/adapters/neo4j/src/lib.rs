//! Neo4j graph backend over core's `GraphBackend` seam. Registers as
//! `type: neo4j` via [`factory`].
//!
//! `neo4rs` is async-only, so the adapter wraps it in a small internal tokio
//! runtime and dispatch stays synchronous.
//!
//! **Concurrency.** Neo4j's per-node write locks alone do not stop a
//! transaction from writing stale data it read earlier, so the
//! compare-and-set on the guard node is the actual correctness mechanism. A
//! Cypher `WHERE` takes no lock, so two transactions could both pass the
//! check against the same stale `last_sequence`, and the last to commit would
//! win regardless of sequence order. `cas_write` therefore takes the write
//! lock immediately after `MERGE`, with a same-value `SET g.kind = $kind`,
//! *before* the `WHERE` reads `last_sequence`. See that function's doc
//! comment.
//!
//! Guard placement: one `__KulyraxGuard` node per `(kind, target, entity_key,
//! facet)` — `kind` is `"node"` or `"edge"` (disambiguating a node label and
//! an edge type that happen to share a name), `target` is the label or edge
//! type, `entity_key` the node id or canonical edge key. This is flatter
//! than every other backend's guard (one record per *entity*, facets nested
//! inside) because Neo4j's CAS needs `last_sequence` to be a plain queryable
//! property per lane — a nested map isn't a valid property value at all, and
//! even a JSON-string blob couldn't be compared in a Cypher `WHERE`.
//!
//! **Batches.** `begin_batch` opens one transaction that every event of the
//! batch runs in, unchanged, compare-and-set included; `commit_batch` commits
//! it once. A transaction reads its own writes, so later events see earlier
//! ones, and guard locks are held until the commit. A conflict fails the
//! event, which rolls the batch back; the per-event path that follows has
//! the usual retry.

#![deny(missing_docs)]

use std::sync::{Arc, Mutex};

mod legacy;

use neo4rs::{BoltList, BoltMap, BoltType, ConfigBuilder, Graph, Neo4jErrorKind, Query, Row, Txn};
use serde::Deserialize;
use serde_json::Value;
use tokio::runtime::Runtime;

use kulyrax_core::adapter::graph::adapter::GraphError;
use kulyrax_core::adapter::graph::exec::{
    self, EdgeRef, GraphBackend, GraphFacetGuard, GraphGuard, GraphOutcome,
};
use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::runtime::{GraphProjection, GraphRuntimeBuilder};
use kulyrax_core::adapter::{AdapterError, AdapterResult, Prepared, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::execution::stages::{Stage, timed};
use kulyrax_core::format::{FormatError, FormatProblem};
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::AdapterBuildContext;
use kulyrax_core::runtime::config::{
    ConfigError, CustomAdapterConfig, MigrationPolicy, expand_env,
};
use kulyrax_core::upcast::UpcasterRegistry;

/// The `config:` block of a `type: neo4j` adapter entry.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct Neo4jConfig {
    /// The adapter's id. In YAML it is the entry's `id:`, not a key of the
    /// `config:` block.
    #[serde(skip)]
    pub id: String,
    /// The adapter's priority. In YAML it is the entry's `priority:`.
    #[serde(skip)]
    pub priority: u32,
    /// Bolt URI (`bolt://host:port` or `neo4j://host:port`); `${ENV_VAR}`
    /// references are expanded.
    pub uri: String,
    /// The user to authenticate as.
    pub user: String,
    /// `${ENV_VAR}` references are expanded, so credentials can stay out of
    /// the committed config.
    pub password: String,
    /// Neo4j's default database when omitted.
    #[serde(default)]
    pub database: Option<String>,
}

impl Neo4jConfig {
    /// Id empty, priority 0, Neo4j's default database.
    pub fn new(
        uri: impl Into<String>,
        user: impl Into<String>,
        password: impl Into<String>,
    ) -> Self {
        Self {
            id: String::new(),
            priority: 0,
            uri: uri.into(),
            user: user.into(),
            password: password.into(),
            database: None,
        }
    }

    /// Set the adapter's id.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = id.into();
        self
    }

    /// Set the adapter's priority: lower runs first.
    pub fn with_priority(mut self, priority: u32) -> Self {
        self.priority = priority;
        self
    }

    /// Set the database to use instead of the server's default.
    pub fn with_database(mut self, database: impl Into<String>) -> Self {
        self.database = Some(database.into());
        self
    }
}

/// Shows the URI redacted and the password as `***`.
impl std::fmt::Debug for Neo4jConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Neo4jConfig")
            .field("id", &self.id)
            .field("priority", &self.priority)
            .field("uri", &kulyrax_core::redact::redact_url(&self.uri))
            .field("user", &self.user)
            .field(
                "password",
                &kulyrax_core::redact::redact_secret(&self.password),
            )
            .field("database", &self.database)
            .finish()
    }
}

/// Registry factory for `type: neo4j`.
pub fn factory(
    entry: &CustomAdapterConfig,
    ctx: &AdapterBuildContext,
) -> Result<Box<dyn StorageAdapter>, ConfigError> {
    let cfg: Neo4jConfig = entry.backend_config()?;
    let cfg = cfg.with_id(entry.id.clone()).with_priority(entry.priority);
    Ok(Box::new(Neo4jAdapter::from_config(&cfg, ctx)?))
}

impl Neo4jAdapter {
    /// Build the adapter, with the run's mappings, upcasters and migration
    /// policy from `ctx`. Fails on the checks below; an unreachable server
    /// surfaces on the first write.
    pub fn from_config(cfg: &Neo4jConfig, ctx: &AdapterBuildContext) -> Result<Self, ConfigError> {
        let id = cfg.id.clone();
        let uri = expand_env(&cfg.uri);
        let password = expand_env(&cfg.password);
        if uri.trim().is_empty() {
            return Err(ConfigError::FactoryConfigInvalid(format!(
                "neo4j adapter {id:?}: `uri` is empty"
            )));
        }
        if let Err(reason) = check_bolt_uri(&uri) {
            return Err(ConfigError::FactoryConfigInvalid(format!(
                "neo4j adapter {id:?}: `uri` {reason}: {}",
                kulyrax_core::redact::redact_url(&uri)
            )));
        }
        Ok(Neo4jAdapter::new(
            id,
            &uri,
            &cfg.user,
            &password,
            cfg.database.clone(),
            cfg.priority,
            GraphRuntimeBuilder::new(ctx.mappings.graph.clone()),
            Arc::clone(&ctx.upcasters),
            ctx.migration_policy,
        ))
    }
}

/// Mirrors the parse `neo4rs` performs when it connects (`NeoUrl::parse` and
/// its scheme match in neo4rs 0.8 `connection.rs`): a URI without a host is
/// retried with `bolt://` prepended, and only the Bolt and Neo4j schemes are
/// accepted. `neo4rs`'s own `ConfigBuilder::build` checks only that a URI is
/// present, so without this a typo would surface only on the first event.
fn check_bolt_uri(uri: &str) -> Result<(), &'static str> {
    let parsed = match url::Url::parse(uri) {
        Ok(u) if u.has_host() => u,
        Ok(_) | Err(url::ParseError::RelativeUrlWithoutBase) => {
            url::Url::parse(&format!("bolt://{uri}")).map_err(|_| "does not parse")?
        }
        Err(_) => return Err("does not parse"),
    };
    match parsed.scheme() {
        "bolt" | "bolt+s" | "bolt+ssc" | "neo4j" | "neo4j+s" | "neo4j+ssc" => Ok(()),
        _ => Err("has an unsupported scheme"),
    }
}

const MAX_ATTEMPTS: u32 = 5;
const GUARD_CONSTRAINT: &str = "CREATE CONSTRAINT kulyrax_guard_key IF NOT EXISTS \
    FOR (g:__KulyraxGuard) REQUIRE (g.kind, g.target, g.entity_key, g.facet) IS UNIQUE";

fn json_to_bolt(v: &Value) -> BoltType {
    match v {
        Value::Null => BoltType::Null(neo4rs::BoltNull),
        Value::Bool(b) => (*b).into(),
        Value::Number(n) => match n.as_i64() {
            Some(i) => i.into(),
            None => n.as_f64().unwrap_or(0.0).into(),
        },
        Value::String(s) => s.as_str().into(),
        Value::Array(a) => {
            let list: BoltList = a.iter().map(json_to_bolt).collect::<Vec<_>>().into();
            BoltType::List(list)
        }
        Value::Object(o) => {
            let map: BoltMap = o
                .iter()
                .map(|(k, v)| (k.as_str().into(), json_to_bolt(v)))
                .collect();
            BoltType::Map(map)
        }
    }
}

fn is_retryable(e: &neo4rs::Error) -> bool {
    matches!(e, neo4rs::Error::Neo4j(ne) if matches!(ne.kind(), Neo4jErrorKind::Transient))
}

/// Any Neo4j error carrying the `Transient` classification (a deadlock, a
/// lock-acquisition conflict) becomes `WriteConflict`; everything else is a
/// hard failure.
fn neo_err(e: neo4rs::Error) -> GraphError {
    if is_retryable(&e) {
        GraphError::WriteConflict
    } else {
        GraphError::WriteFailed(e.to_string())
    }
}

/// The format marker (step 5): one `:__KulyraxFormat` node per component,
/// unique on `component`, so `MERGE` creates it at most once.
const FORMAT_CONSTRAINT: &str = "CREATE CONSTRAINT kulyrax_format_component IF NOT EXISTS \
    FOR (f:__KulyraxFormat) REQUIRE f.component IS UNIQUE";
const FORMAT_MERGE: &str = "MERGE (f:__KulyraxFormat {component: $component}) \
    ON CREATE SET f.version = $version RETURN f.version AS version";
/// The marker node versioning the guards.
const GUARDS_COMPONENT: &str = "guards";
/// The guards' format version.
const GUARDS_FORMAT: i64 = 2;
/// What a Neo4j user needs for the marker (named in its refusal).
const MARKER_GRANT: &str = "the rights to create the constraint kulyrax_format_component and \
     to create and read :__KulyraxFormat nodes (Neo4j Enterprise: CREATE CONSTRAINT, and MERGE \
     and MATCH on that label)";
/// Attempts at the `MERGE` when concurrent first starts collide.
const MARKER_ATTEMPTS: u32 = 5;

/// Step 5's format marker (decision 2026-11-28-format-versions): written only
/// if absent, read back, and checked, before the guard constraint and any
/// 0.12 migration. A database with guards and no marker was written by
/// 0.12.x: format 1.
async fn ensure_format_marker(graph: &Graph, store: &str) -> Result<(), GraphError> {
    let refused = |e: neo4rs::Error| -> GraphError {
        let forbidden = matches!(&e, neo4rs::Error::Neo4j(ne)
            if ne.code() == "Neo.ClientError.Security.Forbidden");
        if forbidden {
            FormatError::new(
                store,
                FormatProblem::MarkerNotCreated {
                    grant: MARKER_GRANT.to_string(),
                    cause: e.to_string(),
                },
            )
            .into()
        } else {
            neo_err(e)
        }
    };
    graph
        .run(Query::new(FORMAT_CONSTRAINT.to_string()))
        .await
        .map_err(refused)?;
    // Format 1 keyed edge guards without their endpoints' labels, which were
    // never stored (decision 2026-11-28-edge-labels). A database with no
    // edge guards is format 2 as it is; one with any is format 1 and cannot
    // move up. 0.12's guards still carry their old label here.
    let edge_guards = {
        let q = Query::new(format!(
            "MATCH (g) WHERE (g:__KulyraxGuard OR g:`{}`) AND g.kind = 'edge' \
             RETURN count(g) > 0 AS any",
            kulyrax_core::adapter::legacy::LEGACY_GUARD_LABEL
        ));
        let row = graph
            .execute(q)
            .await
            .map_err(refused)?
            .next()
            .await
            .map_err(refused)?;
        row.and_then(|r| r.get::<bool>("any").ok()).unwrap_or(false)
    };
    let initial = if edge_guards { 1 } else { GUARDS_FORMAT };
    // Two first starts can both try to create the node; the constraint lets
    // one in and fails the other, which then finds it.
    let mut attempt = 0;
    let row = loop {
        attempt += 1;
        let query = Query::new(FORMAT_MERGE.to_string())
            .param("component", GUARDS_COMPONENT)
            .param("version", initial);
        let result = async { graph.execute(query).await?.next().await }.await;
        match result {
            Ok(row) => break row,
            Err(neo4rs::Error::Neo4j(ne))
                if attempt < MARKER_ATTEMPTS
                    && (matches!(ne.kind(), Neo4jErrorKind::Transient)
                        || ne.code() == "Neo.ClientError.Schema.ConstraintValidationFailed") => {}
            Err(e) => return Err(refused(e)),
        }
    };
    let row = row.ok_or_else(|| {
        GraphError::WriteFailed(format!("{store}: the format marker was not written"))
    })?;
    let version = row
        .get::<i64>("version")
        .ok()
        .and_then(|v| u32::try_from(v).ok())
        .ok_or_else(|| {
            FormatError::new(
                store,
                FormatProblem::Unrecognized(format!(
                    ":__KulyraxFormat {GUARDS_COMPONENT} has a version that is not a number"
                )),
            )
        })?;
    FormatError::check(store, version, GUARDS_FORMAT as u32)?;
    if version < GUARDS_FORMAT as u32 {
        if edge_guards {
            return Err(FormatError::new(
                store,
                FormatProblem::Unrecognized(
                    "it holds edges keyed by an earlier version, without their endpoints' \
                     labels, which were never stored. Clear the database and replay the full \
                     history into it"
                        .to_string(),
                ),
            )
            .into());
        }
        graph
            .run(
                Query::new(
                    "MATCH (f:__KulyraxFormat {component: $component}) WHERE f.version < $version \
                     SET f.version = $version"
                        .to_string(),
                )
                .param("component", GUARDS_COMPONENT)
                .param("version", GUARDS_FORMAT),
            )
            .await
            .map_err(refused)?;
    }
    Ok(())
}

fn de_err(e: neo4rs::DeError) -> GraphError {
    GraphError::WriteFailed(format!("corrupt guard row: {e}"))
}

/// The four flat fields one `__KulyraxGuard` lane carries.
fn lane_fields(guard: &GraphGuard, facet: &str) -> (u64, String, bool, bool) {
    if facet.is_empty() {
        (
            guard.last_sequence,
            guard.last_event_id.clone(),
            guard.deleted,
            guard.permanent,
        )
    } else {
        match guard.facets.get(facet) {
            Some(fg) => (
                fg.last_sequence,
                fg.last_event_id.clone(),
                fg.deleted,
                fg.permanent,
            ),
            None => (0, String::new(), false, false),
        }
    }
}

/// One client transaction per event; the CAS check is an explicit
/// `WHERE`-conditioned `SET`, not implied by transaction isolation (see the
/// module docs).
pub struct Neo4jAdapter {
    id: String,
    priority: u32,
    /// The server and database for messages, the URI redacted.
    store: String,
    /// `Err` when connecting failed; every `handle()` then fails cleanly.
    /// `neo4rs` has no lazy connect, so unlike the other networked backends
    /// this connects when the adapter is built.
    conn: Result<(Runtime, Graph), String>,
    builder: GraphRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
    /// Set by `prepare` once the guard uniqueness constraint exists.
    prepared: Prepared,
    /// The open batch's transaction, from `begin_batch` to `commit_batch` /
    /// `rollback_batch`.
    batch: Mutex<Option<Txn>>,
}

fn poisoned() -> AdapterError {
    AdapterError::WriteFailed("Neo4j batch lock poisoned by an earlier panic".to_string())
}

impl Neo4jAdapter {
    #[allow(clippy::too_many_arguments)]
    #[doc(hidden)]
    pub fn new(
        id: String,
        uri: &str,
        user: &str,
        password: &str,
        database: Option<String>,
        priority: u32,
        builder: GraphRuntimeBuilder,
        upcasters: Arc<UpcasterRegistry>,
        migration_policy: MigrationPolicy,
    ) -> Self {
        let store = format!(
            "Neo4j {} database {}",
            kulyrax_core::redact::redact_url(uri),
            database.as_deref().unwrap_or("(default)")
        );
        let conn = (|| -> Result<(Runtime, Graph), String> {
            let rt = Runtime::new()
                .map_err(|e| format!("failed to start an internal tokio runtime: {e}"))?;
            let mut cfg = ConfigBuilder::new().uri(uri).user(user).password(password);
            if let Some(db) = database {
                cfg = cfg.db(db);
            }
            let config = cfg
                .build()
                .map_err(|e| format!("invalid neo4j config: {e}"))?;
            let graph = rt
                .block_on(Graph::connect(config))
                .map_err(|e| format!("failed to connect to neo4j: {e}"))?;
            Ok((rt, graph))
        })();
        Self {
            id,
            priority,
            store,
            conn,
            builder,
            upcasters,
            migration_policy,
            prepared: Prepared::default(),
            batch: Mutex::new(None),
        }
    }

    fn connection(&self) -> Result<(&Runtime, &Graph), AdapterError> {
        self.conn
            .as_ref()
            .map(|(rt, graph)| (rt, graph))
            .map_err(|e| AdapterError::WriteFailed(e.clone()))
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Graph, error)
    }

    /// Check the format marker, create the guard uniqueness constraint
    /// (idempotent: `IF NOT EXISTS`), then move any 0.12 guards onto it.
    fn ensure_constraint(&self, rt: &Runtime, graph: &Graph) -> Result<(), GraphError> {
        rt.block_on(async {
            ensure_format_marker(graph, &self.store).await?;
            // A kill here: the marker is written, the guard constraint and
            // the 0.12 relabelling have not run (the kill tests start again
            // here).
            kulyrax_core::crash::crash_point("format-marker-written");
            graph
                .run(Query::new(GUARD_CONSTRAINT.to_string()))
                .await
                .map_err(neo_err)?;
            legacy::migrate_guards(graph).await
        })
    }
}

impl StorageAdapter for Neo4jAdapter {
    fn kind(&self) -> StorageKind {
        StorageKind::Graph
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn priority(&self) -> u32 {
        self.priority
    }

    /// Labels and edge types are written into Cypher inside backticks: refuse
    /// a name that would end the quoting (a backtick) or is empty. Then
    /// create the guard constraint.
    fn prepare(&self) -> Result<(), AdapterError> {
        let mut bad: Vec<String> = Vec::new();
        for record in self
            .builder
            .mappings()
            .values()
            .flat_map(GraphMapping::records)
        {
            let names = match record {
                GraphMapping::Node(n) => vec![&n.label],
                // The endpoint labels are written into the edge's `MERGE`s.
                GraphMapping::Edge(e) => vec![&e.edge_type, &e.from_label, &e.to_label],
                // A subgraph's records are checked one by one; a record kind
                // this version does not know names no label or edge type.
                _ => continue,
            };
            for name in names {
                if name.is_empty() || name.contains('`') {
                    bad.push(format!("{name:?}"));
                }
            }
        }
        if !bad.is_empty() {
            bad.sort();
            bad.dedup();
            return Err(AdapterError::WriteFailed(format!(
                "neo4j adapter {:?}: labels and edge types must be non-empty and contain no \
                 backtick: {}",
                self.id,
                bad.join(", ")
            )));
        }
        let reserved: Vec<_> = self
            .builder
            .mappings()
            .iter()
            .flat_map(|(event, m)| {
                kulyrax_core::adapter::reserved::in_graph_mapping(m)
                    .into_iter()
                    .map(move |c| (event.clone(), c))
            })
            .collect();
        if let Some(refusal) = kulyrax_core::adapter::reserved::refusal(
            &format!("neo4j adapter {:?}", self.id),
            reserved,
        ) {
            return Err(AdapterError::WriteFailed(refusal));
        }
        let (rt, graph) = self
            .conn
            .as_ref()
            .map_err(|e| AdapterError::WriteFailed(e.clone()))?;
        self.prepared
            .prepare(|| timed(Stage::Schema, || self.ensure_constraint(rt, graph)))
            .map_err(AdapterError::from)
    }

    fn begin_batch(&self, _events: &[&Event]) -> Result<(), AdapterError> {
        self.prepared.require().map_err(AdapterError::WriteFailed)?;
        let mut batch = self.batch.lock().map_err(|_| poisoned())?;
        if batch.is_some() {
            return Err(AdapterError::WriteFailed(
                "a batch is already open on this adapter".to_string(),
            ));
        }
        let (rt, graph) = self.connection()?;
        let txn = rt
            .block_on(graph.start_txn())
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))?;
        *batch = Some(txn);
        Ok(())
    }

    fn commit_batch(&self) -> Result<(), AdapterError> {
        let mut batch = self.batch.lock().map_err(|_| poisoned())?;
        let Some(txn) = batch.take() else {
            return Ok(());
        };
        let (rt, _) = self.connection()?;
        timed(Stage::Commit, || rt.block_on(txn.commit()))
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))
    }

    fn rollback_batch(&self) {
        let Ok(mut batch) = self.batch.lock() else {
            return;
        };
        if let (Some(txn), Ok((rt, _))) = (batch.take(), self.connection()) {
            let _ = rt.block_on(txn.rollback());
        }
    }

    fn handle(&self, event: &Event) -> AdapterResult {
        if let Err(e) = self.prepared.require() {
            return self.failure(AdapterError::WriteFailed(e));
        }
        let projections = match self.builder.build_all(event, &self.upcasters) {
            Ok(p) => p,
            Err(GraphError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return match self.migration_policy {
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::Graph,
                        SkipReason::UnsupportedVersion,
                        from_version,
                        to_version,
                    ),
                    // `Strict`, and any policy this adapter does not know
                    // (`MigrationPolicy` is `#[non_exhaustive]`): refuse.
                    _ => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::Graph,
                        reason,
                        from_version,
                        to_version,
                    ),
                };
            }
            Err(e) => return self.failure(AdapterError::WriteFailed(e.to_string())),
        };

        let (rt, graph) = match &self.conn {
            Ok(pair) => pair,
            Err(e) => return self.failure(AdapterError::WriteFailed(e.clone())),
        };

        let (source_version, projected_version) = match projections.first() {
            Some(p) => (p.source_version, p.projected_version),
            None => (event.version(), event.version()),
        };

        let mut batch = match self.batch.lock() {
            Ok(b) => b,
            Err(_) => return self.failure(poisoned()),
        };
        let result = match batch.as_mut() {
            // Inside a batch: its transaction, no retry (see the module docs).
            Some(txn) => exec::project_all(&mut Neo4jBackend { rt, txn }, &projections, event),
            None => self.project_alone(rt, graph, &projections, event),
        };
        drop(batch);

        match result {
            Ok(GraphOutcome::Created) => AdapterResult::created_versioned(
                self.id.clone(),
                StorageKind::Graph,
                source_version,
                projected_version,
            ),
            Ok(GraphOutcome::Updated) => AdapterResult::updated_versioned(
                self.id.clone(),
                StorageKind::Graph,
                source_version,
                projected_version,
            ),
            Ok(GraphOutcome::Deleted) => AdapterResult::deleted_versioned(
                self.id.clone(),
                StorageKind::Graph,
                source_version,
                projected_version,
            ),
            Ok(GraphOutcome::Skipped(reason)) => AdapterResult::skipped_versioned(
                self.id.clone(),
                StorageKind::Graph,
                reason,
                source_version,
                projected_version,
            ),
            // `GraphOutcome` is `#[non_exhaustive]`: an outcome this adapter
            // does not know is a failure, never a silent success.
            Ok(other) => self.failure(AdapterError::WriteFailed(format!(
                "neo4j: unknown graph outcome {other:?}"
            ))),
            Err(GraphError::WriteConflict) => self.failure(AdapterError::WriteFailed(format!(
                "neo4j write conflict: exceeded {MAX_ATTEMPTS} attempts against a contended entity"
            ))),
            Err(e) => self.failure(AdapterError::WriteFailed(e.to_string())),
        }
    }
}

impl Neo4jAdapter {
    /// One event (every record of it) in a transaction of its own, retried
    /// on a conflict.
    fn project_alone(
        &self,
        rt: &Runtime,
        graph: &Graph,
        projections: &[GraphProjection],
        event: &Event,
    ) -> Result<GraphOutcome, GraphError> {
        let mut attempts = 0;
        loop {
            attempts += 1;
            let attempt: Result<GraphOutcome, GraphError> = (|| {
                let mut txn = rt.block_on(graph.start_txn()).map_err(neo_err)?;
                let outcome =
                    exec::project_all(&mut Neo4jBackend { rt, txn: &mut txn }, projections, event);
                match outcome {
                    Ok(o) => {
                        timed(Stage::Commit, || rt.block_on(txn.commit())).map_err(neo_err)?;
                        Ok(o)
                    }
                    Err(e) => {
                        let _ = rt.block_on(txn.rollback());
                        Err(e)
                    }
                }
            })();

            match attempt {
                Err(GraphError::WriteConflict) if attempts < MAX_ATTEMPTS => continue,
                other => return other,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Neo4jBackend — the driver
// ---------------------------------------------------------------------------

struct Neo4jBackend<'a> {
    rt: &'a Runtime,
    txn: &'a mut Txn,
}

impl Neo4jBackend<'_> {
    fn query_rows(&mut self, q: Query) -> Result<Vec<Row>, GraphError> {
        let txn = &mut *self.txn;
        self.rt.block_on(async {
            let mut stream = txn.execute(q).await.map_err(neo_err)?;
            let mut rows = Vec::new();
            while let Some(row) = stream.next(&mut *txn).await.map_err(neo_err)? {
                rows.push(row);
            }
            Ok(rows)
        })
    }

    fn run(&mut self, q: Query) -> Result<(), GraphError> {
        let txn = &mut *self.txn;
        self.rt
            .block_on(async { txn.run(q).await.map_err(neo_err) })
    }

    fn read_guard(
        &mut self,
        kind: &str,
        target: &str,
        entity_key: &str,
    ) -> Result<Option<GraphGuard>, GraphError> {
        let q = Query::new(
            "MATCH (g:__KulyraxGuard {kind: $kind, target: $target, entity_key: $entity_key}) \
             RETURN g.facet AS facet, g.last_sequence AS last_sequence, \
                    g.last_event_id AS last_event_id, g.deleted AS deleted, g.permanent AS permanent"
                .to_string(),
        )
        .param("kind", kind)
        .param("target", target)
        .param("entity_key", entity_key);

        let rows = self.query_rows(q)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut guard = GraphGuard::default();
        for row in rows {
            let facet: String = row.get("facet").map_err(de_err)?;
            let last_sequence: i64 = row.get("last_sequence").unwrap_or(0);
            let last_event_id: String = row.get("last_event_id").unwrap_or_default();
            let deleted: bool = row.get("deleted").unwrap_or(false);
            let permanent: bool = row.get("permanent").unwrap_or(false);
            if facet.is_empty() {
                guard.last_sequence = last_sequence as u64;
                guard.last_event_id = last_event_id;
                guard.deleted = deleted;
                guard.permanent = permanent;
            } else {
                guard.facets.insert(
                    facet,
                    GraphFacetGuard::new(last_sequence as u64, last_event_id, deleted, permanent),
                );
            }
        }
        Ok(Some(guard))
    }

    /// CAS-checked guard upsert plus (optionally) the node/edge write, all
    /// in one Cypher statement — `write_extra` is the part of the query that
    /// merges/matches the target record, empty when `properties` is `None`
    /// (the guard-only sequence bump).
    ///
    /// **The forced-lock `SET g.kind = $kind` immediately after `MERGE` is
    /// load-bearing, not a no-op.** Cypher does not take a write lock on a
    /// matched node until it is actually written to; a `WHERE` clause that
    /// merely *reads* `g.last_sequence` (the naive version of this query)
    /// evaluates against whatever the node holds at that moment with **no
    /// lock held**. Two concurrent transactions can both read the same old
    /// `last_sequence`, both pass the `WHERE`, and then whichever one's
    /// `SET` physically commits last in real time wins — independent of
    /// which one actually had the higher, correct sequence. That is a
    /// genuine lost-update race, confirmed empirically under real
    /// concurrent load (20 writers to one entity reproducibly converged to
    /// a stale value in roughly 70% of runs before this fix). Reassigning
    /// `g.kind` to the value it was just matched on is a write — Neo4j
    /// takes the exclusive lock on `g` right there, so every other
    /// transaction touching the same guard blocks until this one commits
    /// or rolls back. Only *after* that lock is held does the `WHERE`
    /// clause read `g.last_sequence`, which is then guaranteed to be the
    /// latest committed value, not a pre-lock snapshot — this is the
    /// standard "force a write to force the lock" idiom Cypher requires in
    /// place of a `SELECT … FOR UPDATE`, which it has no direct equivalent
    /// for.
    #[allow(clippy::too_many_arguments)]
    fn cas_write(
        &mut self,
        kind: &str,
        target: &str,
        entity_key: &str,
        facet: &str,
        seq: u64,
        event_id: &str,
        deleted: bool,
        permanent: bool,
        write_extra: &str,
        extra_params: Vec<(&str, BoltType)>,
    ) -> Result<(), GraphError> {
        let cypher = format!(
            "MERGE (g:__KulyraxGuard {{kind: $kind, target: $target, entity_key: $entity_key, facet: $facet}}) \
             SET g.kind = $kind \
             WITH g WHERE g.last_sequence IS NULL OR g.last_sequence < $seq \
             SET g.last_sequence = $seq, g.last_event_id = $event_id, g.deleted = $deleted, g.permanent = $permanent \
             WITH g \
             {write_extra} \
             RETURN g.last_sequence AS confirmed"
        );
        let mut q = Query::new(cypher)
            .param("kind", kind)
            .param("target", target)
            .param("entity_key", entity_key)
            .param("facet", facet)
            .param("seq", seq as i64)
            .param("event_id", event_id)
            .param("deleted", deleted)
            .param("permanent", permanent);
        for (k, v) in extra_params {
            q = q.param(k, v);
        }
        let rows = self.query_rows(q)?;
        if rows.is_empty() {
            return Err(GraphError::WriteConflict);
        }
        Ok(())
    }
}

impl GraphBackend for Neo4jBackend<'_> {
    fn read_node_guard(
        &mut self,
        label: &str,
        node_id: &str,
    ) -> Result<Option<GraphGuard>, GraphError> {
        self.read_guard("node", label, node_id)
    }

    fn read_edge_guard(
        &mut self,
        edge_type: &str,
        key: &str,
    ) -> Result<Option<GraphGuard>, GraphError> {
        self.read_guard("edge", edge_type, key)
    }

    fn node_is_tombstoned(&mut self, label: &str, node_id: &str) -> Result<bool, GraphError> {
        let q = Query::new(
            "MATCH (g:__KulyraxGuard {kind: 'node', target: $label, entity_key: $id, facet: '', \
             deleted: true}) RETURN count(g) > 0 AS tombstoned"
                .to_string(),
        )
        .param("label", label)
        .param("id", node_id);
        let rows = self.query_rows(q)?;
        match rows.first() {
            Some(row) => row.get::<bool>("tombstoned").map_err(de_err),
            None => Ok(false),
        }
    }

    fn write_node(
        &mut self,
        label: &str,
        node_id: &str,
        properties: Option<&Value>,
        facet: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        let (seq, eid, deleted, permanent) = lane_fields(guard, facet);
        let mut extra_params = vec![("label_id", BoltType::from(node_id))];
        let write_extra = match properties {
            None => String::new(),
            Some(p) => {
                extra_params.push(("properties", json_to_bolt(p)));
                if facet.is_empty() {
                    format!(
                        "MERGE (n:`{label}` {{id: $label_id}}) SET n = $properties, n.id = $label_id"
                    )
                } else {
                    format!("MATCH (n:`{label}` {{id: $label_id}}) SET n += $properties")
                }
            }
        };
        self.cas_write(
            "node",
            label,
            node_id,
            facet,
            seq,
            &eid,
            deleted,
            permanent,
            &write_extra,
            extra_params,
        )
    }

    /// The endpoints are `MERGE`d with their labels: an endpoint not projected
    /// yet becomes a labelled placeholder, which the node's own
    /// `MERGE (n:<Label> {id})` then fills in, and another label's node with
    /// the same id is never touched (decision 2026-11-28-edge-labels).
    fn write_edge(
        &mut self,
        edge: &EdgeRef<'_>,
        properties: Option<&Value>,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        let (seq, eid, deleted, permanent) = lane_fields(guard, "");
        let (edge_type, from_label, to_label) = (edge.edge_type, edge.from_label, edge.to_label);
        let mut extra_params = vec![
            ("edge_from", BoltType::from(edge.from)),
            ("edge_to", BoltType::from(edge.to)),
            ("edge_key", BoltType::from(edge.key)),
        ];
        let write_extra = match properties {
            None => String::new(),
            Some(p) => {
                extra_params.push(("properties", json_to_bolt(p)));
                format!(
                    "MERGE (a:`{from_label}` {{id: $edge_from}}) \
                     MERGE (b:`{to_label}` {{id: $edge_to}}) \
                     MERGE (a)-[r:`{edge_type}` {{key: $edge_key}}]->(b) \
                     SET r = $properties, r.key = $edge_key"
                )
            }
        };
        self.cas_write(
            "edge",
            edge_type,
            edge.key,
            "",
            seq,
            &eid,
            deleted,
            permanent,
            &write_extra,
            extra_params,
        )
    }

    fn delete_node_cascade(
        &mut self,
        label: &str,
        node_id: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        let (seq, eid, deleted, permanent) = lane_fields(guard, "");
        // Statement A: CAS-checked tombstone of the default facet's guard —
        // this is the conflict-detection point for the whole cascade.
        self.cas_write(
            "node",
            label,
            node_id,
            "",
            seq,
            &eid,
            deleted,
            permanent,
            "",
            vec![],
        )?;

        // Statement B: tombstone every named-facet lane (no per-lane CAS —
        // a cascade forcibly tombstones every lane, same as the file adapter).
        if !guard.facets.is_empty() {
            let facets: BoltList = guard
                .facets
                .iter()
                .map(|(name, fg)| {
                    let mut m = BoltMap::new();
                    m.put("name".into(), name.as_str().into());
                    m.put("seq".into(), (fg.last_sequence as i64).into());
                    m.put("eid".into(), fg.last_event_id.as_str().into());
                    m.put("permanent".into(), fg.permanent.into());
                    BoltType::Map(m)
                })
                .collect::<Vec<_>>()
                .into();
            let q = Query::new(
                "UNWIND $facets AS f \
                 MERGE (g:__KulyraxGuard {kind: 'node', target: $label, entity_key: $id, facet: f.name}) \
                 SET g.last_sequence = f.seq, g.last_event_id = f.eid, g.deleted = true, g.permanent = f.permanent"
                    .to_string(),
            )
            .param("label", label)
            .param("id", node_id)
            .param("facets", BoltType::List(facets));
            self.run(q)?;
        }

        // Statement C: find every edge incident to this node, tombstone each
        // one's guard — the graph structure *is* the incident index (no
        // separate bookkeeping needed, unlike the file adapter).
        let incident = self.query_rows(
            Query::new(
                "MATCH (n:`".to_string()
                    + label
                    + "` {id: $id})-[r]-() WITH DISTINCT type(r) AS rtype, r.key AS rkey RETURN rtype, rkey",
            )
            .param("id", node_id),
        )?;
        if !incident.is_empty() {
            let edges: BoltList = incident
                .iter()
                .filter_map(|row| {
                    let etype: String = row.get("rtype").ok()?;
                    let ekey: String = row.get("rkey").ok()?;
                    let mut m = BoltMap::new();
                    m.put("etype".into(), etype.as_str().into());
                    m.put("ekey".into(), ekey.as_str().into());
                    Some(BoltType::Map(m))
                })
                .collect::<Vec<_>>()
                .into();
            let q = Query::new(
                "UNWIND $edges AS e \
                 MERGE (g:__KulyraxGuard {kind: 'edge', target: e.etype, entity_key: e.ekey, facet: ''}) \
                 SET g.last_sequence = $seq, g.last_event_id = $eid, g.deleted = true, g.permanent = $permanent"
                    .to_string(),
            )
            .param("edges", BoltType::List(edges))
            .param("seq", seq as i64)
            .param("eid", eid.as_str())
            .param("permanent", permanent);
            self.run(q)?;
        }

        // Statement D: native DETACH DELETE — removes the node and every
        // incident relationship in one primitive.
        let q = Query::new(format!("MATCH (n:`{label}` {{id: $id}}) DETACH DELETE n"))
            .param("id", node_id);
        self.run(q)
    }

    fn delete_edge(&mut self, edge: &EdgeRef<'_>, guard: &GraphGuard) -> Result<(), GraphError> {
        let (edge_type, key) = (edge.edge_type, edge.key);
        // The key holds both endpoints' labels and ids: it names one edge.
        let write_extra = format!("MATCH ()-[r:`{edge_type}` {{key: $edge_key}}]-() DELETE r");
        self.cas_write(
            "edge",
            edge_type,
            key,
            "",
            guard.last_sequence,
            &guard.last_event_id,
            guard.deleted,
            guard.permanent,
            &write_extra,
            vec![("edge_key", BoltType::from(key))],
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Only rejection paths: they return before `Neo4jAdapter::new`, which
    // would otherwise try to connect.
    fn error_of(uri: &str) -> String {
        let raw = serde_json::to_value(serde_json::json!({
            "type": "neo4j",
            "id": "n1",
            "priority": 10,
            "config": { "uri": uri, "user": "neo4j", "password": "pw" },
        }));
        let Ok(raw) = raw else {
            return "test yaml did not serialize".into();
        };
        let entry = CustomAdapterConfig::new("neo4j", "n1", 10, raw);
        let ctx = AdapterBuildContext::new(
            Default::default(),
            Arc::new(UpcasterRegistry::new()),
            MigrationPolicy::default(),
        );
        factory(&entry, &ctx)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default()
    }

    #[test]
    fn factory_rejects_an_empty_uri() {
        assert!(error_of("").contains("`uri` is empty"));
    }

    #[test]
    fn factory_rejects_an_unparseable_uri() {
        let err = error_of("not a uri");
        assert!(err.contains("`uri` does not parse"), "{err}");
    }

    #[test]
    fn factory_rejects_an_unsupported_scheme() {
        let err = error_of("http://localhost:7687");
        assert!(err.contains("unsupported scheme"), "{err}");
    }

    #[test]
    fn uri_check_accepts_what_neo4rs_accepts() {
        for uri in [
            "bolt://localhost:7687",
            "neo4j://db.example.com",
            "neo4j+s://db.example.com:7687",
            "bolt+ssc://10.0.0.5:7687",
            "localhost:7687",
            "127.0.0.1:7687",
        ] {
            assert_eq!(check_bolt_uri(uri), Ok(()), "{uri}");
        }
    }
}
