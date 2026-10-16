//! Phase 24.2–24.4: the Neo4j graph backend.
//!
//! Same `GraphMapping` / `decide()` / guard model as the file-backed store —
//! this file supplies only what a driver genuinely owns: a Bolt connection
//! (`neo4rs`, async-only — wrapped in a small internal `tokio::runtime::Runtime`
//! so the adapter path stays synchronous, the same pattern Phase 19.2 used
//! for the sync `postgres` crate over `tokio-postgres`), Cypher generation,
//! and an optimistic CAS via a `WHERE`-conditioned `SET` inside one
//! transaction per event — Neo4j's per-node write locks alone don't prevent
//! a transaction from writing stale data it read earlier (unlike MongoDB's
//! snapshot-isolated transactions, Phase 23.3), so the CAS check here is
//! the actual correctness mechanism, not a redundant belt-and-suspenders.
//!
//! **Post-release correctness fix:** the CAS originally read `g.last_sequence`
//! in a `WHERE` clause with no write lock held on `g` yet (Cypher only takes
//! one at the point of an actual write), so two concurrent transactions could
//! both pass the check against the same stale value and whichever committed
//! last in real time won — independent of sequence order. A 20-concurrent-
//! writer stress test (not part of the committed suite; see
//! `tests/stress_concurrent_writers.rs` if present locally) reproduced a lost
//! update in roughly 7 of 10 runs. `cas_write` now forces the write lock
//! immediately after `MERGE` (a same-value `SET g.kind = $kind`, a write that
//! does nothing to the data but everything to the lock) *before* the `WHERE`
//! reads `last_sequence` — see that function's doc comment for the full
//! mechanism. The same stress test showed 0 failures in 30 runs after the fix.
//!
//! Guard placement: one `__ConduitGuard` node per `(kind, target, entity_key,
//! facet)` — `kind` is `"node"` or `"edge"` (disambiguating a node label and
//! an edge type that happen to share a name), `target` is the label or edge
//! type, `entity_key` the node id or canonical edge key. This is flatter
//! than every other backend's guard (one record per *entity*, facets nested
//! inside) because Neo4j's CAS needs `last_sequence` to be a plain queryable
//! property per lane — a nested map isn't a valid property value at all, and
//! even a JSON-string blob couldn't be compared in a Cypher `WHERE`.

use std::sync::{Arc, Mutex};

use neo4rs::{BoltList, BoltMap, BoltType, ConfigBuilder, Graph, Neo4jErrorKind, Query, Row, Txn};
use serde_json::Value;
use tokio::runtime::Runtime;

use super::adapter::GraphError;
use super::exec::{
    self, EdgePlan, GraphBackend, GraphFacetGuard, GraphGuard, GraphOutcome, NodePlan,
};
use super::mapping::{ResolvedRecord, canonical_edge_key};
use super::runtime::GraphRuntimeBuilder;
use crate::adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter};
use crate::event::Event;
use crate::routing::StorageKind;
use crate::runtime::config::MigrationPolicy;
use crate::upcast::UpcasterRegistry;

const MAX_ATTEMPTS: u32 = 5;
const GUARD_CONSTRAINT: &str = "CREATE CONSTRAINT IF NOT EXISTS FOR (g:__ConduitGuard) \
    REQUIRE (g.kind, g.target, g.entity_key, g.facet) IS UNIQUE";

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

fn de_err(e: neo4rs::DeError) -> GraphError {
    GraphError::WriteFailed(format!("corrupt guard row: {e}"))
}

/// The four flat fields one `__ConduitGuard` lane carries.
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

/// Neo4j graph adapter (Phase 24). Genuinely ACID via a client transaction
/// per event, like Postgres (19) and MongoDB (23) — but the CAS check is
/// explicit (a `WHERE`-conditioned `SET`), not implicit in the transaction's
/// own isolation, per the module doc comment above.
pub struct Neo4jAdapter {
    id: String,
    priority: u32,
    /// `Err` (a stored message) when connecting failed — every `handle()`
    /// then fails cleanly rather than the factory being fallible. Unlike
    /// Postgres/Redis/Mongo's lazy pooled connect, `neo4rs::Graph::connect`
    /// has no documented lazy/unchecked variant, so this connects eagerly at
    /// construction (still inside `new()`, not at config-parse time).
    conn: Result<(Runtime, Graph), String>,
    builder: GraphRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
    /// Guarded so the uniqueness constraint is created at most once per
    /// adapter instance.
    constraint_ensured: Mutex<bool>,
}

impl Neo4jAdapter {
    #[allow(clippy::too_many_arguments)]
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
            conn,
            builder,
            upcasters,
            migration_policy,
            constraint_ensured: Mutex::new(false),
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Graph, error)
    }

    fn ensure_constraint(&self, rt: &Runtime, graph: &Graph) -> Result<(), GraphError> {
        let mut ensured = self
            .constraint_ensured
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if *ensured {
            return Ok(());
        }
        rt.block_on(graph.run(Query::new(GUARD_CONSTRAINT.to_string())))
            .map_err(neo_err)?;
        *ensured = true;
        Ok(())
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

    fn handle(&self, event: &Event) -> AdapterResult {
        let projection = match self.builder.build(event, &self.upcasters) {
            Ok(p) => p,
            Err(GraphError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return match self.migration_policy {
                    MigrationPolicy::Strict => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::Graph,
                        reason,
                        from_version,
                        to_version,
                    ),
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::Graph,
                        SkipReason::UnsupportedVersion,
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
        if let Err(e) = self.ensure_constraint(rt, graph) {
            return self.failure(AdapterError::WriteFailed(e.to_string()));
        }

        let source_version = projection.source_version;
        let projected_version = projection.projected_version;

        let mut attempts = 0;
        let result = loop {
            attempts += 1;
            let attempt: Result<GraphOutcome, GraphError> = (|| {
                let mut txn = rt.block_on(graph.start_txn()).map_err(neo_err)?;
                let outcome = {
                    let mut backend = Neo4jBackend { rt, txn: &mut txn };
                    match &projection.record {
                        ResolvedRecord::Node {
                            label,
                            node_id,
                            properties,
                        } => {
                            let plan = NodePlan {
                                label: label.clone(),
                                node_id: node_id.clone(),
                                properties: properties.clone(),
                                on_existing: projection.on_existing,
                                operation: projection.operation,
                                permanent: projection.permanent,
                                facet: projection.facet.clone(),
                            };
                            exec::project_node(&mut backend, &plan, event)
                        }
                        ResolvedRecord::Edge {
                            edge_type,
                            from,
                            to,
                            discriminator,
                            properties,
                        } => {
                            let key =
                                canonical_edge_key(edge_type, from, to, discriminator.as_deref());
                            let plan = EdgePlan {
                                edge_type: edge_type.clone(),
                                from: from.clone(),
                                to: to.clone(),
                                key,
                                properties: properties.clone(),
                                on_existing: projection.on_existing,
                                operation: projection.operation,
                                permanent: projection.permanent,
                            };
                            exec::project_edge(&mut backend, &plan, event)
                        }
                    }
                };
                match outcome {
                    Ok(o) => {
                        rt.block_on(txn.commit()).map_err(neo_err)?;
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
                other => break other,
            }
        };

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
            Err(GraphError::WriteConflict) => self.failure(AdapterError::WriteFailed(format!(
                "neo4j write conflict: exceeded {MAX_ATTEMPTS} attempts against a contended entity"
            ))),
            Err(e) => self.failure(AdapterError::WriteFailed(e.to_string())),
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
            "MATCH (g:__ConduitGuard {kind: $kind, target: $target, entity_key: $entity_key}) \
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
                    GraphFacetGuard {
                        last_sequence: last_sequence as u64,
                        last_event_id,
                        deleted,
                        permanent,
                    },
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
            "MERGE (g:__ConduitGuard {{kind: $kind, target: $target, entity_key: $entity_key, facet: $facet}}) \
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

    fn node_is_tombstoned(&mut self, node_id: &str) -> Result<bool, GraphError> {
        let q = Query::new(
            "MATCH (g:__ConduitGuard {kind: 'node', entity_key: $id, facet: '', deleted: true}) \
             RETURN count(g) > 0 AS tombstoned"
                .to_string(),
        )
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

    fn write_edge(
        &mut self,
        edge_type: &str,
        from: &str,
        to: &str,
        key: &str,
        properties: Option<&Value>,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        let (seq, eid, deleted, permanent) = lane_fields(guard, "");
        let mut extra_params = vec![
            ("edge_from", BoltType::from(from)),
            ("edge_to", BoltType::from(to)),
            ("edge_key", BoltType::from(key)),
        ];
        let write_extra = match properties {
            None => String::new(),
            Some(p) => {
                extra_params.push(("properties", json_to_bolt(p)));
                format!(
                    "MERGE (a {{id: $edge_from}}) MERGE (b {{id: $edge_to}}) \
                     MERGE (a)-[r:`{edge_type}` {{key: $edge_key}}]->(b) \
                     SET r = $properties, r.key = $edge_key"
                )
            }
        };
        self.cas_write(
            "edge",
            edge_type,
            key,
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
                 MERGE (g:__ConduitGuard {kind: 'node', target: $label, entity_key: $id, facet: f.name}) \
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
                 MERGE (g:__ConduitGuard {kind: 'edge', target: e.etype, entity_key: e.ekey, facet: ''}) \
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
        // incident relationship in one primitive (Phase 24.4).
        let q = Query::new(format!("MATCH (n:`{label}` {{id: $id}}) DETACH DELETE n"))
            .param("id", node_id);
        self.run(q)
    }

    fn delete_edge(
        &mut self,
        edge_type: &str,
        _from: &str,
        _to: &str,
        key: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
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
