//! Phase 24.1: the backend-agnostic graph write path.
//!
//! Nodes and edges are separately-keyed records (Phase 16), each an
//! independent `decide()` state machine — so unlike the KV/document seams,
//! this one has two entry points, [`project_node`] and [`project_edge`],
//! mirroring the file store's own pre-24.1 `handle_node`/`handle_edge` split
//! rather than forcing an artificial single `project()`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::adapter::GraphError;
use crate::adapter::{GuardState, OnExisting, Operation, SkipReason, WriteDecision, decide};
use crate::event::Event;

// ---------------------------------------------------------------------------
// The guard — reused as-is as the wire format for every backend (Phase 24.1)
// ---------------------------------------------------------------------------

/// Idempotency guard for one graph record, node or edge. The default facet
/// stays in the flat fields; named-facet lanes (nodes only) live in
/// `facets`. This in-memory shape is shared by every backend, but the
/// *wire* shape isn't uniform the way it is for KV/Document: the file
/// sidecar serializes this struct as-is (one JSON file per entity, facets
/// nested), while Neo4j spreads it across one flat `__ConduitGuard` node
/// per facet lane instead — its CAS needs `last_sequence` to be a plain
/// queryable property, and a nested map isn't a valid property value at
/// all, so a single JSON blob (even as a string) couldn't be compared in a
/// Cypher `WHERE`.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GraphGuard {
    #[serde(default)]
    pub last_sequence: u64,
    #[serde(default)]
    pub last_event_id: String,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub permanent: bool,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub facets: std::collections::BTreeMap<String, GraphFacetGuard>,
}

#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct GraphFacetGuard {
    pub last_sequence: u64,
    pub last_event_id: String,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub permanent: bool,
}

impl GraphGuard {
    pub fn lane_state(&self, facet: &str) -> Option<GuardState> {
        if facet.is_empty() {
            Some(GuardState {
                last_sequence: self.last_sequence,
                deleted: self.deleted,
                permanent: self.permanent,
            })
        } else {
            self.facets.get(facet).map(|f| GuardState {
                last_sequence: f.last_sequence,
                deleted: f.deleted,
                permanent: f.permanent,
            })
        }
    }
}

// ---------------------------------------------------------------------------
// Backend seam
// ---------------------------------------------------------------------------

/// One backend's read/write surface for graph records. Everything a driver
/// genuinely must own — nothing about *when* to write or *what* to decide
/// (that is [`project_node`] / [`project_edge`]).
///
/// **Public extension surface (Phase 25.1).** Implement this trait for a
/// graph store Kulyrax doesn't ship (JanusGraph, ArangoDB, etc.) and every
/// graph mapping feature — node/edge facets, deletes, tombstones, detach-delete
/// cascades — works for free. A breaking change to this trait is a breaking
/// change to `kulyrax-core`, tracked deliberately.
///
/// **The atomicity contract.** `write_node`/`write_edge`/`delete_*` must
/// apply the record and the guard together, indivisibly. This is the
/// flattest of the four guard shapes by necessity, not by choice — see
/// [`GraphGuard`]'s own doc comment for why. Two shapes satisfy the contract
/// in the built-in backends:
/// - **File** ([`super::store`]): a single-writer assumption plus
///   write-to-temp-then-rename, with an explicit incident-edge index kept
///   alongside (Phase 16.2) since the filesystem has no graph structure of
///   its own to query.
/// - **Neo4j** (`kulyrax-adapter-neo4j`): one client-session transaction per event,
///   with an **explicit optimistic CAS** — a `WHERE`-conditioned `SET` on
///   the guard, re-checked against the *current* stored sequence at write
///   time — because Neo4j's per-node write locks alone do **not** prevent a
///   transaction from committing data it read before a concurrent writer
///   committed (unlike MongoDB's snapshot-isolated transactions). Zero rows
///   returned means the CAS lost; map that to `Err(GraphError::WriteConflict)`
///   and let the caller (`Neo4jAdapter::handle()`) retry the whole
///   transaction, bounded. A backend targeting a genuinely serializable /
///   snapshot-isolated store can skip the manual CAS and rely on the
///   transaction's own conflict detection instead, the way MongoDB does —
///   but verify that guarantee for the specific store before assuming it.
pub trait GraphBackend {
    /// Read a node's whole guard (all facet lanes). `Ok(None)` means this
    /// node has never been projected here.
    fn read_node_guard(
        &mut self,
        label: &str,
        node_id: &str,
    ) -> Result<Option<GraphGuard>, GraphError>;

    /// Read an edge's guard. Edges are not faceted — always the default lane.
    fn read_edge_guard(
        &mut self,
        edge_type: &str,
        key: &str,
    ) -> Result<Option<GraphGuard>, GraphError>;

    /// Is `node_id` explicitly tombstoned, under *any* label? (Phase 16.4: an
    /// edge may not attach to an explicitly-deleted node; an endpoint that
    /// was never projected at all is fine.)
    fn node_is_tombstoned(&mut self, node_id: &str) -> Result<bool, GraphError>;

    /// Apply the resolved node properties (or a named facet's own fields)
    /// and the updated guard together. `properties: None` means "leave the
    /// node untouched, just persist the guard" (the `SkipAlreadyDeleted`
    /// sequence bump). May return `Err(GraphError::WriteConflict)` if a
    /// backend's optimistic CAS lost a race — the caller retries from a
    /// fresh read.
    fn write_node(
        &mut self,
        label: &str,
        node_id: &str,
        properties: Option<&Value>,
        facet: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError>;

    /// Apply the resolved edge properties and the updated guard together.
    /// `properties: None` means "leave the edge untouched, just persist the
    /// guard". Edges are not faceted.
    fn write_edge(
        &mut self,
        edge_type: &str,
        from: &str,
        to: &str,
        key: &str,
        properties: Option<&Value>,
        guard: &GraphGuard,
    ) -> Result<(), GraphError>;

    /// Remove the node and every incident edge, tombstoning each at the same
    /// sequence (Phase 16.4 DETACH DELETE semantics) — native in Neo4j
    /// (`DETACH DELETE`), file-backed via the incident index.
    fn delete_node_cascade(
        &mut self,
        label: &str,
        node_id: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError>;

    /// Remove one edge and write its tombstoned guard.
    fn delete_edge(
        &mut self,
        edge_type: &str,
        from: &str,
        to: &str,
        key: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraphOutcome {
    Created,
    Updated,
    Deleted,
    Skipped(SkipReason),
}

pub struct NodePlan {
    pub label: String,
    pub node_id: String,
    pub properties: Value,
    pub on_existing: OnExisting,
    pub operation: Operation,
    pub permanent: bool,
    pub facet: String,
}

pub struct EdgePlan {
    pub edge_type: String,
    pub from: String,
    pub to: String,
    /// Canonical edge identity (`canonical_edge_key`) — stable across
    /// backends, opaque to this layer.
    pub key: String,
    pub properties: Value,
    pub on_existing: OnExisting,
    pub operation: Operation,
    pub permanent: bool,
}

fn dedup_pair<'a>(a: &'a str, b: &'a str) -> Vec<&'a str> {
    if a == b { vec![a] } else { vec![a, b] }
}

/// The Phase 11–16 node write path, once, over any [`GraphBackend`].
pub fn project_node<B: GraphBackend>(
    backend: &mut B,
    plan: &NodePlan,
    event: &Event,
) -> Result<GraphOutcome, GraphError> {
    let is_named_facet = !plan.facet.is_empty();
    let mut stored_guard = backend.read_node_guard(&plan.label, &plan.node_id)?;

    if is_named_facet {
        let present = stored_guard
            .as_ref()
            .and_then(|g| g.lane_state(""))
            .is_some_and(|s| !s.deleted);
        if !present {
            return Ok(GraphOutcome::Skipped(SkipReason::EntityAbsent));
        }
    }

    let stored: Option<GuardState> = stored_guard
        .as_ref()
        .and_then(|g| g.lane_state(&plan.facet));
    let effective_mode = if is_named_facet {
        OnExisting::Replace
    } else {
        plan.on_existing
    };
    let decision = decide(plan.operation, effective_mode, stored, event.sequence);
    let was_tombstoned_default = !is_named_facet && stored.is_some_and(|s| s.deleted);

    match decision {
        WriteDecision::SkipIdempotent => {
            return Ok(GraphOutcome::Skipped(SkipReason::AlreadyProjected));
        }
        WriteDecision::SkipStale => return Ok(GraphOutcome::Skipped(SkipReason::StaleSequence)),
        WriteDecision::SkipTombstoned => return Ok(GraphOutcome::Skipped(SkipReason::Tombstoned)),
        WriteDecision::SkipAlreadyDeleted => {
            let mut g = stored_guard.take().unwrap_or_default();
            g.last_sequence = event.sequence;
            g.last_event_id = event.event_id.clone();
            g.deleted = true;
            backend.write_node(&plan.label, &plan.node_id, None, "", &g)?;
            return Ok(GraphOutcome::Skipped(SkipReason::AlreadyDeleted));
        }
        WriteDecision::Insert | WriteDecision::Update | WriteDecision::Delete => {}
    }

    if decision == WriteDecision::Delete {
        let mut g = stored_guard.take().unwrap_or_default();
        g.last_sequence = event.sequence;
        g.last_event_id = event.event_id.clone();
        g.deleted = true;
        g.permanent = plan.permanent;
        for fg in g.facets.values_mut() {
            fg.deleted = true;
            fg.permanent = plan.permanent;
            fg.last_sequence = event.sequence;
            fg.last_event_id = event.event_id.clone();
        }
        backend.delete_node_cascade(&plan.label, &plan.node_id, &g)?;
        return Ok(GraphOutcome::Deleted);
    }

    let mut g = stored_guard.take().unwrap_or_default();
    if is_named_facet {
        let fg = g.facets.entry(plan.facet.clone()).or_default();
        fg.last_sequence = event.sequence;
        fg.last_event_id = event.event_id.clone();
        fg.deleted = false;
        fg.permanent = false;
    } else {
        g.last_sequence = event.sequence;
        g.last_event_id = event.event_id.clone();
        g.deleted = false;
        g.permanent = false;
        if was_tombstoned_default {
            for fg in g.facets.values_mut() {
                fg.deleted = false;
                fg.permanent = false;
            }
        }
    }
    backend.write_node(
        &plan.label,
        &plan.node_id,
        Some(&plan.properties),
        &plan.facet,
        &g,
    )?;

    Ok(match decision {
        WriteDecision::Insert => GraphOutcome::Created,
        WriteDecision::Update => GraphOutcome::Updated,
        _ => unreachable!("skip and delete decisions returned above"),
    })
}

/// The Phase 11–16 edge write path, once, over any [`GraphBackend`]. Edges
/// are not faceted.
pub fn project_edge<B: GraphBackend>(
    backend: &mut B,
    plan: &EdgePlan,
    event: &Event,
) -> Result<GraphOutcome, GraphError> {
    let mut stored_guard = backend.read_edge_guard(&plan.edge_type, &plan.key)?;
    let stored = stored_guard.as_ref().and_then(|g| g.lane_state(""));
    let decision = decide(plan.operation, plan.on_existing, stored, event.sequence);

    match decision {
        WriteDecision::SkipIdempotent => {
            return Ok(GraphOutcome::Skipped(SkipReason::AlreadyProjected));
        }
        WriteDecision::SkipStale => return Ok(GraphOutcome::Skipped(SkipReason::StaleSequence)),
        WriteDecision::SkipTombstoned => return Ok(GraphOutcome::Skipped(SkipReason::Tombstoned)),
        WriteDecision::SkipAlreadyDeleted => {
            let mut g = stored_guard.take().unwrap_or_default();
            g.last_sequence = event.sequence;
            g.last_event_id = event.event_id.clone();
            g.deleted = true;
            backend.write_edge(&plan.edge_type, &plan.from, &plan.to, &plan.key, None, &g)?;
            return Ok(GraphOutcome::Skipped(SkipReason::AlreadyDeleted));
        }
        WriteDecision::Insert | WriteDecision::Update | WriteDecision::Delete => {}
    }

    // Phase 16.4: an edge may not attach to an explicitly-tombstoned node.
    // An endpoint that was *never* projected is fine (16.3, permissive).
    if decision != WriteDecision::Delete {
        for endpoint in dedup_pair(&plan.from, &plan.to) {
            if backend.node_is_tombstoned(endpoint)? {
                return Ok(GraphOutcome::Skipped(SkipReason::EntityAbsent));
            }
        }
    }

    let mut g = stored_guard.take().unwrap_or_default();
    g.last_sequence = event.sequence;
    g.last_event_id = event.event_id.clone();
    g.deleted = decision == WriteDecision::Delete;
    g.permanent = decision == WriteDecision::Delete && plan.permanent;

    if decision == WriteDecision::Delete {
        backend.delete_edge(&plan.edge_type, &plan.from, &plan.to, &plan.key, &g)?;
        return Ok(GraphOutcome::Deleted);
    }

    backend.write_edge(
        &plan.edge_type,
        &plan.from,
        &plan.to,
        &plan.key,
        Some(&plan.properties),
        &g,
    )?;

    Ok(match decision {
        WriteDecision::Insert => GraphOutcome::Created,
        WriteDecision::Update => GraphOutcome::Updated,
        _ => unreachable!("skip and delete decisions returned above"),
    })
}
