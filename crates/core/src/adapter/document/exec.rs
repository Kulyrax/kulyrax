//! Phase 23.1: the backend-agnostic document write path.
//!
//! The read-guard → facet existence pre-check → [`decide`] → apply
//! write/merge/delete → write guard sequence that used to be inlined in
//! `file.rs` lives here once. A backend supplies only a [`DocumentBackend`] —
//! how the guard is read and how the document/guard pair is atomically
//! written. Unlike the KV seam (Phase 22.1), a named facet's merge is **not**
//! done here: `write()` receives the facet's own unmerged fields plus the
//! facet name, and each backend decides how to apply it — a read-merge-write
//! for the file adapter, a native server-side `$set` for MongoDB (Phase
//! 23.3) — because MongoDB can do that atomically without a round trip this
//! layer would otherwise force.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::adapter::DocumentError;
use crate::adapter::{GuardState, OnExisting, Operation, SkipReason, WriteDecision, decide};
use crate::event::Event;
use crate::execution::stages::{Stage, timed};

// ---------------------------------------------------------------------------
// The guard — reused as-is as the wire format for every backend (Phase 23.1)
// ---------------------------------------------------------------------------

/// Idempotency guard — one record per entity (Phase 11.3/12.4/13.3/15.2). The
/// default facet stays in the flat fields (a pre-Phase-15 sidecar round-trips
/// unchanged); named-facet lanes live in `facets`. Serialized as-is for both
/// the file sidecar (JSON) and the MongoDB guard document (BSON) — no backend
/// gets its own guard schema.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct ProjectionGuard {
    #[serde(default)]
    pub last_sequence: u64,
    #[serde(default)]
    pub last_event_id: String,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub permanent: bool,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub facets: std::collections::BTreeMap<String, FacetGuard>,
}

/// One named-facet lane inside a [`ProjectionGuard`] (Phase 15.2).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct FacetGuard {
    pub last_sequence: u64,
    pub last_event_id: String,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub permanent: bool,
}

impl ProjectionGuard {
    /// The Phase 12/13 [`GuardState`] for one lane — flat fields for the
    /// default facet (`""`), a `facets` entry for a named one.
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

/// One backend's read/write surface for a single `(collection, entity_id)`
/// entity. Everything a driver genuinely must own — nothing about *when* to
/// write or *what* to decide (that is [`project`]).
///
/// **Public extension surface (Phase 25.1).** Implement this trait for a
/// document store Kulyrax doesn't ship (Elasticsearch, CouchDB, etc.) and
/// every document mapping feature — facets, deletes, tombstones,
/// resurrection — works for free. A breaking change to this trait is a
/// breaking change to `kulyrax-core`, tracked deliberately.
///
/// **The atomicity contract.** `write`/`delete` must apply the document (or
/// facet) and the guard together, indivisibly. Unlike the KV trait, a named
/// facet's merge is **not** done by [`project`] — `write` receives the
/// facet's own unmerged fields plus the facet name, so a backend that can do
/// a native partial update (MongoDB's `$set`) doesn't pay for a read it
/// doesn't need. Two shapes satisfy the atomicity contract in the built-in
/// backends:
/// - **File** ([`super::file`]): a single-writer assumption plus
///   write-to-temp-then-rename for the guard file.
/// - **MongoDB** (`kulyrax-adapter-mongodb`): a real client-session transaction
///   wraps the guard write and the target-document write together (they're
///   two different documents in two different collections); a concurrent
///   writer's commit makes ours fail with a retryable
///   (`TransientTransactionError`-labeled) error, mapped to
///   `Err(DocumentError::WriteConflict)`. The caller (`MongoDbAdapter::handle()`)
///   retries the whole transaction from scratch, bounded — copy that loop
///   shape for a new transactional backend.
pub trait DocumentBackend {
    /// Read the whole-entity guard (all facet lanes). `Ok(None)` means this
    /// entity has never been projected here.
    fn read_guard(
        &mut self,
        collection: &str,
        entity_id: &str,
    ) -> Result<Option<ProjectionGuard>, DocumentError>;

    /// Apply the resolved document (or a named facet's own fields) and the
    /// updated guard together. `document: None` means "leave the target
    /// document untouched, just persist the guard" (the `SkipAlreadyDeleted`
    /// sequence bump). `facet`: empty for the default facet (a full
    /// replace); non-empty names a facet whose top-level fields the backend
    /// merges into the existing document (read-merge-write for the file
    /// adapter; a native partial update for MongoDB).
    fn write(
        &mut self,
        collection: &str,
        entity_id: &str,
        document: Option<&Value>,
        facet: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError>;

    /// Atomically remove the document and write the tombstoned guard.
    fn delete(
        &mut self,
        collection: &str,
        entity_id: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError>;
}

/// What [`project`] resolved to — the adapter maps this onto an `AdapterResult`
/// with the version metadata it already holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DocumentOutcome {
    Created,
    Updated,
    Deleted,
    Skipped(SkipReason),
}

/// Everything [`project`] needs from the runtime builder — the resolved
/// identity, document, and the mapping's gating fields.
pub struct DocumentPlan {
    pub collection: String,
    pub entity_id: String,
    pub document: Value,
    pub on_existing: OnExisting,
    pub operation: Operation,
    pub permanent: bool,
    pub facet: String,
}

/// The Phase 11–15 document write path, once, over any [`DocumentBackend`].
///
/// Reads the guard exactly once (Phase 11.3 pattern — the pre-check and the
/// gating decision share one read). A backend whose `write`/`delete` can
/// abort under concurrent modification (MongoDB's session transactions,
/// Phase 23.3) signals that with `Err(DocumentError::WriteConflict)`; the
/// caller re-runs this function from scratch in a fresh transaction against a
/// freshly re-read guard.
pub fn project<B: DocumentBackend>(
    backend: &mut B,
    plan: &DocumentPlan,
    event: &Event,
) -> Result<DocumentOutcome, DocumentError> {
    let is_named_facet = !plan.facet.is_empty();

    let mut stored_guard = timed(Stage::Read, || {
        backend.read_guard(&plan.collection, &plan.entity_id)
    })?;

    // Phase 15.3: a named facet can only touch an entity the default facet
    // created and has not tombstoned.
    if is_named_facet {
        let present = stored_guard
            .as_ref()
            .and_then(|g| g.lane_state(""))
            .is_some_and(|s| !s.deleted);
        if !present {
            return Ok(DocumentOutcome::Skipped(SkipReason::EntityAbsent));
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
            return Ok(DocumentOutcome::Skipped(SkipReason::AlreadyProjected));
        }
        WriteDecision::SkipStale => {
            return Ok(DocumentOutcome::Skipped(SkipReason::StaleSequence));
        }
        WriteDecision::SkipTombstoned => {
            return Ok(DocumentOutcome::Skipped(SkipReason::Tombstoned));
        }
        WriteDecision::SkipAlreadyDeleted => {
            // Still bump last_sequence so a later, truly out-of-order
            // resurrection attempt compares against the highest delete
            // sequence seen, not a stale one. (Default facet only.)
            let mut g = stored_guard.take().unwrap_or_default();
            g.last_sequence = event.sequence;
            g.last_event_id = event.event_id.clone();
            g.deleted = true;
            timed(Stage::Write, || {
                backend.write(&plan.collection, &plan.entity_id, None, "", &g)
            })?;
            return Ok(DocumentOutcome::Skipped(SkipReason::AlreadyDeleted));
        }
        WriteDecision::Insert | WriteDecision::Update | WriteDecision::Delete => {}
    }

    if decision == WriteDecision::Delete {
        let mut g = stored_guard.take().unwrap_or_default();
        g.last_sequence = event.sequence;
        g.last_event_id = event.event_id.clone();
        g.deleted = true;
        g.permanent = plan.permanent;
        // Phase 15.3: cascade the tombstone to every named-facet lane.
        for fg in g.facets.values_mut() {
            fg.deleted = true;
            fg.permanent = plan.permanent;
            fg.last_sequence = event.sequence;
            fg.last_event_id = event.event_id.clone();
        }
        timed(Stage::Write, || {
            backend.delete(&plan.collection, &plan.entity_id, &g)
        })?;
        return Ok(DocumentOutcome::Deleted);
    }

    // Insert / Update.
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
            // Phase 15.3: a resurrection clears every named-facet lane too.
            for fg in g.facets.values_mut() {
                fg.deleted = false;
                fg.permanent = false;
            }
        }
    }
    timed(Stage::Write, || {
        backend.write(
            &plan.collection,
            &plan.entity_id,
            Some(&plan.document),
            &plan.facet,
            &g,
        )
    })?;

    Ok(match decision {
        WriteDecision::Insert => DocumentOutcome::Created,
        WriteDecision::Update => DocumentOutcome::Updated,
        _ => unreachable!("skip and delete decisions returned above"),
    })
}
