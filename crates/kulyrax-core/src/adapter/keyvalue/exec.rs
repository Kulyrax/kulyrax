//! Phase 22.1: the backend-agnostic key-value write path.
//!
//! The read-guard → facet existence pre-check → [`decide`] → apply
//! value write/merge/delete → write guard sequence that used to be inlined in
//! `store.rs` lives here once. A backend supplies only a [`KvBackend`] — how
//! the guard and value for one `(namespace, key)` are read and atomically
//! written. `decide()` and the orchestration are written a single time; the
//! file-backed store and Redis differ only in the four things a driver
//! genuinely must own.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::adapter::KvError;
use crate::adapter::{GuardState, OnExisting, Operation, SkipReason, WriteDecision, decide};
use crate::event::Event;

// ---------------------------------------------------------------------------
// The guard — reused as-is as the wire format for every backend (Phase 22.1)
// ---------------------------------------------------------------------------

/// Idempotency guard — one JSON blob per entity (Phase 11.3/12.4/13.3/15.2).
/// The default facet stays in the flat fields (a pre-Phase-15 sidecar round-trips
/// unchanged); named-facet lanes live in `facets`. Serialized as-is for both the
/// file sidecar and the Redis guard key — no backend gets its own guard schema.
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KvGuard {
    #[serde(default)]
    pub last_sequence: u64,
    #[serde(default)]
    pub last_event_id: String,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub permanent: bool,
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub facets: std::collections::BTreeMap<String, KvFacetGuard>,
}

/// One named-facet lane inside a [`KvGuard`] (Phase 15.2).
#[derive(Debug, Default, Clone, Serialize, Deserialize)]
pub struct KvFacetGuard {
    pub last_sequence: u64,
    pub last_event_id: String,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub permanent: bool,
}

impl KvGuard {
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

/// One backend's read/write surface for a single `(namespace, key)` entity.
/// Everything a driver genuinely must own — nothing about *when* to write or
/// *what* to decide (that is [`project`]).
///
/// **Public extension surface (Phase 25.1).** Implement this trait for a
/// key-value store Kulyrax doesn't ship (DynamoDB, Memcached, etc.) and
/// every KV mapping feature — facets, deletes, tombstones, resurrection —
/// works for free. A breaking change to this trait is a breaking change to
/// `kulyrax-core`, tracked deliberately.
///
/// **The atomicity contract.** `write`/`delete` must apply the value (or
/// facet merge) and the guard together, indivisibly — either both land or
/// neither does. Two shapes satisfy this in the built-in backends:
/// - **File** ([`super::store`]): a single-writer assumption plus
///   write-to-temp-then-rename for the guard file, so a reader never
///   observes a half-written guard.
/// - **Redis** ([`super::redis`]): no transaction with real isolation is
///   available, so `write`/`delete` use an optimistic `WATCH`/`MULTI`/`EXEC`
///   pipeline and return `Err(KvError::WriteConflict)` when a concurrent
///   writer touched the guard first. The caller (the adapter's own
///   `handle()`) retries the whole read-decide-write cycle, bounded — see
///   `RedisAdapter::handle()` for the exact loop shape to copy.
///
/// A backend with a real ACID transaction (a SQL-flavored KV store, say)
/// can skip the retry loop entirely and just commit; a backend without one
/// must implement the retry pattern to stay safe under concurrent writers.
pub trait KvBackend {
    /// Read the whole-entity guard (all facet lanes). `Ok(None)` means this
    /// key has never been projected here.
    fn read_guard(&mut self, namespace: &str, key: &str) -> Result<Option<KvGuard>, KvError>;

    /// Read the current value — used only for a named facet's shallow-merge.
    fn read_value(&mut self, namespace: &str, key: &str) -> Result<Option<Value>, KvError>;

    /// Atomically apply the resolved value (or merged facet) and the updated
    /// guard together. `value: None` means "leave the value key untouched,
    /// just persist the guard" (the `SkipAlreadyDeleted` sequence bump).
    fn write(
        &mut self,
        namespace: &str,
        key: &str,
        value: Option<&Value>,
        guard: &KvGuard,
    ) -> Result<(), KvError>;

    /// Atomically remove the value and write the tombstoned guard.
    fn delete(&mut self, namespace: &str, key: &str, guard: &KvGuard) -> Result<(), KvError>;
}

/// What [`project`] resolved to — the adapter maps this onto an `AdapterResult`
/// with the version metadata it already holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvOutcome {
    Created,
    Updated,
    Deleted,
    Skipped(SkipReason),
}

/// Everything [`project`] needs from the runtime builder — the resolved
/// identity, value, and the mapping's gating fields.
pub struct KvPlan {
    pub namespace: String,
    pub entity_key: String,
    pub value: Value,
    pub on_existing: OnExisting,
    pub operation: Operation,
    pub permanent: bool,
    pub facet: String,
}

/// The Phase 11–15 key-value write path, once, over any [`KvBackend`].
///
/// Reads the guard exactly once (Phase 11.3 pattern — the pre-check and the
/// gating decision share one read). A backend whose `write`/`delete` can
/// abort under concurrent modification (Redis's WATCH/MULTI/EXEC, Phase 22.3)
/// signals that with `Err(KvError::WriteConflict)`; the caller re-runs this
/// function from scratch against a freshly re-read guard.
pub fn project<B: KvBackend>(
    backend: &mut B,
    plan: &KvPlan,
    event: &Event,
) -> Result<KvOutcome, KvError> {
    let is_named_facet = !plan.facet.is_empty();

    let mut stored_guard = backend.read_guard(&plan.namespace, &plan.entity_key)?;

    // Phase 15.3: a named facet can only touch an entity the default facet
    // created and has not tombstoned.
    if is_named_facet {
        let present = stored_guard
            .as_ref()
            .and_then(|g| g.lane_state(""))
            .is_some_and(|s| !s.deleted);
        if !present {
            return Ok(KvOutcome::Skipped(SkipReason::EntityAbsent));
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
            return Ok(KvOutcome::Skipped(SkipReason::AlreadyProjected));
        }
        WriteDecision::SkipStale => {
            return Ok(KvOutcome::Skipped(SkipReason::StaleSequence));
        }
        WriteDecision::SkipTombstoned => {
            return Ok(KvOutcome::Skipped(SkipReason::Tombstoned));
        }
        WriteDecision::SkipAlreadyDeleted => {
            // Still bump last_sequence so a later, truly out-of-order
            // resurrection attempt compares against the highest delete
            // sequence seen, not a stale one. (Default facet only.)
            let mut g = stored_guard.take().unwrap_or_default();
            g.last_sequence = event.sequence;
            g.last_event_id = event.event_id.clone();
            g.deleted = true;
            backend.write(&plan.namespace, &plan.entity_key, None, &g)?;
            return Ok(KvOutcome::Skipped(SkipReason::AlreadyDeleted));
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
        backend.delete(&plan.namespace, &plan.entity_key, &g)?;
        return Ok(KvOutcome::Deleted);
    }

    // Insert / Update.
    let value_to_write = if is_named_facet {
        // Phase 15.4: shallow-merge the facet's own top-level keys into the
        // existing value; every other key is preserved.
        let facet_obj = plan.value.as_object().ok_or_else(|| {
            KvError::BuildFailed("facet mapping must resolve to a JSON object".into())
        })?;
        let mut existing = backend
            .read_value(&plan.namespace, &plan.entity_key)?
            .unwrap_or_else(|| Value::Object(Default::default()));
        let existing_obj = existing
            .as_object_mut()
            .ok_or_else(|| KvError::BuildFailed("existing value is not a JSON object".into()))?;
        for (k, v) in facet_obj {
            existing_obj.insert(k.clone(), v.clone());
        }
        existing
    } else {
        plan.value.clone()
    };

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
    backend.write(&plan.namespace, &plan.entity_key, Some(&value_to_write), &g)?;

    Ok(match decision {
        WriteDecision::Insert => KvOutcome::Created,
        WriteDecision::Update => KvOutcome::Updated,
        _ => unreachable!("skip and delete decisions returned above"),
    })
}
