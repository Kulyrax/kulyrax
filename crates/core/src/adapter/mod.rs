pub mod document;
pub mod graph;
pub mod keyvalue;
pub mod sql;

use core::fmt;

use serde::{Deserialize, Serialize};

use crate::event::Event;
use crate::routing::StorageKind;

/// Adapter-level execution failure. Deliberately narrow — everything that
/// isn't a hard failure (idempotent skip, stale-sequence skip, version
/// mismatch under `IgnoreUnmatched`) is a [`SkipReason`] on [`AdapterOutcome`]
/// instead, not an error (Phase 12.1).
#[derive(Debug)]
pub enum AdapterError {
    WriteFailed(String),
    /// No upcaster chain to the mapping's target version, under `MigrationPolicy::Strict`.
    UnsupportedVersion(String),
}

impl fmt::Display for AdapterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AdapterError::WriteFailed(msg) => write!(f, "{}", msg),
            AdapterError::UnsupportedVersion(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for AdapterError {}

/// Why an adapter skipped an event without writing anything (Phase 12.1;
/// extended Phase 13.1). Machine-readable — call sites match on this instead
/// of string-sniffing a message, and it round-trips through
/// [`crate::execution::AdapterOutcome`] into the JSON execution report.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SkipReason {
    /// Phase 11 idempotency: this entity already has a guard row/marker, and
    /// the mapping's `on_existing` is `ignore` (the default).
    AlreadyProjected,
    /// Phase 12.2: `on_existing: replace`, but `event.sequence` is not newer
    /// than the guard's `last_sequence` — a redelivery or a superseded update.
    StaleSequence,
    /// `MigrationPolicy::IgnoreUnmatched`: no upcaster chain to the mapping's
    /// target version; the event is skipped rather than failing the batch.
    UnsupportedVersion,
    /// Phase 13.2/13.3: a redelivered (or otherwise non-newer) `delete` for an
    /// entity that is already tombstoned. `last_sequence` is still bumped —
    /// see [`decide`].
    AlreadyDeleted,
    /// Phase 13.4: this entity's tombstone is `permanent` — every later event
    /// for this key, `delete` or `upsert`, is rejected. Terminal; nothing
    /// resurrects it.
    Tombstoned,
    /// Phase 15.3: a named-facet update whose entity does not exist (or is
    /// tombstoned) in the default facet. A facet cannot partially-update a row
    /// that was never created, and only a default-facet `upsert` resurrects a
    /// deleted entity — a facet update never does. Not a `decide()` outcome:
    /// the adapter checks the default-facet guard *before* running `decide()`
    /// on the facet's own lane.
    EntityAbsent,
}

/// Outcome of a single adapter invocation (Phase 12.1; extended Phase 13.1
/// with `Deleted`). Replaces the old `success: bool` +
/// `AdapterError::Skipped(String)` encoding with one enum.
#[derive(Debug)]
pub enum AdapterOutcome {
    /// A new entity was written (nothing existed yet, or it's a resurrection — Phase 13.4).
    Created,
    /// An existing entity was overwritten (`on_existing: replace` only).
    Updated,
    /// The entity was removed and its tombstone recorded (Phase 13.2/13.3).
    Deleted,
    /// Nothing was written; see [`SkipReason`] for why.
    Skipped(SkipReason),
    /// The adapter failed; see [`AdapterError`] for why.
    Failed(AdapterError),
}

#[derive(Debug)]
pub struct AdapterResult {
    pub adapter_id: String,
    pub kind: StorageKind,
    pub outcome: AdapterOutcome,
    /// Event payload version this adapter observed (Phase 10.3).
    pub source_version: Option<u32>,
    /// Mapping's target schema version this adapter projected into (Phase 10.3).
    pub projected_version: Option<u32>,
}

impl AdapterResult {
    /// True for every outcome except [`AdapterOutcome::Failed`] (created, updated, deleted, and skipped are all non-failures).
    pub fn is_success(&self) -> bool {
        !matches!(self.outcome, AdapterOutcome::Failed(_))
    }

    pub fn is_skipped(&self) -> bool {
        matches!(self.outcome, AdapterOutcome::Skipped(_))
    }

    pub fn created(adapter_id: String, kind: StorageKind) -> Self {
        Self {
            adapter_id,
            kind,
            outcome: AdapterOutcome::Created,
            source_version: None,
            projected_version: None,
        }
    }

    pub fn created_versioned(
        adapter_id: String,
        kind: StorageKind,
        source_version: u32,
        projected_version: u32,
    ) -> Self {
        Self {
            adapter_id,
            kind,
            outcome: AdapterOutcome::Created,
            source_version: Some(source_version),
            projected_version: Some(projected_version),
        }
    }

    /// Phase 12.3/12.4: an existing entity's projected state was overwritten.
    pub fn updated_versioned(
        adapter_id: String,
        kind: StorageKind,
        source_version: u32,
        projected_version: u32,
    ) -> Self {
        Self {
            adapter_id,
            kind,
            outcome: AdapterOutcome::Updated,
            source_version: Some(source_version),
            projected_version: Some(projected_version),
        }
    }

    /// Phase 13.2/13.3: the entity was removed and its tombstone recorded.
    pub fn deleted_versioned(
        adapter_id: String,
        kind: StorageKind,
        source_version: u32,
        projected_version: u32,
    ) -> Self {
        Self {
            adapter_id,
            kind,
            outcome: AdapterOutcome::Deleted,
            source_version: Some(source_version),
            projected_version: Some(projected_version),
        }
    }

    pub fn failure(adapter_id: String, kind: StorageKind, error: AdapterError) -> Self {
        Self {
            adapter_id,
            kind,
            outcome: AdapterOutcome::Failed(error),
            source_version: None,
            projected_version: None,
        }
    }

    pub fn skipped(adapter_id: String, kind: StorageKind, reason: SkipReason) -> Self {
        Self {
            adapter_id,
            kind,
            outcome: AdapterOutcome::Skipped(reason),
            source_version: None,
            projected_version: None,
        }
    }

    /// Skip carrying the observed/target schema versions (idempotent replay,
    /// stale-sequence rejection, already-deleted, tombstoned, or
    /// `MigrationPolicy::IgnoreUnmatched`).
    pub fn skipped_versioned(
        adapter_id: String,
        kind: StorageKind,
        reason: SkipReason,
        source_version: u32,
        projected_version: u32,
    ) -> Self {
        Self {
            adapter_id,
            kind,
            outcome: AdapterOutcome::Skipped(reason),
            source_version: Some(source_version),
            projected_version: Some(projected_version),
        }
    }

    /// `MigrationPolicy::Strict`: no upcaster chain; the adapter (and, under
    /// fail-fast, the batch) fails immediately.
    pub fn unsupported_version(
        adapter_id: String,
        kind: StorageKind,
        message: String,
        source_version: u32,
        projected_version: u32,
    ) -> Self {
        Self {
            adapter_id,
            kind,
            outcome: AdapterOutcome::Failed(AdapterError::UnsupportedVersion(message)),
            source_version: Some(source_version),
            projected_version: Some(projected_version),
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 12.2 / 13.1: operation, write mode & the gated decision
// ---------------------------------------------------------------------------

/// Mapping-level write mode for an entity that already has a projected state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnExisting {
    /// Entity already projected → clean skip (`SkipReason::AlreadyProjected`).
    /// Identical to Phase 11 behavior; the default, so every mapping written
    /// before Phase 12 is unchanged.
    #[default]
    Ignore,
    /// Entity already projected → overwrite the whole projected state, gated
    /// by `sequence` (Phase 12).
    Replace,
}

/// What a mapping does (Phase 13.1). `on_existing` is read only when
/// `operation: upsert` — orthogonal fields, not a combined enum, so adding a
/// third operation later doesn't multiply the matrix.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    /// Create or update the entity (Phase 11/12 behavior). Default.
    #[default]
    Upsert,
    /// Remove the entity identified by the resolved key (Phase 13.2/13.3).
    Delete,
}

/// The guard's persisted state for one entity, as read before deciding
/// (Phase 13.1). `None` (no `GuardState` at all) means this entity has never
/// been projected here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GuardState {
    pub last_sequence: u64,
    /// This entity's tombstone is currently set (Phase 13.2/13.3).
    pub deleted: bool,
    /// The tombstone is permanent (Phase 13.4) — `decide` short-circuits to
    /// `SkipTombstoned` for every op once this is true, and nothing ever
    /// clears it.
    pub permanent: bool,
}

/// What an adapter should do for one event, given the entity's current guard
/// state. Pure and side-effect free — no I/O, no adapter, exhaustively unit
/// tested below.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteDecision {
    Insert,
    Update,
    /// Phase 13.2/13.3: remove the entity and record the tombstone.
    Delete,
    SkipIdempotent,
    SkipStale,
    /// Phase 13.2/13.3: a `delete` landed on an already-tombstoned entity with
    /// a newer sequence — bump `last_sequence` on the tombstone, write nothing else.
    SkipAlreadyDeleted,
    /// Phase 13.4: this entity's tombstone is permanent.
    SkipTombstoned,
}

/// The gated decision (Phase 12.2, extended Phase 13.1 with `operation` and
/// tombstone-aware `GuardState`).
///
/// A permanent tombstone (`stored.permanent`) short-circuits every other rule:
/// once set, every later event for that key is `SkipTombstoned`, regardless
/// of `op`, `mode`, or `event_seq`.
///
/// | op        | stored                  | `event_seq` vs `last_sequence` | decision           |
/// |-----------|-------------------------|----------------------------------|--------------------|
/// | `delete`  | none                    | —                                 | `Delete`             |
/// | `delete`  | live (not deleted)      | `>`                               | `Delete`             |
/// | `delete`  | live (not deleted)      | `<=`                              | `SkipStale`          |
/// | `delete`  | tombstoned              | `>`                               | `SkipAlreadyDeleted` |
/// | `delete`  | tombstoned              | `<=`                              | `SkipStale`          |
/// | `upsert`  | none                    | —                                 | `Insert`             |
/// | `upsert`  | tombstoned              | `>`                               | `Insert` (resurrection) |
/// | `upsert`  | tombstoned              | `<=`                              | `SkipStale`          |
/// | `upsert`  | live, `mode: ignore`    | —                                 | `SkipIdempotent`     |
/// | `upsert`  | live, `mode: replace`   | `>`                               | `Update`             |
/// | `upsert`  | live, `mode: replace`   | `<=`                              | `SkipStale`          |
pub fn decide(
    op: Operation,
    mode: OnExisting,
    stored: Option<GuardState>,
    event_seq: u64,
) -> WriteDecision {
    if let Some(state) = stored
        && state.permanent
    {
        return WriteDecision::SkipTombstoned;
    }

    match op {
        Operation::Delete => match stored {
            None => WriteDecision::Delete,
            Some(state) if state.deleted => {
                if event_seq > state.last_sequence {
                    WriteDecision::SkipAlreadyDeleted
                } else {
                    WriteDecision::SkipStale
                }
            }
            Some(state) => {
                if event_seq > state.last_sequence {
                    WriteDecision::Delete
                } else {
                    WriteDecision::SkipStale
                }
            }
        },
        Operation::Upsert => match stored {
            None => WriteDecision::Insert,
            Some(state) if state.deleted => {
                if event_seq > state.last_sequence {
                    WriteDecision::Insert
                } else {
                    WriteDecision::SkipStale
                }
            }
            Some(state) => match mode {
                OnExisting::Ignore => WriteDecision::SkipIdempotent,
                OnExisting::Replace => {
                    if event_seq > state.last_sequence {
                        WriteDecision::Update
                    } else {
                        WriteDecision::SkipStale
                    }
                }
            },
        },
    }
}

// ---------------------------------------------------------------------------
// Shared value helpers (Phase 11.1)
// ---------------------------------------------------------------------------

/// Canonical string form of a resolved JSON leaf value (Phase 11.1). Shared by
/// the SQL adapter (legacy string-typed bind params) and the document adapter
/// (`entity_id` guard key / output filename) so the same payload/metadata
/// value always canonicalizes the same way across adapters. Objects and
/// arrays fall back to their JSON text — mapping authors should not use those
/// as an identity or column value (identity paths reject them outright; see
/// [`is_identity_scalar`]).
pub fn json_scalar_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// True for the value types Phase 11 accepts as an entity-identity component
/// (a document `id`, or one column of a SQL `primary_key`): string, number,
/// bool. Objects, arrays, and null are rejected — a path that resolves to the
/// wrong place should fail the build fast, not silently produce `""` or a
/// JSON blob inside a guard key.
pub(crate) fn is_identity_scalar(value: &serde_json::Value) -> bool {
    matches!(
        value,
        serde_json::Value::String(_) | serde_json::Value::Number(_) | serde_json::Value::Bool(_)
    )
}

pub trait StorageAdapter {
    /// Logical storage family (Sql / Document)
    fn kind(&self) -> StorageKind;

    /// Stable adapter identifier (from config)
    fn id(&self) -> &str;

    /// Execution priority (lower = earlier)
    fn priority(&self) -> u32;

    /// Execute side effect
    fn handle(&self, event: &Event) -> AdapterResult;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn live(last_sequence: u64) -> Option<GuardState> {
        Some(GuardState {
            last_sequence,
            deleted: false,
            permanent: false,
        })
    }

    fn tombstoned(last_sequence: u64) -> Option<GuardState> {
        Some(GuardState {
            last_sequence,
            deleted: true,
            permanent: false,
        })
    }

    fn permanent_tombstone(last_sequence: u64) -> Option<GuardState> {
        Some(GuardState {
            last_sequence,
            deleted: true,
            permanent: true,
        })
    }

    // --- upsert, no tombstone involved (Phase 12 behavior unchanged) ---

    #[test]
    fn ignore_absent_inserts() {
        assert_eq!(
            decide(Operation::Upsert, OnExisting::Ignore, None, 1),
            WriteDecision::Insert
        );
    }

    #[test]
    fn ignore_present_skips_idempotent_regardless_of_sequence() {
        assert_eq!(
            decide(Operation::Upsert, OnExisting::Ignore, live(99), 1),
            WriteDecision::SkipIdempotent
        );
    }

    #[test]
    fn replace_absent_inserts() {
        assert_eq!(
            decide(Operation::Upsert, OnExisting::Replace, None, 1),
            WriteDecision::Insert
        );
    }

    #[test]
    fn replace_present_with_newer_sequence_updates() {
        assert_eq!(
            decide(Operation::Upsert, OnExisting::Replace, live(5), 6),
            WriteDecision::Update
        );
    }

    #[test]
    fn replace_present_with_equal_sequence_is_stale() {
        assert_eq!(
            decide(Operation::Upsert, OnExisting::Replace, live(5), 5),
            WriteDecision::SkipStale
        );
    }

    #[test]
    fn replace_present_with_older_sequence_is_stale() {
        assert_eq!(
            decide(Operation::Upsert, OnExisting::Replace, live(5), 4),
            WriteDecision::SkipStale
        );
    }

    // --- delete (Phase 13.1) ---

    #[test]
    fn delete_absent_still_writes_a_tombstone() {
        assert_eq!(
            decide(Operation::Delete, OnExisting::Ignore, None, 1),
            WriteDecision::Delete
        );
    }

    #[test]
    fn delete_live_with_newer_sequence_deletes() {
        assert_eq!(
            decide(Operation::Delete, OnExisting::Ignore, live(5), 6),
            WriteDecision::Delete
        );
    }

    #[test]
    fn delete_live_with_stale_sequence_is_stale() {
        assert_eq!(
            decide(Operation::Delete, OnExisting::Ignore, live(5), 5),
            WriteDecision::SkipStale
        );
    }

    #[test]
    fn delete_already_tombstoned_with_newer_sequence_bumps_and_skips() {
        assert_eq!(
            decide(Operation::Delete, OnExisting::Ignore, tombstoned(5), 6),
            WriteDecision::SkipAlreadyDeleted
        );
    }

    #[test]
    fn delete_already_tombstoned_with_stale_sequence_is_stale() {
        assert_eq!(
            decide(Operation::Delete, OnExisting::Ignore, tombstoned(5), 5),
            WriteDecision::SkipStale
        );
    }

    // --- resurrection (Phase 13.4) ---

    #[test]
    fn upsert_on_tombstone_with_newer_sequence_resurrects() {
        assert_eq!(
            decide(Operation::Upsert, OnExisting::Replace, tombstoned(5), 6),
            WriteDecision::Insert
        );
        assert_eq!(
            decide(Operation::Upsert, OnExisting::Ignore, tombstoned(5), 6),
            WriteDecision::Insert
        );
    }

    #[test]
    fn upsert_on_tombstone_with_stale_sequence_stays_deleted() {
        assert_eq!(
            decide(Operation::Upsert, OnExisting::Replace, tombstoned(5), 5),
            WriteDecision::SkipStale
        );
    }

    // --- permanent tombstone short-circuits everything (Phase 13.4) ---

    #[test]
    fn permanent_tombstone_rejects_delete_regardless_of_sequence() {
        assert_eq!(
            decide(
                Operation::Delete,
                OnExisting::Ignore,
                permanent_tombstone(5),
                999
            ),
            WriteDecision::SkipTombstoned
        );
    }

    #[test]
    fn permanent_tombstone_rejects_upsert_regardless_of_sequence() {
        assert_eq!(
            decide(
                Operation::Upsert,
                OnExisting::Replace,
                permanent_tombstone(5),
                999
            ),
            WriteDecision::SkipTombstoned
        );
    }
}
