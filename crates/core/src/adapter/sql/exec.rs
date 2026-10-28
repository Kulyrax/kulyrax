//! Phase 19.1: the backend-agnostic SQL write path.
//!
//! The read-guard-lane → [`decide`] → apply → record-guard transaction that
//! used to be inlined in `sqlite.rs` lives here once. A backend supplies only a
//! [`SqlTxn`] — connection, transaction, placeholder style, and DDL. `decide()`
//! and the orchestration are written a single time; SQLite and Postgres differ
//! only in the four things a driver genuinely must own.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use serde_json::Value;

use super::adapter::SqlError;
use super::runtime::SqlRuntimeBuilder;
use crate::adapter::{
    AdapterError, AdapterResult, GuardState, OnExisting, Operation, SkipReason, WriteDecision,
    decide,
};
use crate::event::Event;
use crate::execution::stages::{Stage, timed};
use crate::routing::StorageKind;
use crate::runtime::config::MigrationPolicy;
use crate::upcast::UpcasterRegistry;

// ---------------------------------------------------------------------------
// Neutral write description (Phase 19.1) — `SqlMapping::build` stops emitting a
// finished SQL string; each backend renders this.
// ---------------------------------------------------------------------------

/// One projection write, dialect-neutral. `conflict_target` empty → a plain
/// `INSERT` (`on_existing: ignore`); non-empty → `INSERT … ON CONFLICT (…) DO
/// UPDATE SET <set_columns = excluded.…>`.
#[derive(Debug, Clone, PartialEq)]
pub struct SqlWrite {
    pub table: String,
    /// All inserted columns, in a deterministic (sorted) order.
    pub columns: Vec<String>,
    /// Positionally paired with `columns`.
    pub values: Vec<Value>,
    /// The `ON CONFLICT (…)` target — the mapping's `primary_key` columns.
    pub conflict_target: Vec<String>,
    /// Columns that appear in `DO UPDATE SET` (all columns for a full replace,
    /// only the non-key columns for a named facet).
    pub set_columns: Vec<String>,
}

/// Placeholder style — originally the one real dialect branch (Phase 19.1);
/// as of Phase 25.3 it also picks the upsert clause, since MySQL has no
/// `ON CONFLICT` at all (see [`SqlWrite::render`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placeholders {
    /// SQLite: `?1, ?2, …`
    Question,
    /// Postgres: `$1, $2, …`
    Dollar,
    /// MySQL: bare, unnumbered `?` — the wire protocol has no numbered-
    /// placeholder syntax, params bind purely by position (Phase 25.3).
    MySql,
}

impl Placeholders {
    fn nth(self, i: usize) -> String {
        match self {
            Placeholders::Question => format!("?{i}"),
            Placeholders::Dollar => format!("${i}"),
            Placeholders::MySql => "?".to_string(),
        }
    }
}

impl SqlWrite {
    /// Render the `INSERT [… upsert …]` statement text. `ON CONFLICT (…) DO
    /// UPDATE SET …` is byte-identical between SQLite and Postgres — only the
    /// placeholder tokens differ. MySQL (Phase 25.3) has no `ON CONFLICT` at
    /// all: its upsert clause is `ON DUPLICATE KEY UPDATE`, keyed off the
    /// table's own primary key rather than a stated `conflict_target`.
    pub fn render(&self, ph: Placeholders) -> String {
        let cols = self.columns.join(", ");
        let vals = (1..=self.columns.len())
            .map(|i| ph.nth(i))
            .collect::<Vec<_>>()
            .join(", ");
        let mut sql = format!("INSERT INTO {} ({}) VALUES ({})", self.table, cols, vals);
        if !self.conflict_target.is_empty() {
            match ph {
                Placeholders::MySql => {
                    let set = self
                        .set_columns
                        .iter()
                        .map(|c| format!("{c} = VALUES({c})"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    sql.push_str(&format!(" ON DUPLICATE KEY UPDATE {set}"));
                }
                Placeholders::Question | Placeholders::Dollar => {
                    let set = self
                        .set_columns
                        .iter()
                        .map(|c| format!("{c} = excluded.{c}"))
                        .collect::<Vec<_>>()
                        .join(", ");
                    sql.push_str(&format!(
                        " ON CONFLICT ({}) DO UPDATE SET {}",
                        self.conflict_target.join(", "),
                        set
                    ));
                }
            }
        }
        sql
    }
}

/// The guard row `conduit_projection_state` — one lane for `(table, entity_key, facet)`.
#[derive(Debug, Clone, PartialEq)]
pub struct GuardRow {
    pub table: String,
    pub entity_key: String,
    pub facet: String,
    pub last_sequence: u64,
    pub last_event_id: String,
    pub deleted: bool,
    pub permanent: bool,
}

// ---------------------------------------------------------------------------
// Backend seam
// ---------------------------------------------------------------------------

/// One backend's transaction. Everything a driver genuinely must own — nothing
/// about *when* to write or *what* to decide (that is [`project`]).
///
/// **Public extension surface.** Implement this trait for a SQL store and
/// every SQL mapping feature (facets, deletes, tombstones, resurrection)
/// works unchanged; the trait is the entire contract. A breaking change here
/// is a breaking change to `kulyrax-core`.
///
/// **The atomicity contract.** `read_guard_lane` through `commit` all run
/// against the *same* underlying transaction: the guard read
/// (`read_guard_lane`), the target write (`execute_write`/`delete_row`), and
/// the guard write (`upsert_guard_row`/`bump_guard_lane`/`cascade_facets`)
/// must be indivisible from the perspective of any other writer — either all
/// of them land, or none do. Schema setup (`ensure_guard_table`) is kept out
/// of that transaction: see [`ensure_schema`]. How the shipped backends meet
/// it:
/// - **SQLite** ([`super::sqlite::SqliteTxn`]): SQLite serializes writers
///   itself, plus a real `rusqlite::Transaction`; `lock` is a no-op.
/// - **Postgres** (`kulyrax-adapter-postgres`): `READ COMMITTED` with
///   `SELECT … FOR UPDATE` on the guard row when `lock` is set.
/// - **MySQL** (`kulyrax-adapter-mysql`): the same pessimistic lock under
///   `READ COMMITTED` (not `REPEATABLE READ`, whose gap locks on missing guard
///   rows deadlock writers adding neighbouring entities).
///
/// **This seam has no built-in retry path** — unlike the KV/Document/Graph
/// traits below, `SqlError` carries no "someone else changed this, retry"
/// variant, and [`project`] never retries. A new backend needs a real
/// pessimistic lock reachable from a client transaction (`SELECT … FOR
/// UPDATE` or equivalent) to be safe under concurrent writers; a store
/// without one is only safe under this trait's single-writer assumption
/// (SQLite's own fallback). A backend needing optimistic retry instead would
/// have to extend `SqlError` with a conflict variant and add a retry loop
/// around its own `handle()` — the pattern `KvBackend`'s Redis
/// implementation and `GraphBackend`'s Neo4j implementation both use.
pub trait SqlTxn: Sized {
    /// `CREATE TABLE IF NOT EXISTS conduit_projection_state (…)` — Phase 11.2 /
    /// 13.2 / 15.2 shape, in the backend's own DDL. Only ever called through
    /// [`ensure_schema`], from the adapter's `StorageAdapter::prepare`, in a
    /// transaction of its own; never by [`project`].
    fn ensure_guard_table(&mut self) -> Result<(), SqlError>;

    /// Read one guard lane. `lock` → `SELECT … FOR UPDATE` where the backend
    /// supports it (Postgres, Phase 19.4); a no-op flag on SQLite (writers
    /// already serialize).
    fn read_guard_lane(
        &mut self,
        table: &str,
        entity_key: &str,
        facet: &str,
        lock: bool,
    ) -> Result<Option<GuardState>, SqlError>;

    /// Read many lanes of one table at once, for a batch (see
    /// [`prefetch_guards`]); the result is positionally paired with `lanes`
    /// (`(entity_key, facet)`). The default reads them one by one through
    /// [`SqlTxn::read_guard_lane`]; a backend overrides it with one query.
    fn read_guard_lanes(
        &mut self,
        table: &str,
        lanes: &[(String, String)],
        lock: bool,
    ) -> Result<Vec<Option<GuardState>>, SqlError> {
        lanes
            .iter()
            .map(|(key, facet)| self.read_guard_lane(table, key, facet, lock))
            .collect()
    }

    /// Apply the projection `INSERT [… ON CONFLICT …]`.
    fn execute_write(&mut self, w: &SqlWrite) -> Result<(), SqlError>;

    /// `DELETE FROM <table> WHERE <col> = ? [AND …]` (Phase 13.2).
    fn delete_row(&mut self, table: &str, cols: &[String], vals: &[Value]) -> Result<(), SqlError>;

    /// Upsert one guard lane's state (after a write).
    fn upsert_guard_row(&mut self, row: &GuardRow) -> Result<(), SqlError>;

    /// Bump `last_sequence` / `last_event_id` on one existing lane without
    /// changing `deleted` — Phase 13.2 `SkipAlreadyDeleted`.
    fn bump_guard_lane(
        &mut self,
        table: &str,
        entity_key: &str,
        facet: &str,
        seq: u64,
        event_id: &str,
    ) -> Result<(), SqlError>;

    /// Phase 15.3: cascade a default-facet delete's tombstone to every named
    /// facet lane, or (with `deleted = false`, `seq`/`event_id` ignored) clear
    /// them on a resurrection.
    fn cascade_facets(
        &mut self,
        table: &str,
        entity_key: &str,
        deleted: bool,
        permanent: bool,
        seq: u64,
        event_id: &str,
    ) -> Result<(), SqlError>;

    fn commit(self) -> Result<(), SqlError>;
}

/// What [`project`] resolved to — the adapter maps this onto an `AdapterResult`
/// with the version metadata it already holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlOutcome {
    Created,
    Updated,
    Deleted,
    Skipped(crate::adapter::SkipReason),
}

/// Everything [`project`] needs from the runtime builder — the neutral write
/// plus the resolved identity and the mapping's gating fields.
pub struct SqlPlan {
    pub table: String,
    pub entity_key: String,
    pub key_values: Vec<Value>,
    pub primary_key_columns: Vec<String>,
    pub on_existing: OnExisting,
    pub operation: Operation,
    pub permanent: bool,
    pub facet: String,
    pub write: SqlWrite,
}

/// Create the guard table in `txn`, a transaction of its own, and commit it.
///
/// A SQL adapter calls this from its `StorageAdapter::prepare`, never inside
/// an event's transaction: on MySQL, any DDL commits the open transaction
/// implicitly, even `CREATE TABLE IF NOT EXISTS` on a table that exists, and
/// every later statement then autocommits on its own — the guard read, target
/// write and guard write would stop being atomic.
pub fn ensure_schema<T: SqlTxn>(mut txn: T) -> Result<(), SqlError> {
    timed(Stage::Schema, || {
        txn.ensure_guard_table()?;
        txn.commit()
    })
}

/// Guard lanes already read or written in an open batch transaction, so
/// `decide()` reads each lane from the store at most once per batch and later
/// events for an entity see what earlier ones wrote. Keyed by
/// `(table, entity_key)`, then facet; `None` means no guard row.
#[derive(Debug, Default)]
pub struct GuardCache {
    entities: HashMap<(String, String), HashMap<String, Option<GuardState>>>,
}

impl GuardCache {
    fn get(&self, table: &str, entity_key: &str, facet: &str) -> Option<Option<GuardState>> {
        self.entities
            .get(&(table.to_string(), entity_key.to_string()))
            .and_then(|lanes| lanes.get(facet).copied())
    }

    fn set(&mut self, table: &str, entity_key: &str, facet: &str, state: Option<GuardState>) {
        self.entities
            .entry((table.to_string(), entity_key.to_string()))
            .or_default()
            .insert(facet.to_string(), state);
    }

    /// Apply [`SqlTxn::cascade_facets`] to the cached named-facet lanes of
    /// this entity, as its `UPDATE` does to the stored ones: existing lanes
    /// change, absent ones stay absent, and lanes not cached are read from
    /// the store (which has the update) when needed.
    fn cascade_facets(
        &mut self,
        table: &str,
        entity_key: &str,
        deleted: bool,
        permanent: bool,
        seq: u64,
    ) {
        let Some(lanes) = self
            .entities
            .get_mut(&(table.to_string(), entity_key.to_string()))
        else {
            return;
        };
        for (_, state) in lanes.iter_mut().filter(|(facet, _)| !facet.is_empty()) {
            if let Some(s) = state {
                if deleted {
                    *s = GuardState {
                        last_sequence: seq,
                        deleted: true,
                        permanent,
                    };
                } else {
                    s.deleted = false;
                    s.permanent = false;
                }
            }
        }
    }
}

/// Load every guard lane `plans` will read into `cache`, one
/// [`SqlTxn::read_guard_lanes`] call per table, locked (`FOR UPDATE` where the
/// backend supports it). A named facet also needs its entity's default lane.
/// Tables and lanes go in sorted order, so concurrent writers lock rows in
/// the same order.
pub fn prefetch_guards<'p, T: SqlTxn>(
    txn: &mut T,
    plans: impl IntoIterator<Item = &'p SqlPlan>,
    cache: &mut GuardCache,
) -> Result<(), SqlError> {
    let mut by_table: BTreeMap<&str, BTreeSet<(&str, &str)>> = BTreeMap::new();
    for plan in plans {
        let lanes = by_table.entry(plan.table.as_str()).or_default();
        lanes.insert((plan.entity_key.as_str(), plan.facet.as_str()));
        if !plan.facet.is_empty() {
            lanes.insert((plan.entity_key.as_str(), ""));
        }
    }
    for (table, lanes) in by_table {
        let lanes: Vec<(String, String)> = lanes
            .into_iter()
            .filter(|(key, facet)| cache.get(table, key, facet).is_none())
            .map(|(key, facet)| (key.to_string(), facet.to_string()))
            .collect();
        if lanes.is_empty() {
            continue;
        }
        let states = timed(Stage::Read, || txn.read_guard_lanes(table, &lanes, true))?;
        if states.len() != lanes.len() {
            return Err(SqlError::ExecutionFailed(format!(
                "read_guard_lanes returned {} states for {} lanes",
                states.len(),
                lanes.len()
            )));
        }
        for ((key, facet), state) in lanes.iter().zip(states) {
            cache.set(table, key, facet, state);
        }
    }
    Ok(())
}

/// One lane, from `cache` if it holds it, else from the store (then cached).
fn read_lane<T: SqlTxn>(
    txn: &mut T,
    cache: &mut GuardCache,
    table: &str,
    entity_key: &str,
    facet: &str,
) -> Result<Option<GuardState>, SqlError> {
    if let Some(state) = cache.get(table, entity_key, facet) {
        return Ok(state);
    }
    let state = timed(Stage::Read, || {
        txn.read_guard_lane(table, entity_key, facet, true)
    })?;
    cache.set(table, entity_key, facet, state);
    Ok(state)
}

/// The Phase 11–15 SQL write path, once, over any [`SqlTxn`], committing the
/// transaction on success: one event, one transaction. The adapter must have
/// been prepared (see [`ensure_schema`] and [`crate::adapter::Prepared`]).
pub fn project<T: SqlTxn>(
    mut txn: T,
    plan: &SqlPlan,
    event: &Event,
) -> Result<SqlOutcome, SqlError> {
    let outcome = project_in(&mut txn, plan, event, &mut GuardCache::default())?;
    timed(Stage::Commit, || txn.commit())?;
    Ok(outcome)
}

/// [`project`] without the commit, for an event inside a batch transaction:
/// guard lanes come from `cache` when it has them, and every guard write
/// updates it. The caller commits (or rolls back) the whole batch.
pub fn project_in<T: SqlTxn>(
    txn: &mut T,
    plan: &SqlPlan,
    event: &Event,
    cache: &mut GuardCache,
) -> Result<SqlOutcome, SqlError> {
    let is_named_facet = !plan.facet.is_empty();

    // Phase 15.3: a named facet can only touch a row the default facet created
    // and has not tombstoned.
    if is_named_facet {
        let default_state = read_lane(txn, cache, &plan.table, &plan.entity_key, "")?;
        let present = matches!(default_state, Some(s) if !s.deleted);
        if !present {
            return Ok(SqlOutcome::Skipped(SkipReason::EntityAbsent));
        }
    }

    // Phase 12.2 / 13.1 / 15.2: read this lane, decide, write — all in the txn.
    let stored = read_lane(txn, cache, &plan.table, &plan.entity_key, &plan.facet)?;
    let effective_mode = if is_named_facet {
        OnExisting::Replace
    } else {
        plan.on_existing
    };
    let decision = decide(plan.operation, effective_mode, stored, event.sequence);
    let was_tombstoned_default = !is_named_facet && matches!(stored, Some(s) if s.deleted);

    let outcome = match decision {
        WriteDecision::SkipIdempotent => {
            return Ok(SqlOutcome::Skipped(SkipReason::AlreadyProjected));
        }
        WriteDecision::SkipStale => return Ok(SqlOutcome::Skipped(SkipReason::StaleSequence)),
        WriteDecision::SkipTombstoned => return Ok(SqlOutcome::Skipped(SkipReason::Tombstoned)),
        WriteDecision::SkipAlreadyDeleted => {
            // Phase 13.2: still bump last_sequence so a later resurrection
            // compares against the highest delete sequence seen.
            timed(Stage::Write, || {
                txn.bump_guard_lane(
                    &plan.table,
                    &plan.entity_key,
                    &plan.facet,
                    event.sequence,
                    &event.event_id,
                )
            })?;
            if let Some(state) = stored {
                cache.set(
                    &plan.table,
                    &plan.entity_key,
                    &plan.facet,
                    Some(GuardState {
                        last_sequence: event.sequence,
                        ..state
                    }),
                );
            }
            return Ok(SqlOutcome::Skipped(SkipReason::AlreadyDeleted));
        }
        WriteDecision::Insert => SqlOutcome::Created,
        WriteDecision::Update => SqlOutcome::Updated,
        WriteDecision::Delete => SqlOutcome::Deleted,
    };

    let (deleted, row_permanent) = if decision == WriteDecision::Delete {
        (true, plan.permanent)
    } else {
        (false, false)
    };
    // Phase 15.3: a default-facet delete cascades its tombstone to every named
    // facet; a resurrection clears them.
    let cascade = !is_named_facet
        && (decision == WriteDecision::Delete
            || (was_tombstoned_default && decision == WriteDecision::Insert));

    // The target write and its guard row, timed as one stage.
    timed(Stage::Write, || -> Result<(), SqlError> {
        if decision == WriteDecision::Delete {
            txn.delete_row(&plan.table, &plan.primary_key_columns, &plan.key_values)?;
        } else {
            txn.execute_write(&plan.write)?;
        }

        txn.upsert_guard_row(&GuardRow {
            table: plan.table.clone(),
            entity_key: plan.entity_key.clone(),
            facet: plan.facet.clone(),
            last_sequence: event.sequence,
            last_event_id: event.event_id.clone(),
            deleted,
            permanent: row_permanent,
        })?;

        if cascade && decision == WriteDecision::Delete {
            txn.cascade_facets(
                &plan.table,
                &plan.entity_key,
                true,
                plan.permanent,
                event.sequence,
                &event.event_id,
            )?;
        } else if cascade {
            txn.cascade_facets(&plan.table, &plan.entity_key, false, false, 0, "")?;
        }
        Ok(())
    })?;

    cache.set(
        &plan.table,
        &plan.entity_key,
        &plan.facet,
        Some(GuardState {
            last_sequence: event.sequence,
            deleted,
            permanent: row_permanent,
        }),
    );
    if cascade {
        cache.cascade_facets(
            &plan.table,
            &plan.entity_key,
            deleted,
            row_permanent,
            event.sequence,
        );
    }

    Ok(outcome)
}

/// An event's [`SqlPlan`] and its observed / target schema versions.
pub struct PlannedEvent {
    pub plan: SqlPlan,
    pub source_version: u32,
    pub projected_version: u32,
}

/// Build `event`'s plan, the step every SQL adapter's `handle` starts with.
/// `Err` is the finished result to return instead: a skip or failure for a
/// version with no upcaster chain (per `policy`), or a failure to build.
pub fn plan_event(
    adapter_id: &str,
    builder: &SqlRuntimeBuilder,
    upcasters: &UpcasterRegistry,
    policy: MigrationPolicy,
    event: &Event,
) -> Result<PlannedEvent, AdapterResult> {
    let projection = match builder.build(event, upcasters) {
        Ok(p) => p,
        Err(SqlError::UnsupportedVersion {
            from_version,
            to_version,
            reason,
            ..
        }) => {
            return Err(match policy {
                MigrationPolicy::Strict => AdapterResult::unsupported_version(
                    adapter_id.to_string(),
                    StorageKind::Sql,
                    reason,
                    from_version,
                    to_version,
                ),
                MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                    adapter_id.to_string(),
                    StorageKind::Sql,
                    SkipReason::UnsupportedVersion,
                    from_version,
                    to_version,
                ),
            });
        }
        Err(e) => {
            return Err(AdapterResult::failure(
                adapter_id.to_string(),
                StorageKind::Sql,
                AdapterError::WriteFailed(e.to_string()),
            ));
        }
    };
    Ok(PlannedEvent {
        source_version: projection.source_version,
        projected_version: projection.projected_version,
        plan: SqlPlan {
            table: projection.table,
            entity_key: projection.entity_key,
            key_values: projection.key_values,
            primary_key_columns: projection.primary_key_columns,
            on_existing: projection.on_existing,
            operation: projection.operation,
            permanent: projection.permanent,
            facet: projection.facet,
            write: projection.write,
        },
    })
}

/// The adapter result for a [`project`] / [`project_in`] outcome.
pub fn outcome_result(
    adapter_id: &str,
    planned: &PlannedEvent,
    outcome: Result<SqlOutcome, SqlError>,
) -> AdapterResult {
    let (id, kind) = (adapter_id.to_string(), StorageKind::Sql);
    let (sv, pv) = (planned.source_version, planned.projected_version);
    match outcome {
        Ok(SqlOutcome::Created) => AdapterResult::created_versioned(id, kind, sv, pv),
        Ok(SqlOutcome::Updated) => AdapterResult::updated_versioned(id, kind, sv, pv),
        Ok(SqlOutcome::Deleted) => AdapterResult::deleted_versioned(id, kind, sv, pv),
        Ok(SqlOutcome::Skipped(reason)) => {
            AdapterResult::skipped_versioned(id, kind, reason, sv, pv)
        }
        Err(e) => AdapterResult::failure(id, kind, AdapterError::WriteFailed(e.to_string())),
    }
}

// ---------------------------------------------------------------------------
// Shared bind helper
// ---------------------------------------------------------------------------

/// Canonical text form of a JSON scalar for a text-affinity bind (SQLite, and
/// the fallback for a typed column in an external SQL backend). `pub` (not
/// `pub(crate)`) so a backend crate outside `kulyrax-core` — e.g.
/// `kulyrax-adapter-postgres`/`-mysql` — can reuse it for its own text-column
/// binding fallback, the same way the in-tree SQLite adapter does.
pub fn scalar_to_string(v: &Value) -> String {
    crate::adapter::json_scalar_to_string(v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn write(conflict: &[&str], set: &[&str]) -> SqlWrite {
        SqlWrite {
            table: "users".into(),
            columns: vec!["email".into(), "id".into()],
            values: vec![Value::String("a@b.com".into()), Value::String("u1".into())],
            conflict_target: conflict.iter().map(|s| s.to_string()).collect(),
            set_columns: set.iter().map(|s| s.to_string()).collect(),
        }
    }

    #[test]
    fn plain_insert_renders_without_on_conflict() {
        let w = write(&[], &[]);
        assert_eq!(
            w.render(Placeholders::Question),
            "INSERT INTO users (email, id) VALUES (?1, ?2)"
        );
        assert_eq!(
            w.render(Placeholders::Dollar),
            "INSERT INTO users (email, id) VALUES ($1, $2)"
        );
    }

    #[test]
    fn upsert_renders_identically_apart_from_placeholders() {
        let w = write(&["id"], &["email", "id"]);
        assert_eq!(
            w.render(Placeholders::Question),
            "INSERT INTO users (email, id) VALUES (?1, ?2) ON CONFLICT (id) DO UPDATE SET email = excluded.email, id = excluded.id"
        );
        assert_eq!(
            w.render(Placeholders::Dollar),
            "INSERT INTO users (email, id) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET email = excluded.email, id = excluded.id"
        );
    }

    #[test]
    fn facet_upsert_sets_only_non_key_columns() {
        let w = write(&["id"], &["email"]);
        assert_eq!(
            w.render(Placeholders::Dollar),
            "INSERT INTO users (email, id) VALUES ($1, $2) ON CONFLICT (id) DO UPDATE SET email = excluded.email"
        );
    }

    #[test]
    fn composite_conflict_target() {
        let w = write(&["post_id", "tag_id"], &["note"]);
        assert!(
            w.render(Placeholders::Question)
                .contains("ON CONFLICT (post_id, tag_id) DO UPDATE SET note = excluded.note")
        );
    }
}
