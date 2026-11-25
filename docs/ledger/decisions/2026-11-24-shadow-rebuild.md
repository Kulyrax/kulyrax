---
status: accepted
---

# Rebuild a SQL projection in shadow tables, swapped in with one transaction

Task: [2026-11-24-shadow-rebuild](../tasks/2026-11-24-shadow-rebuild.md)

## Context

The roadmap listed "a dedicated rebuild path: `COPY` into a shadow table, then an
atomic swap, instead of rebuilding through the per-event guards". Until now a
rebuild meant projecting the history into an empty store with `run --once` or
`replay`, and pointing readers at it by hand, or emptying the live tables first
and leaving readers with a partial projection until the replay finished.

The owner chose (2026-11-24) the variant where **other writers are paused** for the
rebuild, over one where live ingest keeps running and the swap drains what it
projected meanwhile.

Constraints: `decide()` and the batch path unchanged; no storage-kind special case
in `dispatch.rs`; the CLI goes through `pipeline`.

## Options considered

1. **Shadow tables through the normal write path, swapped in one transaction.**
   Point the adapter's SQL mappings at `<table>__kulyrax_rebuild`, replay the
   history through the unchanged batch path, then in one transaction replace
   each table's guard lanes and the table itself. Needs transactional DDL.
2. **`COPY` the final rows into the shadow.** Faster in principle, but `COPY`
   cannot update a row a later event changes, so the final state would have to
   be computed in memory for the whole history first (or `decide()` reimplemented
   in SQL, which CLAUDE.md rules out). Postgres only.
3. **Truncate and replay in place.** Simplest, but readers see a partial
   projection for the length of the rebuild, and a failed rebuild leaves it so.

## Evidence

- Verified (source): a rebuild into fresh tables already skips guard lookups
  (`KnownGuards`, v0.10), and Postgres writes each batch in bulk. The 1M-event
  rebuild measured 13,022 events/s on Postgres (task
  [2026-10-28-batch-transactions](../tasks/2026-10-28-batch-transactions.md)),
  past the 10,000 events/s target. `COPY` would only matter past that, and
  benchmarking is off for now (owner, 2026-11-24): its gain cannot be shown.
- Verified (docs): SQLite and Postgres run `CREATE`/`DROP`/`ALTER TABLE` inside a
  transaction. MySQL commits implicitly before and after each such statement, so
  its table swap could not be one transaction with the guard-lane move.
- Verified (tests): see the task.

## Decision

Option 1, as `kulyrax rebuild --adapter <id>` (`pipeline::rebuild`,
`crate::rebuild`), for SQLite and Postgres.

- The rebuild builds a runtime with that adapter alone, routing only the event
  types routed to it, and their SQL mappings pointed at the shadow tables.
- **Start** (`ShadowRebuild::start_rebuild`, one transaction): create each
  shadow table empty, shaped like its table (Postgres `LIKE … INCLUDING ALL`;
  SQLite from the table's and its indexes' own `CREATE` statements), delete any
  guard lanes left under the shadow's name, and take the target tables'
  **guard fingerprint**: the number of guard lanes and the sum of their
  sequences.
- **Replay** the history into the shadow tables through the normal batch path. If
  any event fails, stop: nothing is swapped, the shadows stay for inspection.
- **Swap** (`ShadowRebuild::finish_rebuild`, one transaction): lock the target
  tables (Postgres `LOCK TABLE … ACCESS EXCLUSIVE`; SQLite writers already
  serialize on the database), take the fingerprint again with the guard lanes
  locked, and refuse if it changed. Otherwise delete each table's guard lanes,
  rename the shadow's to the table's name, drop the table and rename the shadow
  in its place.

**Refused at preflight.** Before creating anything, `start_rebuild` asks each
target table for objects the swap would break or drop
(`SqlTxn::rebuild_blockers`), and refuses with every one named, table first
(`users: view users_active`). Nothing is created and no guard lane is touched;
the error reaches the CLI as `PipelineError::Rebuild`.
- Postgres: a view depending on the table (`pg_depend` / `pg_rewrite`); a
  foreign key into or out of it (`pg_constraint`; `LIKE` copies none, and an
  incoming one blocks the `DROP`); a column owning a sequence, `serial` or
  identity (`pg_get_serial_sequence`; table and shadow would share it, and the
  `DROP` takes it along).
- SQLite: a view or trigger whose SQL names the table as a whole word, or that
  is defined on it (the `DROP` would remove its triggers and leave views naming
  a table that is about to be replaced); a foreign key pointing at it, from
  another table or itself.

**Why a fingerprint, not a lock.** Holding live writers off would mean every live
SQL adapter taking a lock the rebuild can see. The fingerprint needs nothing from
them: every projection write either adds a guard lane or raises one's sequence
(`decide()` never writes a lower sequence), so count and sum change with any
write. Checked under the table lock, nothing can change between the check and
the swap.

**Indexes.** The swap renames the table, not its indexes. On SQLite the shadow's
indexes are named with the suffix added, or removed if the name already has it,
so names alternate between rebuilds instead of growing. Postgres names the
shadow's indexes itself.

## Trade-offs accepted

- Other writers to the rebuilt tables must be stopped; a writer that keeps
  running makes the swap refuse (detected, not prevented).
- No `COPY`: the rebuild runs at the batch path's speed.
- MySQL is not supported.
- During the swap, Postgres readers of the tables wait for the
  `ACCESS EXCLUSIVE` lock (milliseconds: renames and a drop).
- Tables with dependent views, foreign keys, triggers (SQLite) or sequence-owning
  columns (Postgres) cannot be rebuilt this way at all: they are refused at
  preflight, not handled.
- New public API: `StorageAdapter::shadow_rebuild`, `crate::rebuild`
  (`ShadowRebuild`, `rebuild`, `RebuildReport`), five `SqlTxn` methods with
  defaults, `PipelineError::Rebuild`.

## Revisit if

- Benchmarks show the batch path limits rebuild time at a size users need:
  then option 2, at least for Postgres.
- Users need rebuilds while live ingest keeps running (the option the owner
  declined).
- MySQL users need rebuilds: a non-atomic swap with a recovery step.
