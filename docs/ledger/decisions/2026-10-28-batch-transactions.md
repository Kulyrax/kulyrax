---
status: accepted
---

# One transaction per adapter per batch

Task: [2026-10-28-batch-transactions](../tasks/2026-10-28-batch-transactions.md)

## Context

Every backend commits once per event and makes its round trips once per event:
guard read, target write, guard write, `BEGIN` and `COMMIT`, plus (Postgres) a pool
checkout. On this machine that is 24 ms of commit per event on SQLite and Postgres
and 100 ms on MySQL, against a target of 200 µs per event
([2026-10-26-benchmark-baselines](../tasks/2026-10-26-benchmark-baselines.md),
Summary). Tuning a single event's path cannot close a gap of 100× or more; the
commit and the round trips have to be shared by many events.

The engine already has a batch: the source loop polls up to `max_batch` events,
dispatches them one at a time, and commits the source position only once every
event in the batch is resolved. Replay (`kulyrax replay`) buffers the whole stream
and dispatches one event at a time with no checkpoint.

## Options considered

1. **One transaction per adapter per batch, optimistic.** Each adapter opens one
   transaction for the batch; events are dispatched exactly as today (in order, each
   event through its adapters in dependency order) into those open transactions;
   at the end every adapter commits, then the source position is committed. On any
   failure, every open transaction is rolled back and the batch is dispatched again
   one event at a time, the current path, which isolates the failing event for the
   existing retry and dead-letter handling.
2. **Savepoint per event inside the batch transaction**, rolling back only the
   failing event. Rejected by the owner: a savepoint is an extra round trip per
   event on Postgres, which is the cost being removed, and not every store has one.
3. **Reorder or group the batch by entity or table** to write in bulk (multi-row
   `INSERT`, `_bulk`, pipelining). Rejected for this phase: it changes the order in
   which `decide()` sees events and the per-event order across dependent adapters.
   Bulk writes come later (Elasticsearch `_bulk` is Phase 4).

## Evidence

- Stage timings at 100k events: SQLite spends 23,687 of 24,046 µs per event in the
  commit; Postgres 23,955 of 33,441 µs, plus 2,197 µs of pool checkout and `BEGIN`
  and 1,694 µs of guard read.
- Owner's Phase 2 design (2026-10-26 plan; approved 2026-10-28): per-event order
  A→B kept, one transaction per adapter per batch, no savepoints, optimistic replay
  on failure, checkpoint after all commits, one held connection per adapter per
  batch, guard reads per batch, default batch 1,000, replay/rebuild 5,000.

## Decision

Option 1.

**Adapter contract.** `StorageAdapter` gains three methods, all with defaults:

- `begin_batch(&self, events: &[&Event]) -> Result<(), AdapterError>`: open the
  batch transaction and, optionally, load what `handle` will need for these events
  (the guard lanes). Default: nothing; the adapter keeps committing per event.
- `commit_batch(&self) -> Result<(), AdapterError>`: commit everything written
  since `begin_batch`. Default: nothing.
- `rollback_batch(&self)`: discard it. Default: nothing.

While a batch is open, `handle` writes into the batch transaction and does not
commit. Without an open batch, `handle` behaves exactly as before. Third-party
adapters that implement none of this keep their per-event commits and stay correct:
a batch replay re-delivers their events, which their guards skip.

**Orchestration** (`kulyrax_core::batch`, used by the source loop and by replay):

1. Route every event; `begin_batch` on each routed adapter with its events. A
   `begin_batch` error rolls back the ones already begun and falls back (step 4).
2. Dispatch each event as today (`dispatch`), in order. The first event whose
   report is `Failed` stops the batch: roll back every adapter, fall back.
3. `commit_batch` each adapter in dependency order. If one fails, roll back the
   rest and fall back. Adapters that already committed stay committed; their guards
   turn the fallback's re-delivery into skips.
4. Fallback: dispatch the batch one event at a time with no batch open. The source
   loop's retry budget and dead-letter queue then work exactly as before.

The source position is committed only after step 3 (or the fallback) resolves
every event, as before.

**Guard reads.** The SQL write path (`sql::exec`) keeps a per-batch guard cache.
`begin_batch` loads every lane the batch will touch in one query per table
(`entity_key IN (…)` / `= ANY($1)`), with `FOR UPDATE` on Postgres. Inside the batch,
`decide()` reads lanes from the cache, and every guard write updates it, so later
events for the same entity see earlier ones. A facet cascade drops that entity's
named-facet lanes from the cache so they are re-read inside the transaction.
`SqlTxn` gains `read_guard_lanes`, with a default that reads lane by lane, so other
`SqlTxn` implementations keep working.

**Connections.** SQLite already holds one connection; a batch is one `BEGIN … COMMIT`
on it. Postgres checks one connection out of the pool in `begin_batch` and holds it
until `commit_batch` / `rollback_batch`: one checkout and one `BEGIN` per batch, not
per event. Statements are prepared once per batch on that connection and reused, so
each write is one round trip instead of two (the `postgres` crate prepares an
unnamed statement on every `execute(&str)`). SQLite caches prepared statements on
its connection.

**Batch size.** `--max-batch` stays configurable. Default 1,000 for continuous
`run` and `ingest`; 5,000 for `run --once` (catch-up and rebuild) and `replay`. The
directory source splits a file larger than the batch instead of taking it whole.

**Scope.** SQLite and Postgres only, benchmarked at 100k before any other backend.
MySQL, MongoDB, Neo4j, Redis, Elasticsearch and the file stores keep the default
(per-event commit) until then.

## Trade-offs accepted

- One bad event costs a rollback of the whole batch and a per-event pass over it:
  slow, but only on failure, and it isolates the event exactly as today.
- Guard-row locks (Postgres `FOR UPDATE`) are held for the whole batch, not one
  event. A second writer on the same entities waits longer and, with overlapping
  keys in different orders, can deadlock; Postgres detects it, the batch fails,
  rolls back and falls back. Single-instance Kulyrax has one writer per adapter.
- If one adapter's `commit_batch` fails after another committed, the batch is
  partly durable until the fallback re-delivers it. The source position has not
  moved, so a crash at that point re-delivers the batch on restart; guards make it
  idempotent. The fallback's report shows those events as skips for the adapter that
  had already committed.
- Reports for a batch are built before its commit; they are only returned once the
  commit succeeds, and replaced by the fallback's reports otherwise.
- The directory source's position gains an in-file offset for split files, so the
  checkpoint format changes for files larger than the batch.

## Revisit if

- Postgres or SQLite falls short of the Phase 2 exit target (Postgres ≥ 5,000
  events/s, SQLite ≥ 20,000 events/s, rebuild ≥ 2× live) with these changes: the
  remaining per-event round trips (target write, guard write) are then the next
  candidate, via bulk writes.
- A backend cannot keep a transaction open across many `handle` calls.

## Amendment (2026-10-28): Postgres writes each batch in bulk

The first round left Postgres round-trip bound (952 of 1,031 µs per event: one
round trip each for the target write and the guard write). The owner chose bulk
writes per batch over merging the two statements into one (a ~2× half-fix).

- `decide()` stays per event, in memory, against the batch's guard lanes
  (`project_in`, unchanged). The Postgres adapter records the writes it asks for
  instead of sending them (`kulyrax-adapter-postgres` `bulk.rs`).
- Each target row and guard lane collapses to its net effect for the batch, since
  one `INSERT … ON CONFLICT DO UPDATE` cannot touch the same row twice: writes merge
  (union of columns, latest value), a delete discards what came before it and
  writes after it build a new row, a guard lane keeps its final state.
- `commit_batch` sends, per table, one `DELETE … USING unnest(…)` and one
  `INSERT … SELECT FROM unnest(…)` per row shape (`ON CONFLICT … DO UPDATE` for
  upserts), one guard upsert and one guard update, then `COMMIT`.
- A facet cascade updates lanes the batch never loaded, so the adapter flushes
  what it has recorded and runs the cascade directly: per-entity order holds.
- Values are bound per event, so a value that doesn't fit its column still fails
  its own `handle`. A row the database rejects fails the bulk write and
  `commit_batch`; the batch rolls back and falls back to one event at a time.

This reverses "no bulk writes" in Options considered for Postgres only: the
per-event order of `decide()` is unchanged; only the sending is batched. Other
backends are unchanged.
