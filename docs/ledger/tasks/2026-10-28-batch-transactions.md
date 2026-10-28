---
status: in-progress
---

# Phase 2: one transaction per adapter per batch (SQLite, Postgres)

## Objective

Share the commit and the round trips across a batch of events instead of paying
them per event, on SQLite and Postgres first. Design:
[2026-10-28-batch-transactions](../decisions/2026-10-28-batch-transactions.md).
Baselines: [2026-10-26-benchmark-baselines](2026-10-26-benchmark-baselines.md).
Approved by the owner on 2026-10-28.

## Acceptance criteria

- [ ] `StorageAdapter` has `begin_batch` / `commit_batch` / `rollback_batch` with
  no-op defaults; an adapter that implements none of them still works through the
  batched source loop (tested).
- [ ] The source loop and `replay` dispatch each batch in one transaction per
  adapter, in the existing per-event order, and commit the source position only
  after every adapter has committed.
- [ ] A failing event rolls back every adapter's batch, and the batch is replayed
  one event at a time: the events before it land, it goes to retry/DLQ as today,
  and the result is the same as without batching (tested, SQLite).
- [ ] A `commit_batch` failure falls back the same way; nothing is lost or doubled
  (tested).
- [ ] Guard lanes are loaded once per batch (`IN (…)` / `= ANY`); inside a batch,
  later events for the same entity see earlier ones, including deletes, tombstones,
  resurrections and facet cascades (tested: the existing facet, delete and
  out-of-order suites pass in batch mode).
- [ ] Postgres holds one connection per adapter per batch: one checkout and one
  `BEGIN` per batch, not per event; statements are prepared once per batch.
- [ ] Batch size is configurable (`--max-batch`); default 1,000 for continuous
  `run` and `ingest`, 5,000 for `run --once` and `replay`.
- [ ] The directory source splits a file larger than the batch.
- [ ] fmt, clippy `-D warnings`, `cargo test --workspace`, and the Postgres suite
  against a container pass before each push.
- [ ] SQLite and Postgres benchmarked at 100k events, throughput and rebuild, with
  per-stage timings.
- [ ] Exit target met, or stopped and reported with per-stage numbers: Postgres
  ≥ 5,000 events/s, SQLite ≥ 20,000 events/s, rebuild ≥ 2× live.

## Constraints and assumptions

- Constraint: per-event order kept: events in sequence order, each through its
  adapters in dependency order. No savepoints. No reordering or bulk writes.
- Constraint: SQLite and Postgres only. No other backend changes until both are
  benchmarked. No Phase 3 or 4 work.
- Constraint: Zero Panic Rule in `kulyrax-core`; no behaviour change outside the
  batch path (a single `handle` without an open batch behaves as before).
- Assumption: one Kulyrax instance per adapter's store (the single-writer case the
  benchmarks measure). Multi-writer stays correct but may fall back more often.
- Internal baselines only (this laptop); published numbers come from a Linux VM.

## Decisions

- See the decision record. Statement reuse on the held Postgres connection is part
  of "one held connection": the `postgres` crate prepares a statement on every
  `execute(&str)`, a second round trip per write.

## Verification

Pending.

## Risks and open questions

- On this machine every Postgres round trip crosses Docker Desktop's VM network
  (~1.4 ms per statement in the baselines). With the target write and guard write
  still one statement each per event, Postgres may stay round-trip bound short of
  5,000 events/s here; if so, the numbers are reported per stage and bulk writes are
  the next decision, not started.
- MySQL deadlock decision (from the `prepare()` change) is still open; MySQL is out
  of scope here.

## Outcome

In progress.
