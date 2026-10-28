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

- [x] `StorageAdapter` has `begin_batch` / `commit_batch` / `rollback_batch` with
  no-op defaults; an adapter that implements none of them still works through the
  batched source loop (tested).
- [x] The source loop and `replay` dispatch each batch in one transaction per
  adapter, in the existing per-event order, and commit the source position only
  after every adapter has committed.
- [x] A failing event rolls back every adapter's batch, and the batch is replayed
  one event at a time: the events before it land, it goes to retry/DLQ as today,
  and the result is the same as without batching (tested, SQLite).
- [x] A `commit_batch` failure falls back the same way; nothing is lost or doubled
  (tested).
- [x] Guard lanes are loaded once per batch (`IN (…)` / `= ANY`); inside a batch,
  later events for the same entity see earlier ones, including deletes, tombstones,
  resurrections and facet cascades (tested: batched and one-at-a-time runs of a
  mixed stream leave identical rows and guards, SQLite and Postgres).
- [x] Postgres holds one connection per adapter per batch: one checkout and one
  `BEGIN` per batch, not per event; statements are prepared once per batch (in
  the code; the Postgres test sees one commit and one guard read per batch).
- [x] Batch size is configurable (`--max-batch`); default 1,000 for continuous
  `run` and `ingest`, 5,000 for `run --once` and `replay`.
- [x] The directory source splits a file larger than the batch.
- [x] fmt, clippy `-D warnings`, `cargo test --workspace`, and the Postgres suite
  against a container pass before each push.
- [x] SQLite and Postgres benchmarked at 100k events, throughput and rebuild, with
  per-stage timings.
- [x] Exit target met, or stopped and reported with per-stage numbers: Postgres
  ≥ 5,000 events/s, SQLite ≥ 20,000 events/s, rebuild ≥ 2× live.

## Constraints and assumptions

- Constraint: per-event order kept: events in sequence order, each through its
  adapters in dependency order. No savepoints. No reordering of events; bulk
  writes only for Postgres, from round 2 (owner, 2026-10-28).
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

Code at `8b14872` (batching) and `5581a3a` (batch stage timing). Internal baselines:
this laptop, Postgres in Docker Desktop (see the Phase 1 task record).

| Criterion | Check | Observed result |
|---|---|---|
| Gates | `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace --no-fail-fast` | clean; clean; 111 binaries, 340 passed, 0 failed |
| Postgres suite | `cargo test -p kulyrax-adapter-postgres` against the benchmark container, twice fresh, twice reused | 11/11 each time |
| Batch = one at a time | `batch_transactions::batch_equals_one_event_at_a_time` (18 events, 4 entities: facets, deletes, permanent tombstone, resurrection, stale redelivery, delete before create); `batch_boundaries_do_not_matter` (sizes 1, 2, 3, 5, 7); `pg_batch_matches_one_event_at_a_time` | identical target rows, guard rows and per-event outcomes; batched replay made 1 guard read and 1 commit for the whole batch |
| Failing event | `failing_event_is_isolated_and_the_rest_lands` (an event writing to a missing table, DLQ, retry budget 0) | batch fell back; only that event parked; state identical to batch size 1 |
| Failing commit | `failed_commit_falls_back_without_loss_or_doubling` (two SQLite adapters, the second's first commit fails) | fell back; the first adapter's events skipped by its guards, the second's created; every guard at sequence 1 |
| Rollback | `rollback_leaves_nothing_and_the_adapter_carries_on`, `pg_batch_rollback_leaves_nothing` | no rows, no guards; per-event `handle` works after |
| Default adapters | `default_prepare` (an adapter with none of the batch methods) and `source_transient_failure_retry` through the batched loop | pass |
| Directory split | `source_directory_split` (a 5-event file, batch 2, restart mid-file) | resumes inside the file, then the next file whole |

Throughput, 100k events, files of 1,000 (µs per event; "batch" is begin + commit
per adapter, which holds the guard read and the commit):

| Backend | Batch | events/s | adapter | write | read | commit | batch/adapter other | dispatch other | source read | Target |
|---|---|---|---|---|---|---|---|---|---|---|
| SQLite | 1,000 | 15,883 | 9.3 | 5.2 | 1.0 | 28.9 | 8.5 | 3.6 | 5.1 | fail (≥ 20,000) |
| SQLite | 5,000 | 25,855 | 9.6 | 5.4 | 1.0 | 13.9 | 9.0 | 3.6 | 2.6 | pass |
| Postgres | 1,000 | 970 | 965 | **952** | 8.6 | 28.6 | 20.8 | 10.2 | 6.5 | fail (≥ 5,000) |
| Postgres | 5,000 | 984 | 971 | **958** | 10.9 | 13.2 | 19.5 | 9.8 | 2.7 | fail |

Phase 1 for comparison: SQLite 42 events/s (commit 23,687 µs/event), Postgres 30
(33,441 µs/event).

Rebuild while ingesting (Postgres, `rebuild.sh 100000 485 180`, rebuild batch 5,000,
live batch 1,000 at half the live throughput): rebuild **554 events/s**, live lag
p50 317 ms / p99 1,062 ms (not scored). Rebuild is 0.57× live, against ≥ 2×: fail.
Postgres served about 1,040 events/s in total, its ceiling here.

### Round 2 (owner, 2026-10-28): Postgres bulk writes, rebuild, MySQL

Code at `54d9aa8` (bulk), `59d551d` (rebuild timing fix), `5a93a75` (MySQL).

| Criterion | Check | Observed result |
|---|---|---|
| Gates | fmt, clippy `-D warnings`, `cargo test --workspace` | clean; clean; 111 binaries, 343 passed, 0 failed |
| Postgres suite | `cargo test -p kulyrax-adapter-postgres` against a container | 13/13 |
| Repeated keys in one batch | `pg_bulk_repeated_keys_in_one_batch` (3 keys × 5 rounds of update + facet, a delete and re-create mid-batch, a final delete) | rows and guard lanes identical to one event at a time |
| Bulk failure | `pg_bulk_failure_fails_the_commit_and_per_event_isolates_it` (`CHECK` rejects one row) | `commit_batch` fails, nothing written; per event, only that event fails |

Throughput, 100k events (µs per event):

| Postgres | events/s | adapter | write (record + flush) | read | commit | other | Target ≥ 5,000 |
|---|---|---|---|---|---|---|---|
| Round 1, batch 1,000 | 970 | 965 | 952 | 8.6 | 28.6 | 31 | fail |
| Bulk, batch 1,000 | 5,041 | 6.8 | 110 | 11.2 | 34.4 | 16 | pass (barely) |
| Bulk, batch 5,000 | 10,934 | 8.6 | 40.7 | 11.1 | 16.1 | 17 | pass |

Write at batch 1,000 is ~110 ms per flush; the same 1,000-row statements take
~13 ms (data) + ~17 ms (guards) inside the container (`psql \timing`), so ~80 ms
per flush is spent outside the server, most likely moving the arrays through
Docker Desktop's port forwarding. Not proven.

**Why the rebuild was slower than live (554 vs 970 events/s):**

1. Mostly a measurement bug: `rebuild.sh` stopped the rebuild's clock after
   waiting for it, which happened after the 180 s live run; the rebuild had
   finished first, so its time was stretched to 180 s. Fixed (`59d551d`); the same
   run timed correctly is **796 events/s**.
2. It did use 5,000-event batches (20 batches for 100k).
3. Contention with the live writer: the write round trip rose from 958 to
   1,189 µs per event (+25%) while the live writer ran, with neither the Postgres
   container (19–41% of a core) nor the host (38–65%) saturated, so the shared
   resource is most likely the Docker Desktop network path. Not proven.
4. And by design: before bulk writes, rebuild and live did the same two round
   trips per event, so a larger batch only shrank the commit share (1–3% of the
   time). A rebuild can only outrun live once the per-event cost is gone.

Rebuild while ingesting, bulk path (live at 2,500 events/s, batch 1,000):

| History | Rebuild events/s | vs live (5,041) | Overlap |
|---|---|---|---|
| 100k | 10,527 | 2.09× | rebuild took 9.5 s of the 180 s live run |
| 1,000,000 | 9,942 | **1.97×** | rebuild took 100.6 s, live throughout |

At 1M the rebuild's guard read is 106 ms per 5,000-event batch (56 ms at 100k: a
10× larger guard table, shared with the live writer) and its source read 42 ms
per poll (15 ms: the directory source lists and sorts every file on each poll).

**MySQL root cause:** recorded in
[2026-10-26-sql-schema-out-of-transaction](2026-10-26-sql-schema-out-of-transaction.md):
gap locks from the guard read under `REPEATABLE READ` on the shared guard table;
fixed with `READ COMMITTED`, no retry.

## Risks and open questions

- **Round 1 (resolved by round 2): Postgres was round-trip bound** at 952 of its
  1,031 µs per event (target write and guard write, one round trip each). The owner
  chose bulk writes per batch (see the decision record's amendment).
- **Postgres at the live batch size is only just over target** (5,041 vs 5,000).
  About 80 ms of each ~110 ms flush of 1,000 rows is outside the server (see Round
  2). Unverified; the Linux VM run will show how much of it is Docker Desktop.
- **Rebuild with a 1M-event history is 1.97× live**, just under 2×. Its guard read
  and source read grow with history size (Round 2). The directory source lists
  and sorts every file on each poll; with 1,000 files that is ~40 ms per poll.
- **SQLite at the live batch size is commit bound.** At 1,000 events per batch the
  WAL fsync costs 28.9 µs per event (one ~29 ms `FlushFileBuffers` per batch on
  this laptop), more than half of the 50 µs budget. At 5,000 it passes (25,855
  events/s). Owner, 2026-10-28: 5,000 is acceptable; a time-based flush
  (`max_batch_delay`, default 100 ms) then lets live run at 5,000 without hurting
  lag. Not started.

## Outcome

**Round 1** stopped short of the exit target; **round 2** (2026-10-28) stops after
the owner's three items, as instructed:

- Postgres bulk writes: 970 → 5,041 events/s at batch 1,000 and 10,934 at 5,000.
  Target ≥ 5,000: met, at 1,000 by 0.8%.
- Rebuild: the 554 events/s was a timing bug plus contention; with bulk writes,
  10,527 events/s (2.09× live) over 100k and 9,942 (1.97×) over 1M, against ≥ 2×.
- MySQL deadlocks: our gap-lock pattern; fixed with `READ COMMITTED`, no retry.
- SQLite: unchanged (15,883 at 1,000; 25,855 at 5,000).

Not started: the time-based flush (owner's item 3), the lag reruns at 1,000/s and
5,000/s, other backends, Phase 3/4.
