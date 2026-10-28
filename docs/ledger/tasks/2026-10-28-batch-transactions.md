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

## Risks and open questions

- **Confirmed: Postgres is round-trip bound.** 952 µs of its 1,031 µs per event is
  the write stage: the target write and the guard write, one statement and one
  round trip each (~476 µs per round trip through Docker Desktop's VM network,
  down from ~1.4 ms per statement in Phase 1 now that each is one round trip, not
  two). Commit is 1–3% of the time, so a larger batch barely helps (970 → 984).
  Closing the gap needs fewer round trips per event; the candidates, none started,
  need an owner decision:
  - combine the target write and guard write into one statement (a data-modifying
    CTE): ~2× at best here, still short of 5,000;
  - pipeline the batch's statements (`tokio-postgres` pipelining) or write the
    batch as multi-row statements per table: the bulk path the decision record
    deferred, and the only one that removes per-event round trips;
  - measure on the Linux VM first: a local round trip there is likely ~10× shorter
    than through Docker Desktop, which changes how far the current code gets.
    Not measured.
- **SQLite at the live batch size is commit bound.** At 1,000 events per batch the
  WAL fsync costs 28.9 µs per event (one ~29 ms `FlushFileBuffers` per batch on
  this laptop), more than half of the 50 µs budget; everything else is ~34 µs.
  At 5,000 it passes (25,855 events/s).
- Rebuild runs into the same Postgres ceiling as live ingest (they share it), so
  "≥ 2× live" needs the round-trip fix above, not a larger rebuild batch.
- MySQL deadlock decision (from the `prepare()` change) is still open; MySQL is out
  of scope here.

## Outcome

**Stopped short of the exit target** (2026-10-28), per the owner's rule; no Phase 3
or 4 work started. Batching is implemented, tested and pushed for SQLite and
Postgres: SQLite 42 → 15,883 events/s at batch 1,000 (378×) and 25,855 at 5,000;
Postgres 30 → 970 events/s (32×). Exit target: SQLite passes at 5,000 and misses at
1,000 (15,883 vs 20,000); Postgres misses (970 vs 5,000); rebuild misses (0.57× live
vs 2×). Next step is the owner's call between the options under Risks.
