---
status: completed
---

# Phase 1: benchmark every backend and find where the time goes

## Objective

Measure throughput, per-stage time and projection lag for every backend before
designing batching (Phase 2) or crash-safe file adapters (Phase 3). In particular, find
where Postgres spends 5.7 ms per event with `synchronous_commit` off (from
[2026-10-26-sqlite-connection-reuse](2026-10-26-sqlite-connection-reuse.md)).

## Acceptance criteria

- [x] `examples/benchmark` covers SQLite, Postgres, MySQL, Redis, MongoDB,
  Elasticsearch, Neo4j and the file-backed document, key-value and graph adapters.
- [x] The run report has per-stage timing: source read (read + parse), dispatch,
  `decide()` reads, adapter write, commit, checkpoint.
- [x] Input is split across many files (default 1,000 events per file).
- [x] Throughput baseline for every backend: 100,000 events, except MySQL and
  Elasticsearch at 10,000 (their paths were known to be broken or unexplained), and
  Neo4j at 10,000 (owner, 2026-10-28; see Decisions).
- [x] p50/p99 projection lag recorded for every backend. Not scored against the
  lag target: at half throughput it is not meaningful (see Decisions).
- [x] One rebuild-while-ingesting run recorded (Postgres).
- [x] Where Postgres's 5.7 ms/event goes, from 100k-event stage timings.
- [x] File stores measured with Defender on and with `work/` excluded, side by side.
- [x] Directory-size isolation: file-document at 10k vs 100k events, both excluded.
- [x] Summary table of all backends with per-stage breakdown and pass/fail against
  both targets.
- [x] fmt, clippy `-D warnings` and `cargo test --workspace` pass.

## Targets

Set by the owner (2026-10-26); the summary reports pass/fail for each backend.

- Throughput: at least 5,000 events/s per adapter, about 200 µs per event.
- Lag: p99 under 1 s.

## Constraints and assumptions

- **Every number here is an internal baseline.** The machine is a domain-joined
  Windows laptop with Defender on, Hyper-V crashes, and databases in Docker Desktop's
  VM, whose disk is a file on NTFS. Published benchmarks will run on a clean Linux VM
  with NVMe, with hardware and settings recorded.

- Constraint: no engine behaviour change in this phase. Stage timing only adds a
  `timings` field to the run report and a table to its text output.
- Constraint: no new backends or features until benchmarks are published.
- Assumption: wall-clock stage timing is cheap enough not to distort results. It adds
  about 8 `Instant::now()` calls per event, against costs measured in milliseconds.

## Decisions

- Stage timing uses a thread-local accumulator (`kulyrax_core::execution::stages`).
  The stages sit behind `StorageAdapter::handle(&self, &Event)` and the backend traits,
  which have nowhere to pass a timer, and dispatch is synchronous on the run-loop
  thread. Changing the adapter trait to carry timings was rejected: it breaks every
  third-party backend just to measure.
- Lag is measured from outside the engine: `benchtool` publishes files at a fixed rate
  (write, then rename) and polls the source checkpoint every 1 ms. A file's lag runs
  from its rename to the first checkpoint at or past it, which is the moment the
  projection is durable and acknowledged. No engine change needed.
- Lag runs last 180 s at half the backend's measured throughput, not 100,000 events.
  At 2–200 events/s, 100,000 paced events would take between 8 minutes and 14 hours
  per backend. Approved by the owner (2026-10-26).
- **Lag results at half throughput are not meaningful** (owner, 2026-10-28): half of
  ~30 events/s is ~15/s, near idle, so a "pass" there says nothing. After Phase 2,
  lag is rerun at fixed absolute rates (1,000/s and 5,000/s) for every backend; that
  is the lag result that counts. Here they are recorded but not scored.
- **Rebuild must run at least 2× live throughput**, or it has no catch-up mode
  (owner, 2026-10-28). The Postgres rebuild ran at 30 events/s, the same as live: a
  failure.
- Neo4j, Elasticsearch and MySQL run at 10,000 events. Neo4j at 11 events/s would
  need 2.5 h for 100k to confirm the pattern every other backend already shows; its
  summary row is marked "10k, same pattern". (The 100k Neo4j run was voided: the
  laptop slept on battery from 2026-10-27 02:28 to 2026-10-28 18:13 mid-run.)
- Owner, 2026-10-28, from the summary: sharding file-store directories is dropped
  (no directory-size effect). MySQL and Elasticsearch server settings stay as they
  are (`sync_binlog=1`, translog `request`); batching (Phase 2) and the
  Elasticsearch `_bulk` API (Phase 4) are the fixes. Phase 2 approved.
- `lag.sh` uses `--poll-interval-ms 100`. The CLI default is 1,000 ms, which would add
  up to a second of idle wait to every lag figure.

## Verification

Hardware: Intel Core i5-10210U (4 cores / 8 threads), 32 GB RAM, LITEON NVMe SSD,
Windows 11 Pro 10.0.26200, Docker Desktop. Containers: postgres:16, mysql:8, redis:7,
mongo:7 (single-node replica set), elasticsearch 8.15.0, neo4j:5.

| Criterion | Check | Observed result |
|---|---|---|
| Gates | `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace --no-fail-fast` | clean; clean; 107 test binaries, 314 passed, 0 failed |
| Stage counts are right | `cargo test -p kulyrax-core --test source_stage_timings` | passed: 5 events, 2 batches → 3 source reads, 2 dispatches, 2 checkpoints, 5 of each per-event stage |
| Every backend runs and verifies | `bash run.sh <backend> 300 100` (smoke test, not a baseline); file backends at 2,000 | all 10 verified every entity |
| Lag and rebuild scripts work | `lag.sh file-document 50 10`; `lag.sh postgres 10 15`; `rebuild.sh 600 10 15` | all verified; Postgres p50 96 ms / p99 571 ms alone, p50 113 ms / p99 675 ms during the rebuild |

Smoke-test stage times (300 events; 2,000 for the file stores), µs per event. Indicative
only: small runs include first-write costs. The 100k baselines replace them.

| Backend | events/s | schema | read | write | commit | other |
|---|---|---|---|---|---|---|
| SQLite | 42 | 56 | 39 | 76 | 23,270 | 190 |
| Postgres | 32 | 2,555 | 1,438 | 2,536 | 22,788 | 2,008 |
| MySQL | 4 | 89,136 | 2,011 | 177,267 | 1,759 | 1,566 |
| Redis | 469 | – | 734 | 392 | – | 768 |
| MongoDB | 23 | – | 2,120 | 4,863 | 29,370 | 3,220 |
| Elasticsearch | 7 | – | 2,971 | 132,619 | – | 19 |
| Neo4j | 19 | – | 7,492 | 11,076 | 28,334 | 4,810 |
| File document | 275 | – | 75 | 3,476 | – | 26 |
| File key-value | 250 | – | 89 | 3,810 | – | 40 |
| File graph | 186 | – | 92 | 5,139 | – | 36 |

Observed (smoke runs, to be confirmed at 100k):
- Postgres: with commit excluded, `schema` + `read` + `write` ≈ 6.5 ms per event,
  close to the 5.7 ms gap. `schema` is a `CREATE TABLE IF NOT EXISTS` round trip on
  every event.
- SQLite, Postgres, MongoDB and Neo4j each spend 75–99% of every event in the commit.
- The file stores spend 3.5–5.1 ms per write without any fsync.

Recorded MySQL atomicity check (`docker compose exec mysql mysql bench`):
`START TRANSACTION; INSERT 1; CREATE TABLE IF NOT EXISTS <existing>; INSERT 2; ROLLBACK;`
left both rows in the table.

## Summary (2026-10-28)

Internal baselines only (see Constraints). Throughput binary: `cb9a21b` (pinned in
`target/bench/`), except "MySQL, fixed": a release build of `b22fb1b`, the
`prepare()` fix. Stage columns are µs per event; "other" is adapter time outside
schema/read/write/commit (mapping, pool checkout, `BEGIN`). Every result verified
every entity.

**Gates and the MySQL fix.** At `b22fb1b`: fmt clean, clippy `-D warnings` clean,
`cargo test --workspace` 109 binaries / 330 passed / 0 failed; Postgres, MongoDB and
Neo4j suites pass against containers. The MySQL atomicity test **fails on the old
code** (`cb9a21b`: the target row survives the failed guard write) and **passes on
the new**. Details and one open blocker (MySQL deadlocks when its tests run in
parallel) in [2026-10-26-sql-schema-out-of-transaction](2026-10-26-sql-schema-out-of-transaction.md).

### Throughput (target ≥ 5,000 events/s ≈ 200 µs/event)

| Backend | Events | events/s | schema | read | write | commit | other | Target |
|---|---|---|---|---|---|---|---|---|
| SQLite (Defender excluded) | 100k | 42 | 75 | 71 | 148 | **23,687** | 65 | fail |
| Postgres | 100k | 30 | 2,758 ¹ | 1,694 | 2,836 | **23,955** | 2,197 | fail |
| MySQL, old code | 10k | 4 | 77,800 | 3,033 | **181,063** | 2,852 | 2,643 | fail |
| MySQL, fixed | 10k | 9 | ~10 ² | 2,912 | 5,823 | **99,899** ³ | 2,818 | fail |
| Redis | 100k | 433 | – | 897 | 481 | – | 911 | fail |
| MongoDB | 100k | 26 | – | 2,216 | 4,179 | **31,245** | 77 | fail |
| Elasticsearch | 10k | 10 | – | 3,204 | **95,598** ⁴ | – | 36 | fail |
| Neo4j (10k, same pattern) | 10k | 22 | – | 5,755 | 7,095 | **30,009** | 2,682 | fail |

¹ Old code ran guard-table DDL on every event; `prepare()` removes it.
² One call, 103 ms in total.
³ InnoDB redo-log fsync plus binlog fsync (`sync_binlog=1`) on every transaction,
on a VM disk. Server settings stay as they are; batching is the fix.
⁴ The translog fsync on every request (below). The adapter moves to the `_bulk` API
in Phase 4; durability stays `request`.

### File stores, Defender on vs `work/` excluded (100k events)

| Backend | Defender on: events/s (write µs) | Excluded: events/s (write µs) | Target |
|---|---|---|---|
| File document | 159 (6,077) | 393 (2,453) | fail |
| File key-value | 218 (4,406) | 430 (2,238) | fail |
| File graph | 143 (6,830) | 360 (2,681) | fail |

The suite's file key-value run (244 events/s) had the exclusion added partway through;
the rerun above replaces it. File document's "on" figure is the suite run.

Defender costs 2.0–2.5× on file creation, but excluded, every file store still
spends 2.2–2.7 ms per write with no fsync: more than 10× over the target either way.
The fix is Phase 3 (per-batch writes and fsyncs), not per-file tuning before Phase 2.

**Directory size: no effect.** File document, both excluded: 402 events/s (write
2,391 µs) at 10k vs 393 events/s (2,453 µs) at 100k, within noise. Sharding the
store directories is dropped from the plan (owner, 2026-10-28).

### Lag (target p99 < 1 s): recorded, not scored

At half of each backend's throughput, which for most is near idle; **not
meaningful**. After Phase 2, lag is rerun at 1,000/s and 5,000/s for every backend.

| Backend | Rate | p50 | p99 |
|---|---|---|---|
| File document | 79/s | 243 ms | 2,573 ms |
| File key-value | 122/s | 84 ms | 228 ms |
| File graph | 180/s | 129 ms | 546 ms |
| Redis | 216/s | 102 ms | 484 ms |
| SQLite | 21/s | 102 ms | 727 ms |
| Postgres | 15/s | 92 ms | 710 ms |
| MySQL, old code | 2/s | 335 ms | 891 ms |
| MongoDB | 13/s | 151 ms | 1,122 ms |
| Elasticsearch | 5/s | 147 ms | 1,923 ms |
| Neo4j | 11/s | 103 ms | 680 ms |

### Rebuild while ingesting (Postgres): fail

100,000 events rebuilt at 30 events/s, the same as live; the target is at least 2×
live. Live lag during the rebuild at 15 events/s: p50 199 ms, p99 962 ms.

### Elasticsearch: `refresh` and translog durability

No write sends `refresh`; the index default `refresh_interval` (1 s) applies. 300
sequential `PUT ?op_type=create` per setting, two runs each, timed by curl
(`time_total`), against the benchmark container:

| `index.translog.durability` | mean | p50 | p99 |
|---|---|---|---|
| `request` (default) | 114 / 86 ms | 81 / 72 ms | 712 / 583 ms |
| `async` (fsync every 5 s) | 8.4 / 7.5 ms | 7.2 / 5.8 ms | 13.5 / 12.9 ms |

About 90% of a write is the per-request translog fsync, matching the 96 ms/write in
the benchmark. The adapter default is unchanged.

### Conclusion

**The bottleneck is per-event commit plus per-event round trips, on every
backend.** Each event pays its own durable commit (an fsync in SQLite, Postgres,
MySQL, MongoDB, Neo4j and Elasticsearch) and its own network round trips for the
guard read, the write and the transaction itself. No backend reaches a tenth of the
target; most miss it by more than 100×. Tuning won't close that gap; only the batch
contract (Phase 2) will.

## Phase 2 inputs

Owner's reading of these results (2026-10-28), to be carried into the Phase 2 scope:

- **The bottleneck is per-event commit plus per-event round trips, on every
  backend.** Tuning won't close a gap of 100× or more against 5,000 events/s; only
  the batch contract will.
- The replay/rebuild path is in scope, with larger batches (e.g. 5,000), not only
  live ingest: rebuild must reach at least 2× live.
- Postgres: hold one connection per adapter for the batch or run instead of a pool
  checkout and `BEGIN` per event (~2.2 ms of "other" per event).
- Redis: pipeline reads and writes per batch (~1.8 ms of round trips per event, with
  no commit at all).
- Guard reads: load every guard for a batch in one query (`WHERE key IN (…)`,
  `$in`) instead of one per event (1.7 ms Postgres, 2.2 ms MongoDB).

## Risks and open questions

- **MySQL adapter bug (correctness, not just speed).** `MySqlTxn::ensure_guard_table`
  runs `CREATE TABLE IF NOT EXISTS` inside the transaction. In MySQL, DDL implicitly
  commits, even when the table already exists, and later statements autocommit. So the
  guard read (`FOR UPDATE`), the target write and the guard write are not atomic, which
  breaks the `SqlTxn` atomicity contract. A crash between the target write and the
  guard write leaves a row with no guard. **Fixed** by the explicit `prepare()` step
  ([2026-10-26-sql-schema-out-of-transaction](2026-10-26-sql-schema-out-of-transaction.md));
  "MySQL, old code" measures the broken path, "MySQL, fixed" the new one. The fix
  exposed MySQL deadlocks between concurrent writers: open, recorded in that task.
- Elasticsearch: **explained**, the translog fsync on every request (Summary).
  Phase 4 moves the adapter to `_bulk`.
- File stores: **partly explained.** Defender accounts for 2.0–2.5×; with `work/`
  excluded, writes still take 2.2–2.7 ms each with no fsync. Phase 3's fsyncs will
  add to this unless they are per batch.
- Every number comes from Docker Desktop on Windows, where container storage sits in
  a VM. Flush costs on Linux on bare metal may differ a lot.
- The mapping loader requires `sql/` and `document/` folders even when a run uses
  neither (the benchmark ships `.gitkeep` files). Usability issue, out of scope.

## Outcome

Completed 2026-10-28. Every backend is measured with per-stage timings; every
backend fails the throughput target and the Postgres rebuild fails the 2× rule. The
cause is the same everywhere (Conclusion). The owner approved Phase 2 from this
summary.
