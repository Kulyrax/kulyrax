---
status: in-progress
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
- [ ] 100,000-event throughput baseline recorded for every backend.
- [ ] p50/p99 projection lag recorded for every backend.
- [ ] One rebuild-while-ingesting run recorded (Postgres).
- [ ] Where Postgres's 5.7 ms/event goes, from 100k-event stage timings.
- [x] fmt, clippy `-D warnings` and `cargo test --workspace` pass.

## Constraints and assumptions

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
  per backend. Throughput and rebuild runs use 100,000 events. **Open:** the requester
  asked for 100k minimum; this deviation needs their acceptance.
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

## Risks and open questions

- **MySQL adapter bug (correctness, not just speed).** `MySqlTxn::ensure_guard_table`
  runs `CREATE TABLE IF NOT EXISTS` inside the transaction. In MySQL, DDL implicitly
  commits, even when the table already exists, and later statements autocommit. So the
  guard read (`FOR UPDATE`), the target write and the guard write are not atomic, which
  breaks the `SqlTxn` atomicity contract. A crash between the target write and the
  guard write leaves a row with no guard. Needs its own fix and approval; MySQL
  benchmark numbers measure this broken path until then.
- Elasticsearch: 132 ms per `PUT` with no `refresh` parameter. Cause unknown.
- File stores: 3.5–5 ms per new file on Windows. Inferred, not checked: antivirus
  scanning of newly created files. Phase 3's fsyncs will add to this.
- Every number comes from Docker Desktop on Windows, where container storage sits in
  a VM. Flush costs on Linux on bare metal may differ a lot.
- The mapping loader requires `sql/` and `document/` folders even when a run uses
  neither (the benchmark ships `.gitkeep` files). Usability issue, out of scope.

## Outcome

In progress: tooling done and verified; the 100k baseline suite has not finished yet.
