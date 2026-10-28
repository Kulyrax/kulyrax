---
status: in-progress
---

# Adapters set up their store in an explicit prepare step

## Objective

Fix a MySQL atomicity bug found while benchmarking
([2026-10-26-benchmark-baselines](2026-10-26-benchmark-baselines.md)): every SQL
projection ran `CREATE TABLE IF NOT EXISTS conduit_projection_state` inside the
event's transaction. MySQL commits an open transaction implicitly on any DDL, so the
guard read, target write and guard write each autocommitted on their own. A failure
between the target write and the guard write left a target row with no guard.

Approved by the owner on 2026-10-26, before Phase 2.

## Acceptance criteria

- [x] No SQL adapter runs DDL in the event path. The guard table is created by an
  explicit `StorageAdapter::prepare`, in its own transaction; `run`, `ingest`,
  `replay` and `run --event` call it before writing; `dry-run` never does.
- [x] MongoDB (guard index) and Neo4j (guard constraint) move their set-up into
  `prepare` in the same change, with the same two tests: `handle` before `prepare`
  fails and writes nothing; `prepare` does the set-up once.
- [x] `handle` on an unprepared SQL adapter fails and creates nothing.
- [x] An adapter that keeps the default no-op `prepare` works unchanged (tested).
- [x] Core tests prepare through one helper (`tests/common::prepared`), not ~30
  copies.
- [x] Breaking change in `RELEASES/v0.10.0.md` and `CHANGELOG.md`, with a migration
  line.
- [x] A test forces a failure between the target write and the guard write and
  checks that neither row remains: SQLite (always runs), Postgres and MySQL (run when
  `KULYRAX_PG_URL` / `KULYRAX_MYSQL_URL` are set).
- [x] The MySQL test fails on the old code and passes on the new.
- [x] Every adapter audited for implicit commits or DDL inside a transaction.
- [x] fmt, clippy `-D warnings` and `cargo test --workspace` pass; the Postgres and
  MySQL suites pass against the benchmark containers.
- [x] MySQL 10k benchmark rerun after the fix, recorded as its baseline.
- [ ] The MySQL suite passes when its tests run in parallel, as CI's `test (mysql)`
  job runs them. Blocked: see Risks (deadlocks); needs an owner decision.

## Constraints and assumptions

- Constraint: building an adapter has no side effects (`dry-run` must not write).
- Assumption: nothing drops the guard table while an adapter is running. Before
  this change a dropped table was recreated on the next event; now it is recreated
  on the next `prepare` (restart or `kulyrax reload`).

## Decisions

- Schema setup is an explicit step:
  [2026-10-26-explicit-adapter-prepare](../decisions/2026-10-26-explicit-adapter-prepare.md).
  An earlier draft created the table lazily on each adapter's first event; the
  owner rejected that because hidden first-event side effects are harder to reason
  about and test.
- MongoDB and Neo4j move into `prepare` in the same change (owner, 2026-10-26: "a
  contract that's only half-applied is worse than none"). Their set-up is timed as
  the `schema` stage too.
- `Prepared` lives in `kulyrax_core::adapter`, generic over the set-up's error type,
  so all five adapters share it.
- The failure is forced inside the database, not by a wrapper type: a trigger
  (SQLite) or a `CHECK` constraint (Postgres, MySQL) rejects one event id's guard
  row. This tests the real adapter through `StorageAdapter::handle` and adds no
  public test-only API.

## Verification

Audit (read in source, 2026-10-26):

| Adapter | Schema setup before this change | In the event's transaction? | Finding |
|---|---|---|---|
| MySQL | `CREATE TABLE IF NOT EXISTS`, every event | yes | bug: implicit commit; only DDL statement in the adapter |
| Postgres | same, every event, in a savepoint | yes | atomic (transactional DDL), ~2.6 ms/event |
| SQLite | same, every event | yes | atomic (transactional DDL), ~56 µs/event |
| MongoDB | `create_index`, once per adapter, on first event | no | correct; moved into `prepare` anyway (no hidden first-event side effects) |
| Neo4j | `CREATE CONSTRAINT`, once per adapter, on first event | no | correct; moved into `prepare` anyway |
| Elasticsearch, Redis, file stores | none | no transactions | n/a |

MySQL's other statements are all DML (SELECT … FOR UPDATE, INSERT … ON DUPLICATE KEY
UPDATE, UPDATE, DELETE). Redis uses WATCH/MULTI/EXEC.

Bug reproduction (benchmark task record): in `mysql:8`, `START TRANSACTION; INSERT 1;
CREATE TABLE IF NOT EXISTS <existing>; INSERT 2; ROLLBACK;` left both rows.

`dry-run` stays write-free: `crates/core/tests/dry_run.rs` already asserts the
SQLite database file doesn't exist after a dry-run, which `prepare` would create.

Checks run on 2026-10-28, at `b22fb1b`:

| Criterion | Check | Observed result |
|---|---|---|
| Gates | `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace --no-fail-fast` | clean (after `rustfmt` and one clippy `let_and_return` fix, `b22fb1b`); clean; 109 test binaries, 330 passed, 0 failed |
| Backend suites | `cargo test -p kulyrax-adapter-{postgres,mongodb,neo4j}` against the benchmark containers | 9/9, 8/8, 9/9 passed |
| MySQL suite | `cargo test -p kulyrax-adapter-mysql --test mysql_backend -- --test-threads=1` on a fresh container | 9/9 passed. In parallel: flaky, see Risks |
| Fails on old code | `mysql_failed_guard_write_leaves_neither_row` copied unchanged into a worktree at `cb9a21b` (the old helper has no `prepare`) | **failed**: `left: Some("b")`, the target row survived the failed guard write |
| Passes on new code | same test at `b22fb1b` | passed |
| MySQL 10k baseline | `run.sh mysql 10000 1000` with a release build of `b22fb1b` | 9 events/s (old code: 4); `schema` 1 call (103 ms) instead of 10,000; commit 99.9 ms/event |

## Risks and open questions

- **MySQL deadlocks between concurrent writers (exposed by this fix; open, needs
  an owner decision).** Run in parallel against one database, the MySQL suite
  intermittently fails with `ERROR 1213 (40001): Deadlock found` (2 of 6 runs; 0 of 6
  on `cb9a21b`). Before this change the per-event DDL committed implicitly, so the
  `SELECT … FOR UPDATE` that followed ran in autocommit and locked nothing. Now the
  transaction is real: under `REPEATABLE READ`, `FOR UPDATE` on a guard row that
  doesn't exist yet takes a gap lock, and two writers inserting new entities into
  the same gap deadlock. A single Kulyrax instance dispatches synchronously and
  never hits it; CI's parallel `test (mysql)` job will. The adapter's module doc
  ("no retry loop is needed") is wrong. Options: (1, recommended) retry the
  transaction in the MySQL adapter on 1213/1205, bounded at 3 attempts, with a test;
  (2) `READ COMMITTED`; (3) run the suite single-threaded in CI (hides it).
- `mysql_column_missing_rejected` fails on old and new code against a reused
  container: table names restart at `t_0` each run and that test doesn't clear guard
  rows. CI uses a fresh container. Test-only; not fixed.

- Every caller that builds SQL adapters itself and calls `handle` directly (tests,
  embedding applications) must now call `prepare` first. They fail loudly ("adapter
  not prepared") rather than silently.

## Outcome

Blocked on one item: the fix is done and verified (atomicity test fails on the old
code, passes on the new; all gates green), but the MySQL suite deadlocks
intermittently when run in parallel, which CI does. Not mergeable until the owner
picks a deadlock fix.
