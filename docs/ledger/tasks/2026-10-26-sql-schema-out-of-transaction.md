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

- [ ] No SQL adapter runs DDL in the event path. The guard table is created by an
  explicit `StorageAdapter::prepare`, in its own transaction; `run`, `ingest`,
  `replay` and `run --event` call it before writing; `dry-run` never does.
- [ ] MongoDB (guard index) and Neo4j (guard constraint) move their set-up into
  `prepare` in the same change, with the same two tests: `handle` before `prepare`
  fails and writes nothing; `prepare` does the set-up once.
- [ ] `handle` on an unprepared SQL adapter fails and creates nothing.
- [ ] An adapter that keeps the default no-op `prepare` works unchanged (tested).
- [ ] Core tests prepare through one helper (`tests/common::prepared`), not ~30
  copies.
- [ ] Breaking change in `RELEASES/v0.10.0.md` and `CHANGELOG.md`, with a migration
  line.
- [ ] A test forces a failure between the target write and the guard write and
  checks that neither row remains: SQLite (always runs), Postgres and MySQL (run when
  `KULYRAX_PG_URL` / `KULYRAX_MYSQL_URL` are set).
- [ ] The MySQL test fails on the old code and passes on the new.
- [x] Every adapter audited for implicit commits or DDL inside a transaction.
- [ ] fmt, clippy `-D warnings` and `cargo test --workspace` pass; the Postgres and
  MySQL suites pass against the benchmark containers.
- [ ] MySQL 10k benchmark rerun after the fix, recorded as its baseline.

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

## Risks and open questions

- Every caller that builds SQL adapters itself and calls `handle` directly (tests,
  embedding applications) must now call `prepare` first. They fail loudly ("adapter
  not prepared") rather than silently.

## Outcome

In progress: code, tests and docs written; build and tests wait for the benchmark
suite to finish so compiling doesn't distort its timings.
