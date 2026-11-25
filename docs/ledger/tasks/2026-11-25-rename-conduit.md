---
status: in-progress
---

# Rename conduit → kulyrax (1.0 plan, step 2)

GHSA-6chh-gx4c-448g is published and 0.12.1 and 0.12.2 are released. The branch
is rebased onto main (runbook B1). The test-isolation fix the owner asked for
(B2) is in, and the branch goes to a PR.

## Objective

Every persisted, user-visible and code name `conduit` becomes `kulyrax`. Existing
stores migrate in place at start-up, once, idempotently and with every guard
lane kept. A migration killed halfway completes on the next start. Design:
[2026-11-25-rename-conduit](../decisions/2026-11-25-rename-conduit.md)
(accepted with the owner's three changes).

## Acceptance criteria

- [x] Decision record approved by the owner (2026-11-25, with changes 1–3).
- [x] SQLite, Postgres, MySQL, MongoDB, Neo4j and the file stores migrate as the
  decision says. When both names exist, start-up is refused and both are named.
- [x] Tombstones: a file `.conduit` (file stores), a non-insertable view (SQL), a
  collection whose validator rejects everything (MongoDB). Neo4j documented only.
- [x] A real 0.12.1 binary fails loudly against a migrated store, per backend
  with a tombstone.
- [x] Layout marker version 2 under `.kulyrax/layout/<kind>`.
- [x] Per backend: a 0.12.1 store opens with every lane intact, an older
  redelivery is `Skipped(StaleSequence)`, and a second start is a no-op.
- [x] Interrupted migration recovers. The kill tests use crash points:
  - file stores at each move, park and tombstone;
  - SQLite and Postgres before commit;
  - MySQL and MongoDB around both renames;
  - Neo4j between relabel batches;
  - a 0.12.1 batch log left behind is replayed.
- [x] `.kulyrax/` shared safely by construction:
  - every owner's entries are listed in `kulyrax_core::kulyrax_dir`;
  - `control` and `state` are reserved, and a store refuses to write there;
  - no "is `.kulyrax/` empty" check remains;
  - a test runs with store root = config folder = `state_dir` parent: reload,
    a store and a checkpoint together, then a restart.
- [x] Code, docs, comments and test names renamed. `grep -ri conduit` hits only
  `legacy.rs` files, `*rename*` tests, `RELEASES/`, and `docs/ledger/`. CI
  enforces it (job `old name only in the migration`).
- [x] Release notes: upgrade section at the top of `RELEASES/v1.0.0-rc.1.md`,
  with the reverse rename per backend.
- [x] `decide()`, `dispatch.rs` and the dependency graph unchanged; no panic in
  core library code; four gates green; every Docker suite run.
- [x] Rebased onto main at 0.12.2 (runbook B1). 0.12.2 persists what 0.12.1
  does, so the upgrade path stays "from 0.12.1"; the rc.1 notes say it covers
  0.12.2.
- [x] Test isolation (owner, runbook B2). No test reads or writes the shared
  `kulyrax_ci` guard objects:
  - a schema (Postgres) or database (MySQL, MongoDB) per test binary, checked
    empty at start;
  - the ingest and outbox tests too;
  - the CLI upgrade tests drop what they create;
  - Neo4j: a test owns the whole database (process and file lock, wipe before
    and after, a leftover guard fails loudly);
  - CONTRIBUTING documents one Neo4j container per branch.
- [ ] Pushed to origin, PR opened, CI green (including `upgrade from 0.12.1` and
  `old name only in the migration`), merged.

## Constraints and assumptions

- Builds on 0.12.1 (the layout marker, `.conduit/kv/`).
- 0.12.0 key-value stores need one start under 0.12.1 first. Their guards sit
  in `.conduit/<namespace>/`, and a namespace could collide with another
  owner's entry. This is recorded in the decision record and the release notes.

## Decisions

- [2026-11-25-rename-conduit](../decisions/2026-11-25-rename-conduit.md)
- Migration code lives in files named `legacy.rs`, one per crate that has any:
  - core `adapter/legacy.rs`: file stores, SQLite, shared SQL names;
  - `postgres`, `mysql`, `mongodb` and `neo4j`: each its own `src/legacy.rs`.

  That makes the CI allowlist exact by file name, without whole `lib.rs` files.
- MongoDB has no lock to serialise concurrent migrations. Each pass re-reads the
  state and retries up to 5 times after a failed create or rename, and drops a
  second temporary tombstone that a late start created. Neo4j retries on
  transient errors.
- The crash hook moved to `kulyrax_core::crash` (`#[doc(hidden)]`), so the
  backend crates can use it, together with a feature-gated `test_knob` that the
  Neo4j relabel batch size reads.

## Verification

| Check | Command | Result |
|---|---|---|
| Inventory of persisted names | `grep -rnoi conduit crates --include=*.rs`; `docs/stability.md` §5–6 | as listed in the decision record |
| Core suite after the file-store change, first run | `cargo test -p kulyrax-core --no-fail-fast` | many tests failed because they expected `.conduit/` paths (expected). Fixed by the bulk rename of tracked files. The three 0.12-layout tests in `file_store_names.rs` broke as a result; moved to `rename_migration.rs` and rewritten |
| `rename_migration.rs` (core), first run | `cargo test -p kulyrax-core --test rename_migration` | 6/7. **Test bug**: asserted `.kulyrax/wal/` absent, but the store always creates it; now asserts the guard file is absent. Then 7/7 |
| `kulyrax_dir_shared.rs`, first run | `cargo test -p kulyrax-core --test kulyrax_dir_shared` | failed. **Test config**: `on_existing: replace` needs the `upsert` capability; added `capabilities`. Then 1/1 |
| `check_paths` unit test | `cargo test -p kulyrax-core --lib file_store` | passed first run |
| Postgres migration tests | `cargo test -p kulyrax-adapter-postgres --test pg_rename` (own schema per test) | 2/2 first run; 3/3 with the concurrent-start test |
| MySQL migration tests | `cargo test -p kulyrax-adapter-mysql --test mysql_rename` (own database per test) | 3/3 first run; 4/4 with the concurrent-start test |
| MongoDB migration tests, first run | `cargo test -p kulyrax-adapter-mongodb --test mongo_rename` via `127.0.0.1` | 0/3. **Environment**: a non-Docker `mongod` also listens on `127.0.0.1:27017` on this laptop ("transactions … only allowed on a replica set member"). The existing `mongo_backend` failed the same way (2/11). Via `localhost`: 3/3 and 11/11 |
| MongoDB backend suite after the migration landed | `cargo test -p kulyrax-adapter-mongodb` | 7/11 failed once. **Code bug**: `kulyrax_ci` still had a 0.12 guard collection, and 11 tests migrated it concurrently. One won and the rest failed on the rename race. Fixed with re-read-and-retry |
| MongoDB concurrent starts, first run | new `concurrent_starts_all_migrate` | every prepare succeeded, but a stray `kulyrax_tombstone_tmp` was left. **Code bug**: a late start made a second temporary after the migration finished. Fixed: it is dropped once the tombstone is in place. Then 5/5 runs green |
| Neo4j migration test | `cargo test -p kulyrax-adapter-neo4j --test neo4j_rename` (batch 3 over 7 lanes) | 1/1 first run |
| CLI migration tests, compile | `cargo test -p kulyrax-cli --test rename_migration --no-run` | failed: a shadowed variable (`dir`), fixed |
| 0.12.1 binary | `git worktree add ../kx-0121 eb1ae61`; `cargo build -p kulyrax-cli --features kulyrax-core/crash-points` | `kulyrax 0.12.1` |
| CLI upgrade and kill tests, all backends | `cargo test -p kulyrax-cli --test rename_migration` with `KULYRAX_V0121_BIN` and every backend URL | 15/15 first run, serially. In parallel, `upgrade_elasticsearch` failed in step 1 (the 0.12.1 binary writing to ES). **Environment**: the ES log shows "writing cluster state took 31–36 s" under disk contention, past the 10 s client timeout. Fixed in the test with `timeout_ms: 60000`, plus deleting this session's leftover test indices. Then 3/3 parallel runs green |
| 0.12.1 on a migrated store | the same test, with the old binary's output printed | file stores: `AdapterPrepare … os error 183`; MySQL: `AdapterPrepare … is not BASE TABLE`; SQLite, Postgres, MongoDB: every event failed, `RetryExhausted`, nothing committed. Non-zero exit in all of them |
| Postgres backend suite after the CLI tests | `cargo test -p kulyrax-adapter-postgres --test pg_backend` | `pg_prepare_twice_is_idempotent` failed (4 ≠ 1). **Test bug**: it counted the guard table in every schema, and the CLI tests left schemas behind. Fixed: it counts `current_schema()`, and the CLI tests drop their schemas and databases |
| MySQL `mysql_column_missing_rejected` | `cargo test -p kulyrax-adapter-mysql --test mysql_backend` | fails, as on `cda1862`, from a leftover guard row (`fresh_table` restarts at `t_0`). Covered by release follow-up (b). Not caused by this change |
| Every Docker suite | postgres 21 + 3, mysql 12 of 13 + 4 + the other MySQL files, redis 9, mongodb 11 + 4, neo4j 13 + 1, elasticsearch 9 (serial), postgres outbox source 12 | all green except the pre-existing MySQL test above |
| Gates | `cargo build --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt --check`; `cargo test --workspace --no-fail-fast` | clean; clean (exit 0); clean; 478 passed, 0 failed |
| Old-name check | the CI step's command over tracked files | no hits outside the allowlist |
| Engine untouched | `git diff --stat HEAD -- crates/core/src/dispatch.rs crates/core/src/runtime/dependency_graph.rs`; `adapter/mod.rs` diff | no changes; `adapter/mod.rs` only gains `pub mod legacy` |
| No panics in core library code | added lines in `crates/core/src` outside `#[cfg(test)]` | none |

After the rebase onto main at 0.12.2 (runbook B1) and the isolation fix (B2):

| Check | Command | Result |
|---|---|---|
| Rebase | `git rebase --onto upstream/main eb1ae61 step2-rename` | `eb1ae61` dropped: main's `92aaf59` is the same fix (`git range-diff` differs only in lockfile context). Conflicts resolved, with both sides kept: `ci.yml` (main's audit job, this branch's two jobs); `crates/cli/Cargo.toml` (`rusqlite` 0.43, main's version); `file_store.rs` (the crash hook moved to `crate::crash`; main's DLQ crash points repointed); `stability.md`; `Cargo.lock` |
| Old name after the rebase | the CI step's grep over tracked files | 1 hit from main: `redacted_errors.rs` waited on `store/.conduit/lock`. Now `.kulyrax/lock` |
| Gates after the rebase | fmt; clippy `-D warnings`; `cargo test --workspace` | clean; clean; 518 passed, 0 failed |
| 0.12.1 binary from the tag | `git checkout v0.12.1` in `../kx-0121`; `cargo build -p kulyrax-cli --features kulyrax-core/crash-points --target-dir target/v0121-tag` | `kulyrax 0.12.1` (`12a3f93`). The earlier local binary was built from `eb1ae61`, before the release |
| Shared guard objects before the run | `docker exec … psql / mysql / mongosh` on `kulyrax_ci` | 0.12-era `conduit_projection_state`: Postgres 534 rows, MySQL 3, MongoDB 168. A test that migrated them would change this |
| Gates after B2 | fmt; clippy `-D warnings`; `cargo test --workspace` | clean; clean; 518 passed, 0 failed |
| Every Docker suite after B2, first run | `cargo test -p <crate> -- --test-threads=1`: Postgres, MySQL, Redis, MongoDB, Elasticsearch, this branch's own Neo4j (`bolt://localhost:7688`), outbox, ingest, CLI with `KULYRAX_V0121_BIN` | 0 failures: postgres 30, mysql 25, redis 14, mongodb 21, elasticsearch 11, neo4j 20, source-postgres 14, ingest 14, cli 32. A rerun with `--nocapture` printed no `SKIP` line in the CLI, Neo4j, MongoDB, Postgres, MySQL, ingest or outbox suites |
| Shared guard objects after the run | the same queries, then `diff` | unchanged |
| CI `old name only in the migration`, PR #26, first run | the CI job | **failed**. Cause: B2's Neo4j wipe helpers (`crates/adapters/neo4j/tests/common/mod.rs`, `crates/cli/tests/support/neo4j.rs`) and CONTRIBUTING named the 0.12 guard label and constraint prefix. The local grep had run before B2, not after. Fixed: both old names moved to a `legacy.rs` beside each helper (the allowlist's convention), and CONTRIBUTING says "0.12's prefix". The grep over tracked files then finds nothing. Neo4j 13 + 1 + 4 and CLI `ordering` 3 + `rename_migration` 15 rerun green, with no SKIP lines |

Not run: the new CI jobs (`old-name`, `upgrade-from-0-12-1`). They run on the
PR.

## Risks and open questions

- Neo4j has no tombstone. A 0.12 process started after the upgrade would
  recreate `__ConduitGuard` nodes; the next 1.0 start would then relabel them, or
  refuse if a lane is under both labels. The release notes say to stop all 0.12
  processes first.
- Rust's test harness has no end-of-binary hook. A per-binary schema or
  database (`kx_run_<binary>`) is therefore dropped when the next run starts,
  not when its own run ends (the same as the MySQL helper the owner approved).
  Tests that make one per test drop it at their end.
- The Postgres adapter reads a table's column types from
  `information_schema.columns` by table name only, not schema
  (`crates/adapters/postgres/src/lib.rs`). With the same table name in two
  schemas, the types merge. It predates this work and is reported to the owner,
  not fixed here.
- Local environment notes: on this laptop, use `localhost` for MongoDB (another
  `mongod` sits on `127.0.0.1`). Delete leftover ES test indices before parallel
  runs.

## Outcome

Implemented and verified on `step2-rename`, rebased onto main at 0.12.2, with
the test-isolation fix.
