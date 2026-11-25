---
status: blocked
---

# Rename conduit → kulyrax (1.0 plan, step 2)

Blocked only on publishing: the work is done and verified locally, but it builds
on 0.12.1 (GHSA-6chh-gx4c-448g), which is private until the advisory is
published. It is pushed to origin and opened as a PR after that.

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
- [ ] Pushed to origin and PR opened: waits for the advisory.

## Constraints and assumptions

- Builds on 0.12.1 (the layout marker, `.conduit/kv/`), private until
  GHSA-6chh-gx4c-448g is published. The branch `step2-rename` stays local.
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

Not run: the new CI jobs (`old-name`, `upgrade-from-0-12-1`). The upgrade job
needs the `v0.12.1` tag, which exists only after the release.

## Risks and open questions

- Neo4j has no tombstone. A 0.12 process started after the upgrade would
  recreate `__ConduitGuard` nodes; the next 1.0 start would then relabel them, or
  refuse if a lane is under both labels. The release notes say to stop all 0.12
  processes first.
- The upgrade CI job depends on the `v0.12.1` tag existing (after publication).
- Local environment notes: on this laptop, use `localhost` for MongoDB (another
  `mongod` sits on `127.0.0.1`). Delete leftover ES test indices before parallel
  runs.

## Outcome

Implemented and verified on the local branch `step2-rename`, not pushed. Once
GHSA-6chh-gx4c-448g is published and main has 0.12.1, the branch is rebased onto
main, pushed to origin, and opened as a PR.
