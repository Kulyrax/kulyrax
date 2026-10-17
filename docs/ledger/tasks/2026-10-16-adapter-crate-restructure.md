---
status: in-progress
---

# Finish the adapter crate split and make the workspace navigable

## Objective

Every storage backend should be findable in one predictable place, and adding a backend should follow a single path. Today they are split three ways: four networked drivers live inside `kulyrax-core`, two have their own crates, and `kulyrax-backends` registers only those two. Source comments are also full of phase history, which buries the comments that explain *why*.

Out of scope: behaviour changes to `decide()`, guard lanes, dispatch order, or the YAML `type:` names users write.

## Acceptance criteria

- [ ] `kulyrax-core` depends on no networked driver (`mysql`, `redis`, `mongodb`, `neo4rs`) and no `tokio`.
- [ ] Every networked backend lives in `crates/adapters/kulyrax-adapter-<name>/`, with its tests in that crate.
- [ ] `kulyrax-backends` registers all of them behind one Cargo feature each; `default = ["all"]`.
- [ ] Existing configs keep working unchanged (`type: mysql|redis|mongodb|neo4j|postgres|elasticsearch`).
- [x] Adapter factories receive mappings, upcasters and migration policy through one context value instead of capturing them at registration time.
- [ ] The four copy-pasted `loader.rs` files are replaced by one generic loader.
- [ ] No `// Phase N:` history comments remain in `crates/*/src`; comments that explain an invariant or a reason are kept.
- [ ] CLAUDE.md, architecture.md and docs/writing-a-backend.md describe the real layout.
- [ ] `cargo build`, `test`, `clippy -D warnings` and `fmt --check` all pass for the workspace at every step.

## Constraints and assumptions

- Constraint: Zero Panic Rule and Thin CLI Pattern (CLAUDE.md).
- Constraint: the main checkout has 14 uncommitted modified files (ingest, client, postgres tests, examples). Work happens on branch `refactor/adapter-crates` in worktree `.claude/worktrees/adapter-restructure`, based on 4d37922, so those files stay untouched. Merging back may conflict in `kulyrax-adapter-postgres/tests/pg_backend.rs` and `kulyrax-ingest/src/lib.rs`.
- Assumption (owner said "start" without answering): the pre-1.0 API break is accepted, and SQLite plus the file-backed document / KV / graph adapters stay in core. They are the reference backends most of `crates/kulyrax-core/tests/` runs against, and they need no network.
- Constraint: the networked backend tests (`*_backend.rs`) return early unless `KULYRAX_<DB>_URL` is set. Without live databases they prove only that the code compiles.
- Assumption: the unmerged branch `claude/phase-28-mysql-crate` (d8de7bf, still using the old `conduit-*` names) can serve as a reference for the MySQL extraction. It cannot be merged as is.

## Plan

1. Done: introduce `MappingSet` and `AdapterBuildContext`, change `AdapterFactory` to `Fn(&CustomAdapterConfig, &AdapterBuildContext)`, and add `CustomAdapterConfig::backend_config`. Port Postgres and Elasticsearch to it. The only API removal is `KulyraxRuntime::{sql,document,kv,graph}_mappings()`.
2. Done: guard against the regressions the extraction would cause. Validation classifies first-party custom types (`postgres`, `mysql`, `mongodb`, `elasticsearch`, `redis`, `neo4j`) by storage kind via a name → kind table, so their mapping and capability checks still run; third-party types stay deferred. `KulyraxRuntime::build_adapters()` returns `PipelineError::AdapterBuild` when any custom adapter fails to build; every pipeline command and `kulyrax-ingest` call it at startup. This also restores both checks for Postgres, which lost them when it moved out.
3. Extract MySQL (done), then Redis (done), then MongoDB, then Neo4j, one commit each. Remove each enum variant from `AdapterConfig` and move its `*_backend.rs` test.
4. Done early, so new crates are created in place: move the adapter crates under `crates/adapters/`.
5. Replace the four `loader.rs` files with one generic `load_mappings<M>`.
6. Do the comment pass.
7. Update the docs.

## Decisions

- [Networked backends live in their own crates and resolve through the registry](../decisions/2026-10-16-backends-out-of-core.md)
- Use a parameter object (`AdapterBuildContext`) rather than a DI container: it removes 16 `#[allow(clippy::too_many_arguments)]` and the closure-capture boilerplate in `kulyrax-backends`, and adds no new runtime machinery.

## Verification

| Criterion | Check | Observed result |
|---|---|---|
| Survey: where adapters live | `Glob crates/**/src/**/*.rs`, root `Cargo.toml` | mysql/redis/mongo/neo4j in `kulyrax-core/src/adapter/*`; postgres and elasticsearch in their own crates (recorded during planning) |
| Survey: comment load | `grep -rn "^\s*//.*Phase [0-9]" crates/*/src` | 361 lines |
| Survey: argument bloat | `grep -rn too_many_arguments crates/*/src` | 16 occurrences |
| Survey: loader duplication | `diff` of document vs keyvalue `loader.rs` | identical apart from the type name and error strings |

| Baseline at 4d37922 | `cargo test --workspace` | exit 0. The pass count was not captured: output was truncated by `tail` |
| Baseline at 4d37922 | `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt --check` | clean; exit 0 |
| Step 1: factory context | `cargo clippy --workspace --all-targets -- -D warnings` | clean (after fixing a stale `factory(...)` call in the Postgres unit test) |
| Step 1: factory context | `cargo test --workspace` (before adding the `backend_config` tests) | 94 test binaries, 261 passed, 0 failed |
| Step 1: factory context | `cargo fmt --check` | exit 0 |
| Step 1: `backend_config` | `cargo test -p kulyrax-core --lib registry` | 5 passed, including `backend_config_reads_the_config_block` and `backend_config_errors_name_the_type_and_id` |
| Step 1: final gates | `cargo clippy … -D warnings`; `cargo test --workspace`; `cargo fmt --check` | clean; 94 test binaries, 263 passed, 0 failed; exit 0 |
| Step 2: new behaviour | `cargo test -p kulyrax-core --test validation --test kulyrax_runtime` | 5 new tests pass: `first_party_custom_type_is_validated_as_its_storage_kind`, `third_party_custom_type_defers_kind_checks_to_build_time`, and three `build_adapters_*` tests |
| Step 2: final gates | clippy `-D warnings`; `cargo test --workspace`; `cargo fmt --check` | clean; 94 test binaries, 268 passed, 0 failed; exit 0 |
| Step 2: example configs still validate | `kulyrax explain --mappings …` on `examples/postgres` and `examples/automotive-csv-producer` (the latter with dummy `KULYRAX_*_URL` values that parse) | both exit 0 |
| Step 2: startup failure end to end | `DATABASE_URL="not a url" kulyrax dry-run` on `examples/postgres` | exit 1 at startup: `AdapterBuild([… "pg-primary" … \`url\` does not parse: not a url])` |
| Step 4: crate move | clippy `-D warnings`; `cargo test --workspace` | clean; 94 test binaries, 268 passed, 0 failed |
| Step 3: MySQL out of core | `cargo tree -p kulyrax-core -e normal --depth 1` | no `mysql`; still `mongodb`, `neo4rs`, `redis`, `tokio` |
| Step 3: MySQL resolves via registry | `cargo test -p kulyrax-backends --test registration` | postgres, elasticsearch and mysql factories each reject an unparseable URL by name, so each was found and ran |
| Step 3: MySQL factory | `cargo test -p kulyrax-adapter-mysql --lib` | 2 passed (empty and unparseable URL rejected) |
| Step 3: final gates | clippy `-D warnings`; `cargo test --workspace`; `cargo fmt --check` | clean; 97 test binaries, 273 passed, 0 failed; exit 0 |

| Step 3: Redis out of core | `cargo tree -p kulyrax-core -e normal --depth 1` | no `redis` or `r2d2`; still `mongodb`, `neo4rs`, `tokio` |
| Step 3: Redis factory | `cargo test -p kulyrax-adapter-redis --lib` | 3 passed (empty URL, unparseable URL, valid URL builds without a server because the pool connects lazily) |
| Step 3: Redis final gates | clippy `-D warnings`; `cargo test --workspace`; `cargo fmt --check` | clean; 99 test binaries, 277 passed, 0 failed; exit 0 |

| Rebase onto `origin/main` f325f83 (Phase 29.1 pushed) | `git rebase --onto origin/main 4d37922 --exec "… --reset-author"` | one add/add conflict in this record: `main` held its original `proposed` draft, and all 7 lines unique to it had been revised on this branch, so the branch's copy was kept. The `pg_backend.rs` stale-row fix followed the move, and `ack_protocol_version` merged next to the startup check. All 6 commits are now authored by Panchauly |
| Rebased branch gates | clippy `-D warnings`; `cargo test --workspace`; `cargo fmt --check` | clean; 99 test binaries, 280 passed, 0 failed; exit 0 |
| CI on `origin/main` after the push | `gh run view 39023441834` | `test (postgres)` passed this time (it failed in run 38847664638), along with 8 other jobs; `test (windows-latest)` was still running when recorded |

Not run: the `redis_backend` integration suite against a real Redis. It is gated on `KULYRAX_REDIS_URL` the same way.

Not run: the `mysql_backend` integration suite against a real MySQL. Its 7 tests return early without `KULYRAX_MYSQL_URL` (they report "ok" in 0.00s), so they only show the moved file compiles. No database was available in this session.

## Risks and open questions

- Removing `AdapterConfig::{MySql, Redis, MongoDb, Neo4j}` and changing the `AdapterFactory` signature breaks the public API for embedders. That is acceptable before 1.0 but needs a release note. The owner must accept it.
- Validation previously gave a false positive for adapters resolved through the registry (commit 49dc976). Each extraction must re-check `validation.rs` for arms specific to a storage kind.
- If an embedder calls `execute_event*` in `lib.rs`, it uses an empty registry and will lose the networked backends. Those helpers need a registry parameter, or must be documented as core-only. `kulyrax-cli` and `kulyrax-ingest` are not affected: both build through `KulyraxRuntime` plus `register_default_backends` (verified with grep).
- Resolved 2026-10-16: `docs/writing-a-backend.md` called keeping MySQL in core "a deliberate product decision", which contradicted `kulyrax-backends/Cargo.toml`. The owner chose to move all four backends out.
- Behaviour change: an unregistered `type:` or a rejected factory entry now stops `run` / `dry-run` / `replay` / `ingest` at startup. Before, the CLI started and failed every event routed to that adapter. `docs/writing-a-backend.md` still describes the old per-event behaviour and must be updated in step 7.
- **Merge order (agreed 2026-10-17, done the same day):** this branch was based on `kulyrax-main` at 4d37922, which is 4 commits ahead of `origin/main` (Kulyrax/kulyrax) and has uncommitted Phase 29.1 work (`ack_protocol_version`) in the main checkout. The order is: commit Phase 29.1 on `kulyrax-main`, push it to `origin/main` (Kulyrax/kulyrax), then rebase this branch onto it and open a PR. Expect conflicts in `kulyrax-ingest/src/lib.rs` (the one-line startup build check) and in `pg_backend.rs`, which this branch moved to `crates/adapters/`. Also expect `kulyrax-ingest`'s new Postgres/Elasticsearch dev-dependencies to need checking against the moved crates.
- CI on `origin/main` (Kulyrax/kulyrax) is red: run 38847664638 failed in `test (postgres)` with `SqlMappingNoSqlTarget` on `examples/postgres`, the false positive fixed in 49dc976, which is not pushed yet. CI only starts helping this branch once that push lands.
- The commits on this branch were authored as `naman <naman@slt.work>`. The repo identity is now `Panchauly <panchauly@gmail.com>`, so re-author them during the rebase (`--exec "git commit --amend --no-edit --reset-author"`).
- The first-party name → kind table in `validation.rs` must be kept in sync with `kulyrax-backends`. A wrong entry would apply the wrong kind's checks to that type.

## Outcome

