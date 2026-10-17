---
status: completed
---

# Finish the adapter crate split and make the workspace navigable

## Objective

Every storage backend should be findable in one predictable place, and adding a backend should follow a single path. Today they are split three ways: four networked drivers live inside `kulyrax-core`, two have their own crates, and `kulyrax-backends` registers only those two. Source comments are also full of phase history, which buries the comments that explain *why*.

Out of scope: behaviour changes to `decide()`, guard lanes, dispatch order, or the YAML `type:` names users write.

## Acceptance criteria

- [x] `kulyrax-core` depends on no networked driver (`mysql`, `redis`, `mongodb`, `neo4rs`) and no `tokio`.
- [x] Every networked backend lives in `crates/adapters/<name>/`, with its tests in that crate.
- [x] `kulyrax-backends` registers all of them behind one Cargo feature each; `default = ["all"]`.
- [x] Existing configs keep working unchanged (`type: mysql|redis|mongodb|neo4j|postgres|elasticsearch`).
- [x] Adapter factories receive mappings, upcasters and migration policy through one context value instead of capturing them at registration time.
- [x] Crate directories drop the `kulyrax-` prefix (`crates/core`, `crates/adapters/postgres`, …); package names, `-p` flags and `use kulyrax_*` paths are unchanged.
- [ ] Waived by the owner on 2026-10-17: the four copy-pasted `loader.rs` files are replaced by one generic loader. Not done, and no longer planned: see Decisions.
- [ ] Waived by the owner on 2026-10-17: no `// Phase N:` history comments remain in `crates/*/src`. Not done; 296 lines still match (361 at the survey).
- [x] CLAUDE.md, architecture.md and docs/writing-a-backend.md describe the real layout.
- [x] `cargo build`, `test`, `clippy -D warnings` and `fmt --check` all pass for the workspace at every step.

## Constraints and assumptions

- Constraint: Zero Panic Rule and Thin CLI Pattern (CLAUDE.md).
- Constraint: work happened on branch `refactor/adapter-crates`, merged into `main` as PR #1.
- Assumption (owner said "start" without answering): the pre-1.0 API break is accepted, and SQLite plus the file-backed document / KV / graph adapters stay in core. They are the reference backends most of `crates/core/tests/` runs against, and they need no network.
- Constraint: the networked backend tests (`*_backend.rs`) return early unless `KULYRAX_<DB>_URL` is set. Without live databases they prove only that the code compiles.
- Assumption: the unmerged branch `claude/phase-28-mysql-crate` (d8de7bf, still using the old `conduit-*` names) can serve as a reference for the MySQL extraction. It cannot be merged as is.

## Plan

1. Done: introduce `MappingSet` and `AdapterBuildContext`, change `AdapterFactory` to `Fn(&CustomAdapterConfig, &AdapterBuildContext)`, and add `CustomAdapterConfig::backend_config`. Port Postgres and Elasticsearch to it. The only API removal is `KulyraxRuntime::{sql,document,kv,graph}_mappings()`.
2. Done: guard against the regressions the extraction would cause. Validation classifies first-party custom types (`postgres`, `mysql`, `mongodb`, `elasticsearch`, `redis`, `neo4j`) by storage kind via a name → kind table, so their mapping and capability checks still run; third-party types stay deferred. `KulyraxRuntime::build_adapters()` returns `PipelineError::AdapterBuild` when any custom adapter fails to build; every pipeline command and `kulyrax-ingest` call it at startup. This also restores both checks for Postgres, which lost them when it moved out.
3. Done: extract MySQL, Redis, MongoDB and Neo4j, one commit each. Remove each enum variant from `AdapterConfig` and move its `*_backend.rs` test.
4. Done early, so new crates are created in place: move the adapter crates under `crates/adapters/`.
5. Done (owner request, 2026-10-17): drop the `kulyrax-` prefix from crate directories and fix every path reference in docs, examples, proto comments and the ledger's current-state lines. Also fixed while checking the docs: the `CLAUDE.md` tree and overview, the startup-failure paragraph in `docs/writing-a-backend.md`, and three stale example README lines.
6. Dropped: replace the four `loader.rs` files with one generic `load_mappings<M>`.
7. Not done (waived): the comment pass.
8. Covered by step 5: the docs were checked against the final layout, which steps 6 and 7 would not have changed.

## Decisions

- [Networked backends live in their own crates and resolve through the registry](../decisions/2026-10-16-backends-out-of-core.md)
- Use a parameter object (`AdapterBuildContext`) rather than a DI container: it removes 16 `#[allow(clippy::too_many_arguments)]` and the closure-capture boilerplate in `kulyrax-backends`, and adds no new runtime machinery.
- Rename crate directories, not packages. A package named `core` would shadow Rust's built-in `core` crate, and `cli` / `client` / `ingest` are not usable names on crates.io. Keeping package names also leaves `-p kulyrax-*` in CI, `use kulyrax_*` in code and `Cargo.lock` untouched. Directory-only naming is common in Rust workspaces.
- Keep the four `loader.rs` files; don't build a generic loader yet (owner agreed, 2026-10-17). They total 158 lines, each has one caller in `pipeline.rs`, and no commit has changed loader behaviour since the rebrand. A generic version needs a trait to read each mapping's event name (graph's mapping is an enum), plus four impls and an error label, so it saves about 100 lines at the cost of one more indirection. Revisit when loader behaviour has to change, for example to accept `.yml` (all four silently skip it), to read subfolders, or to return typed errors. At that point one generic loader avoids making the same edit four times.

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

| Step 3: MongoDB out of core | `cargo tree -p kulyrax-core -e normal --depth 1` | no `mongodb`; still `neo4rs`, `tokio` |
| Step 3: MongoDB factory | `cargo test -p kulyrax-adapter-mongodb --lib` | 4 passed (empty URL, empty database, unparseable URL, valid URL builds without a server) |
| Step 3: MongoDB final gates | `cargo test -p kulyrax-backends --test registration`; clippy `-D warnings`; `cargo test --workspace`; `cargo fmt --check` | 5/5 registration tests; clean; 101 test binaries, 285 passed, 0 failed; exit 0 |

| Step 3: Neo4j out of core | `cargo tree -p kulyrax-core -e normal` piped to `grep -c` for mysql/redis/r2d2/mongodb/neo4rs/tokio | 0 matches: core has no networked driver and no tokio |
| Step 3: Neo4j URI check | first run of the factory and registration tests | **failed**: `neo4rs::ConfigBuilder::build()` accepts `"not a uri"`. Reading neo4rs 0.8 source shows `build()` only checks that fields are present; the URI is parsed in `connection.rs` at connect time. So the pre-branch load-time check never rejected a malformed URI either. Fixed with `check_bolt_uri`, which mirrors neo4rs's `NeoUrl::parse` plus its scheme match |
| Step 3: Neo4j factory | `cargo test -p kulyrax-adapter-neo4j --lib` | 4 passed (empty, unparseable, unsupported scheme; six valid forms accepted, including scheme-less `host:port`) |
| Step 3: Neo4j final gates | registration test (6/6); clippy `-D warnings`; `cargo test --workspace --no-fail-fast`; `cargo fmt --check` | clean; 103 test binaries, 290 passed, 0 failed; exit 0 |
| Configs unchanged | `kulyrax-backends/tests/registration.rs` parses a YAML entry for each of the six `type:` names and reaches that backend's own factory | 6/6 pass |

| Step 5: rename safety survey | grep for cross-crate relative paths in `crates/`, `-p`/`--package` in `.github/`, path deps in `examples/`; crates.io API for `kulyrax-core` / `kulyrax-cli` | no relative paths across crates; CI selects by package name only; 2 example path deps; both crates 404 (unpublished) |
| Step 5: no stale paths | grep for `crates/(adapters/)?kulyrax-` and `kulyrax-<crate>/<path>` outside `target/`, `ledger/`, `RELEASES/`; existence check of every `crates/…` path in `*.md` / `*.proto` / `*.toml` / `*.cs` | 0 left; one dangling path found (`pg_backend.rs` still pointed into core) and fixed |
| Step 5: doc claim on startup failure | read `KulyraxRuntime::build_adapters` (`crates/core/src/runtime/engine.rs:150`) and its call sites in `crates/core/src/pipeline.rs` and `crates/ingest/src/lib.rs` | returns `PipelineError::AdapterBuild` before the first event; `docs/writing-a-backend.md` rewritten to match |
| Step 5: final gates | `cargo fmt --check`; clippy `-D warnings`; `cargo test --workspace --no-fail-fast`; `git diff Cargo.lock` | clean; clean; 103 test binaries, 290 passed, 0 failed; root lockfile unchanged |
| Step 5: examples outside the workspace | `cargo check --manifest-path` for `examples/embedded` and `examples/grpc-producer/producer` | both finish. `examples/embedded/Cargo.lock` lost about 3,100 lines: it was stale since core dropped its network drivers, and the regenerated file is kept |
| Merge | `gh pr checks 1`; `gh pr view 1` | all 10 required CI checks pass on `e9baa66`; merged into `main` as `e02c744` on 2026-10-17 |
| Waived criteria, current state | `grep -c "Phase [0-9]"` over `crates/*/src` and `crates/adapters/*/src`; `ls crates/core/src/adapter/*/loader.rs` | 296 lines; 4 loader files |

Not run: the `neo4j_backend` integration suite against a real Neo4j (gated on `KULYRAX_NEO4J_URI`).

Not run: the `mongo_backend` integration suite against a real MongoDB (gated on `KULYRAX_MONGO_URL`).

Not run: the `redis_backend` integration suite against a real Redis. It is gated on `KULYRAX_REDIS_URL` the same way.

Not run: the `mysql_backend` integration suite against a real MySQL. Its 7 tests return early without `KULYRAX_MYSQL_URL` (they report "ok" in 0.00s), so they only show the moved file compiles. No database was available in this session.

## Risks and open questions

- Removing `AdapterConfig::{MySql, Redis, MongoDb, Neo4j}` and changing the `AdapterFactory` signature breaks the public API for embedders. That is acceptable before 1.0 but needs a release note. The owner must accept it.
- Validation previously gave a false positive for adapters resolved through the registry (commit 49dc976). Each extraction must re-check `validation.rs` for arms specific to a storage kind.
- If an embedder calls `execute_event*` in `lib.rs`, it uses an empty registry and will lose the networked backends. Those helpers need a registry parameter, or must be documented as core-only. `kulyrax-cli` and `kulyrax-ingest` are not affected: both build through `KulyraxRuntime` plus `register_default_backends` (verified with grep).
- Resolved 2026-10-16: `docs/writing-a-backend.md` called keeping MySQL in core "a deliberate product decision", which contradicted `kulyrax-backends/Cargo.toml`. The owner chose to move all four backends out.
- Behaviour change: an unregistered `type:` or a rejected factory entry now stops `run` / `dry-run` / `replay` / `ingest` at startup. Before, the CLI started and failed every event routed to that adapter. `docs/writing-a-backend.md` now describes this (step 5).
- **Merge order (agreed 2026-10-17, done the same day):** this branch was based on `kulyrax-main` at 4d37922, which is 4 commits ahead of `origin/main` (Kulyrax/kulyrax) and has uncommitted Phase 29.1 work (`ack_protocol_version`) in the main checkout. The order is: commit Phase 29.1 on `kulyrax-main`, push it to `origin/main` (Kulyrax/kulyrax), then rebase this branch onto it and open a PR. Expect conflicts in `kulyrax-ingest/src/lib.rs` (the one-line startup build check) and in `pg_backend.rs`, which this branch moved to `crates/adapters/`. Also expect `kulyrax-ingest`'s new Postgres/Elasticsearch dev-dependencies to need checking against the moved crates.
- CI on `origin/main` (Kulyrax/kulyrax) is red: run 38847664638 failed in `test (postgres)` with `SqlMappingNoSqlTarget` on `examples/postgres`, the false positive fixed in 49dc976, which is not pushed yet. CI only starts helping this branch once that push lands.
- The commits on this branch were authored as `naman <naman@slt.work>`. The repo identity is now `Panchauly <panchauly@gmail.com>`, so re-author them during the rebase (`--exec "git commit --amend --no-edit --reset-author"`).
- The first-party name → kind table in `validation.rs` must be kept in sync with `kulyrax-backends`.
- Neo4j connects eagerly when its adapter is built, because `neo4rs` has no lazy connect. Adapters are now built at startup, so that connection attempt moves from the first event to startup. A connection failure is still stored and reported per event, as before; only the timing changed.
- Neo4j's factory now rejects a malformed URI or unsupported scheme at startup. Before this branch such a config passed validation and failed on every event. Behaviour change, in the safe direction. A wrong entry would apply the wrong kind's checks to that type.

## Outcome

Completed on 2026-10-17 and merged as PR #1 (`e02c744`). The owner declared the work complete and waived the comment pass. The generic loader was dropped (see Decisions). Every networked backend now lives in `crates/adapters/<name>/` and resolves through the registry. `kulyrax-core` has no network driver and no tokio. Crate directories drop the `kulyrax-` prefix, and adapters that fail to build stop startup.

Follow-ups, not started:
- Not planned: a generic mapping loader. Revisit with the first change to how mappings load, such as `.yml` support.
- Remove the 296 `Phase N` history comments that are left.
- Write the release note for the public API break (removed `AdapterConfig` variants, new `AdapterFactory` signature).
- Give `execute_event*` a registry parameter, or document them as core-only.
- Run the four networked backend suites against real databases. CI runs them in their own jobs, which pass.

