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
2. Extract MySQL, then Redis, then MongoDB, then Neo4j, one commit each. Remove each enum variant from `AdapterConfig` and move its `*_backend.rs` test.
3. Move all adapter crates under `crates/adapters/`.
4. Replace the four `loader.rs` files with one generic `load_mappings<M>`.
5. Do the comment pass.
6. Update the docs.

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

## Risks and open questions

- Removing `AdapterConfig::{MySql, Redis, MongoDb, Neo4j}` and changing the `AdapterFactory` signature breaks the public API for embedders. That is acceptable before 1.0 but needs a release note. The owner must accept it.
- Validation previously gave a false positive for adapters resolved through the registry (commit 49dc976). Each extraction must re-check `validation.rs` for arms specific to a storage kind.
- If an embedder calls `execute_event*` in `lib.rs`, it uses an empty registry and will lose the networked backends. Those helpers need a registry parameter, or must be documented as core-only. `kulyrax-cli` and `kulyrax-ingest` are not affected: both build through `KulyraxRuntime` plus `register_default_backends` (verified with grep).
- **Open, owner decision needed before step 2:** `docs/writing-a-backend.md` says "MySQL ships inside `kulyrax-core` itself (a deliberate product decision)". `kulyrax-backends/Cargo.toml` and the unmerged MySQL branch say the opposite. Extracting MySQL, Redis, MongoDB and Neo4j reverses whichever decision is current.

## Outcome

