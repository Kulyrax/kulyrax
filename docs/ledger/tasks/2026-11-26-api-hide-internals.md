---
status: in-progress
---

# Hide internals; one canonical path per type (step 4, D2)

## Objective

Runbook D2: hide internals per `docs/stability.md`; one canonical path per type;
duplicate re-exports removed or `#[doc(hidden)]` and noted. This is tranche 1 of
Phase D's split, after D1.

Decision: [2026-11-26-canonical-paths](../decisions/2026-11-26-canonical-paths.md)
(the owner chose "module path + front door").

## Acceptance criteria

- [x] Canonical path per type = its defining module; the crate root keeps the
  front door visible.
- [x] Every other re-export `#[doc(hidden)]`; the duplicate `report` module
  removed.
- [x] Every UNSTABLE item hidden: `#[doc(hidden)]` where Kulyrax's other crates
  use it, `pub(crate)` where nothing outside core does.
- [x] Dead code exposed by `pub(crate)` removed, or marked with the reason.
- [x] Docs point at stable paths (the embedded example's README now uses
  `pipeline::load_project`).
- [ ] Four gates green; every Docker suite green; PR merged.
- [x] Re-count of undocumented public items after D2, and a new D8 estimate (the
  owner's request for the tranche 1 report).

## Verification

| Check | Command | Result |
|---|---|---|
| Uses outside core, per module | `git grep 'kulyrax_core::<module>'` outside `crates/core/src` | 0 for `batch`, `scaffold`, `file_store`, the loaders and previews, `execution::{time, time_option}`, `runtime::{execute, factory, result}`, `report`. Others in use: `crash` 4, `dispatch` 4, `kulyrax_dir` 1, `redact` 7, `execution::stages` 15, `runtime::dependency_graph` 1 |
| `pub(crate)`, first build | `cargo build --workspace --all-targets` | 12 dead-code warnings, all newly visible. The 4 `*PreviewBuilder`s, `ExecutionResult` and `FileStore::dir` are never used: removed. `BatchResult::RolledBack`'s reason is never read: kept, with `#[expect(dead_code)]` and the reason |
| Build and clippy | `cargo build`; `cargo clippy --workspace --all-targets -- -D warnings` | clean |
| Rustdoc, not a gate | `RUSTDOCFLAGS=-D warnings cargo doc -p kulyrax-core --no-deps` | main: 6 errors. After hiding: 10, from links to newly private items (`FileStore`, `crate::batch`, `crate::adapter::file_store`). Those became plain code text. Now 3, all on main already, left for D8 |
| Gates and Docker suites | fmt; clippy `-D warnings`; `cargo test --workspace`; every example built, and the embedded one run; every Docker suite (Neo4j on 7688) | clean; clean; 527 passed, 0 failed; 0 example build errors, the embedded example projects `users.u1`; postgres 30, mysql 25, redis 14, mongodb 21, elasticsearch 11, neo4j 20, source-postgres 14, ingest 15, cli 32, all passed |
| Undocumented public items after D2 | `RUSTFLAGS=-W missing_docs cargo check --workspace --lib`, counted per crate by file path | **567 hand-written**: core 536 (609 before D2), ingest 13, sources/postgres 6, client 5, elasticsearch 3, neo4j 2, mongodb 1, mysql 1. Plus 6 in the generated `pb` code, which D3 hides. The sizing on 2026-11-26 counted core's 609 in every crate's total; the real figure then was about 620 |
