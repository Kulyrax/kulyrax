---
status: completed
---

# `dry-run` must not write

## Objective

`kulyrax dry-run` wrote to the real database. `KulyraxRuntime::dry_run_once` called `run_once`, and its own comment said "a true dry run depends on adapter-level support". The library helper `execute_event_with_mode(…, ExecutionMode::DryRun)` also built real adapters and dispatched, despite being documented as a "no-write simulation". Anyone using either to check an event before running it against production would change production data. The owner asked for this to be fixed before the remaining features.

## Acceptance criteria

- [x] `kulyrax dry-run` writes nothing and builds no adapter, so it never connects to a store.
- [x] It reports, per routed adapter in execution order, the write a live run would attempt: target, entity key, values, operation, on-existing mode, facet, and versions.
- [x] It resolves writes with the same builders and upcasting a live run uses.
- [x] An unresolvable write is reported per adapter, and the CLI exits 1.
- [x] A third-party adapter type, whose kind is unknown without building it, is reported as not previewable.
- [x] Text and `--output json` output.
- [x] No public API still claims to dry-run while writing.
- [x] fmt, clippy `-D warnings` and `cargo test --workspace` pass.

## Constraints and assumptions

- Constraint: Zero Panic Rule; Thin CLI (the CLI only renders).
- Constraint: never special-case a storage kind in `dispatch.rs`, `dependency_graph.rs` or `decide()`. Dry-run lives in its own module and leaves those untouched.
- Verified: the per-kind `*PreviewBuilder` types are unused and skip upcasting, so they were not used. Dry-run calls the kinds' `*RuntimeBuilder::build`, the code a live run uses.

## Decisions

- Resolve writes at the engine level from the mappings and each adapter's storage kind, rather than adding a `preview` method to `StorageAdapter`. Building an adapter can connect to a store (Neo4j does), and a dry run must not. This also needs no change from third-party backend authors.
- Guard state is not read, so the report shows the write a live run would *attempt*, not whether it would then be skipped. The CLI says so on every run. Reading the guard would mean connecting to the store.
- `KulyraxRuntime` keeps its `MappingSet` and hands each adapter build a copy, instead of moving it into the first build. Dry-run needs the mappings, and so does any rebuild. This fixed a second bug: after `run_sources` consumed the adapters, the next run rebuilt them with empty mappings, so every event would fail. The CLI never hit it (one command per process), but embedders and the planned `reload` would.
- Removed `execute_event_with_mode` and `ExecutionMode`. The function had no callers in the repo, and keeping it would keep a public API that writes while claiming not to. `KulyraxRuntime::dry_run_once` is the replacement.

## Verification

| Criterion | Check | Observed result |
|---|---|---|
| Bug, before | quickstart copy: `dbtool init`, then `kulyrax dry-run --event` with the first event | table empty before, `u1\|Ada\|ada@example.com` after; the dry run wrote |
| Rebuild bug, before | new test `adapters_rebuilt_after_run_sources_still_receive_the_mappings` | failed: the factory saw `[1, 0]` mappings, expected `[1, 1]` |
| Rebuild bug, after | same test | passes (6/6 in `kulyrax_runtime`) |
| No writes, end to end | quickstart copy: `dbtool init`, `kulyrax dry-run` with the `UserEmailChanged` event | table empty before and after; exit 0; reports `would Upsert users key ["u1"]` with the values a live run writes |
| JSON output | same, with `--output json` | `status: planned`, operation, target, entity_key and values as JSON |
| Behaviour | `cargo test -p kulyrax-core --test dry_run` | 5 passed: SQLite file and document root never created; two adapters in execution order with the expected writes; missing key field → `Failed`; a `postgres` adapter with an unreachable URL is previewed without connecting; a third-party type → `NotPreviewable` |
| Gates | `cargo fmt --check`; clippy `-D warnings`; `cargo test --workspace --no-fail-fast` | clean; clean; 104 test binaries, 306 passed, 0 failed |

## Risks and open questions

- Public API changes, for the release note: `KulyraxRuntime::dry_run_once` now takes `&self` and `&Event` and returns `Result<DryRunReport, PipelineError>`; `pipeline::dry_run` no longer takes a backend-registration callback; `execute_event_with_mode` and `ExecutionMode` are removed.
- Dry-run can't say whether a write would be skipped as already projected or stale. Adding that means reading guard state, which means connecting to the store; that could be an opt-in flag if someone needs it.
- The unused `*PreviewBuilder` types remain. They duplicate the runtime builders without upcasting and could be removed in a later cleanup.

## Outcome

`dry-run` now shows what each routed adapter would write and never writes, builds adapters, or connects to a store. A second bug that dropped mappings when adapters were rebuilt is fixed.
