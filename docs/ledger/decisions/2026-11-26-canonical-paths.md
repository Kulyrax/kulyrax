---
status: accepted
---

# One canonical path per public type; internals hidden (step 4, D2)

Task: [2026-11-26-api-hide-internals](../tasks/2026-11-26-api-hide-internals.md)

## Context

Runbook D2: "Hide internals per docs/stability.md; one canonical path per type;
duplicate re-exports removed or doc(hidden) and noted". `stability.md` §1 lists the
UNSTABLE items, and says "the root keeps the most-used ones". Many types are
reachable at several public paths. For example:
- `ExecutionReport` at `execution::report::`, `execution::`, `report::` and the
  crate root;
- `SqlTxn` at `adapter::sql::exec::` and `adapter::sql::`;
- `DryRunReport` at `runtime::dry_run::` and `runtime::`.

`cargo-semver-checks` treats a `#[doc(hidden)]` item as outside the public API,
so a hidden path is not frozen.

## Options considered

The owner was asked, with three options:
- **module path + front door** (recommended);
- strictly one visible path;
- facade paths canonical.

## Decision

The owner chose **module path + front door**:

1. **Canonical path = the module that defines the type.** Examples:
   - `runtime::engine::KulyraxRuntime`;
   - `runtime::dry_run::DryRunReport`;
   - `adapter::sql::exec::SqlTxn`;
   - `execution::report::ExecutionReport`.
2. **The crate root keeps visible re-exports of the front door**, the paths the
   README and the examples use:
   - `KulyraxRuntime`, `AdapterRegistry`, `AdapterBuildContext`, `MappingSet`;
   - `ExecutionReport`, `AdapterExecutionReport`, `AdapterOutcome`,
     `AdapterReportError`, `ExecutionStatus`;
   - `Upcaster`, `UpcasterRegistry`, `UpcastError`;
   - `EventSource`, `SourcedEvent`, `SourcePosition`, `SourceError`;
   - `run_sources`, `RunMode`, `SourceRunOptions`, `SourceRunReport`,
     `StoppedReason`;
   - `ReplayReport`, `ReplayRunOptions`;
   - `ValidationReport`, `ValidationIssue`, `validate_projection_config`;
   - `execute_event*`, which is defined at the root.

   These types have two documented paths.
3. **Every other re-export is `#[doc(hidden)]`**: still reachable, outside
   semver. That covers:
   - the flat `runtime::*` and `execution::*` re-exports;
   - the storage kinds' facade re-exports (`adapter::sql::SqlTxn`, …);
   - the other root re-exports (`ReplayContext`, `replay_stream*`,
     `events_from_path`, `run_sources_with_adapters`, the dependency-graph
     functions, `AdapterExecutionMeta`).

   The root `report` module, a pure duplicate that nothing used, is removed.
   One exception: where the defining module is itself hidden, its re-export is
   the canonical path (`pipeline::InitError` and `pipeline::init_project`,
   from `scaffold`).
4. **UNSTABLE (stability §1) is hidden:**
   - **`#[doc(hidden)]`, used by Kulyrax's other crates:** `dispatch`,
     `kulyrax_dir`, `redact`, `crash`, `runtime::dependency_graph`,
     `execution::stages`.
   - **`#[doc(hidden)]` on single items:** `routing::{load_routing,
     route_with_rules}`, `runtime::reload::{FileReloader, control_dir}`,
     `replay::{ReplayContext, replay_stream, replay_stream_with_options}`,
     `source::runner::{run_sources_with_adapters, run_sources_with_reload,
     Reloader, Reloaded}`, the `DirectorySource` / `StdinSource` constructors,
     `PostgresTxn`, `MySqlTxn`, `PostgresOutboxSource::{open, inspect}`,
     `WATCH_CAP`.
   - **`pub(crate)`, unused outside core:** `batch`, `scaffold`,
     `adapter::file_store`, every kind's `loader`, `execution::{time,
     time_option}`, `runtime::{execute, factory}`.
5. **Dead code that `pub` had hidden goes:** the four `preview` builders,
   `runtime::result::ExecutionResult`, and `FileStore::dir` are never used
   anywhere.

   `BatchResult::RolledBack`'s reason is kept, marked
   `#[expect(dead_code)]`: the run loop does not report why a batch rolled
   back, including a failed commit's adapter and message. Reported to the
   owner as an observability gap; it is not changed here (no new features).

## Trade-offs accepted

- **Front-door types have two documented paths** (root and module), as
  `stability.md` asked.
- **Hidden paths still compile**, so nothing outside the stable surface breaks
  today. They may be removed in a minor release.
- **Canonical module paths are sometimes long**
  (`runtime::engine::KulyraxRuntime`). The front door covers the common
  imports.

## Revisit if

- A hidden item turns out to be needed by third-party code: give it a canonical
  path, in a minor release.
