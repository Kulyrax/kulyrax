---
status: accepted
---

# Adapters set up their store in an explicit `prepare` step

Task: [2026-10-26-sql-schema-out-of-transaction](../tasks/2026-10-26-sql-schema-out-of-transaction.md)

## Context

SQL adapters created their guard table with `CREATE TABLE IF NOT EXISTS` inside
every event's transaction. On MySQL that DDL commits the transaction implicitly,
breaking the atomicity of guard read, target write and guard write. The DDL has to
move out of the event path. Building adapters must stay free of side effects,
because `dry-run` builds the same adapters and must not write.

## Options considered

1. **Lazy, once per adapter, on the first event**, in a transaction of its own
   (the pattern MongoDB and Neo4j use for their index and constraint).
2. **An explicit `StorageAdapter::prepare`**, called by every write path after
   building adapters and before projecting; never by `dry-run`.
3. **At construction.** Rejected: makes building adapters touch the store, which
   `dry-run` and `explain` also do.

## Evidence

- The bug and its reproduction: the task record above.
- `dry-run` builds adapters through the same pipeline helpers as `run`
  (`crates/core/src/pipeline.rs`), so a side effect at construction reaches it.
- Owner review (2026-10-26): hidden first-event side effects are harder to reason
  about and test.

## Decision

Option 2. `StorageAdapter` gains `fn prepare(&self) -> Result<(), AdapterError>`,
defaulting to `Ok(())`, so adapters that need no set-up, including third-party ones,
are unaffected. The SQL adapters create the guard table there through
`sql::exec::ensure_schema`, in a transaction of their own; MongoDB creates its guard
index and Neo4j its guard constraint. Each records success in
`kulyrax_core::adapter::Prepared`, set only after the set-up has committed. `handle`
on an unprepared adapter of these five fails with "adapter not prepared" (and the
last prepare error, if any), and writes nothing.

Callers:

- `run` / `ingest` (source loop): prepare every adapter before the first poll; any
  failure stops the run with `SourceError::Prepare` before ingesting.
- `kulyrax reload`: the new adapter set is prepared before it is offered; a failure
  rejects the reload. The `Reloader` contract now says swapped-in adapters arrive
  prepared.
- CLI `run --event`, `replay`, `ingest`: `KulyraxRuntime::prepare_adapters()` right
  after building, failing with `PipelineError::AdapterPrepare`.
- Report-returning library APIs (`KulyraxRuntime::run_once`, `execute_event`,
  `ReplayContext::run_stream*`) prepare before projecting. They can't return an error
  without a signature change, so an adapter that failed to prepare fails each event
  routed to it, with the prepare error in the report.
- `dry-run` and `explain` never call it.

## Trade-offs accepted

- A public trait method is added. It has a default, so existing implementations
  compile; but code that builds these five adapters itself and calls `handle` directly must
  now call `prepare` first.
- `prepare` may run more than once in a process (the CLI prepares, then the run loop
  prepares the same adapters). It is idempotent; the cost is one DDL round trip per
  call, not per event.
- A guard table dropped while running is recreated only at the next `prepare`, not
  on the next event.
- Code that builds these adapters directly is broken until it calls `prepare`:
  recorded as a breaking change with a migration line in `RELEASES/v0.10.0.md` and
  `CHANGELOG.md`.

## Revisit if

- An adapter needs set-up that is not idempotent.
