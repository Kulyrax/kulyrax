---
status: in-progress
---

# Errors: `Send + Sync + 'static`, typed safe causes, `AdapterFailure` (step 4, D6)

## Objective

Runbook D6: "Errors: `Error + Send + Sync + 'static` everywhere, with `source()`
chains; `DependencyOrderError` implements `Error`; `AdapterReportError` renamed
(no "Error" suffix)." Tranche 3 of Phase D.

The task turned out bigger than planned: a full chain means re-plumbing about
250 adapter error sites and re-checking driver text for secrets (F-06). The
owner chose to chain only the safe causes: decision
[2026-11-26-error-chains](../decisions/2026-11-26-error-chains.md).

## Acceptance criteria

- [x] Every public error in core, ingest and client is `Error + Send + Sync +
  'static`, held by compile-time tests (`tests/error_traits.rs` in each).
- [x] `source()` returns the typed cause:
  - `PipelineError::ConfigParse(YamlError)` and `EventParse(serde_json::Error)`;
  - `Mapping` / `Routing(LoadError)`, where `LoadError` is new and
    `#[non_exhaustive]`: `Parse` (YAML), `Read` (I/O), `Invalid`;
  - `ReplayLoadError::Json { file, source }`;
  - `InitError::Io`, `IngestError::Runtime`, and all `ServeError` variants.
- [x] Each `Display` is unchanged, so the CLI prints the same text.
- [x] `DependencyOrderError` implements `Display` (the dispatch report's words)
  and `Error`.
- [x] `AdapterReportError` renamed `AdapterFailure`, with its variants and JSON
  unchanged. In `dispatch.rs` that is an identifier swap only: no behaviour
  change.
- [x] Driver causes stay redacted strings (decision item 3).
- [x] Docs: `stability.md` "Errors", and the rc.1 notes (a table of changes,
  before/after code).
- [x] Gates: fmt, clippy `-D warnings`, workspace tests, examples, every Docker
  backend suite.
- [ ] PR merged with the 12 required checks green.

## Constraints and assumptions

- `decide()`, the dependency-graph algorithm and dispatch behaviour are
  unchanged.
  - `dependency_graph.rs` only gains the `Display` / `Error` impls.
  - `dispatch.rs` only gets the renamed type.
- A `Display` that prints its cause and a `source()` that returns it both stay,
  as `PipelineError` already did. A chain printer shows the cause twice; that
  is the existing convention, and it is kept rather than changing every
  message.

## Verification

| Check | Command | Result |
|---|---|---|
| Core error traits and chains | `cargo test -p kulyrax-core --test error_traits` | 5 pass, first run |
| Client / ingest | `cargo test -p kulyrax-client --test error_traits`; the same for `kulyrax-ingest` | 1 pass; 2 pass, first run |
| clippy, first run | `cargo clippy --workspace --all-targets -- -D warnings` | 3 kinds of failure. `result_large_err` in ingest: an inline `YamlError` grew `PipelineError` to 128 bytes; fixed by boxing `YamlError`'s details, so it is one pointer wide. Old tests called `.contains` on the `String` error; fixed with `.to_string()`. `err_expect` in the new test; fixed. Then clean |
| fmt | `cargo fmt --check` | clean |
| Workspace tests | `cargo test --workspace` | 586 pass, 0 fail |
| Examples | the 6 example manifests; `examples/embedded` run | 0 errors each; `projected: users.u1.name = "Ada"` |
| Docker suites (Neo4j on 7688) | `docker-suites.sh` | postgres 35, mysql 30, redis 19, mongodb 26, elasticsearch 16, neo4j 25, source-postgres 16, ingest 17, cli 32: all pass, 0 fail |

## Risks and open questions

- A caller cannot downcast to a driver error. The decision records when to
  revisit that.

## Outcome

In progress.
