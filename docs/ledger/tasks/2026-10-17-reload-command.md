---
status: completed
---

# Reload a running process's config, routing and mappings

## Objective

A long-running `kulyrax run` (without `--once`) or `kulyrax ingest` read its config, routing and mappings once and needed a restart for any change. The owner asked for a `kulyrax reload` command that makes the running process pick up changes safely.

## Acceptance criteria

- [x] `kulyrax reload --config <file>` makes a running `run` or `ingest` re-read config, routing and mappings and use them for the following batches.
- [x] The swap happens only between batches; no batch runs on a mix of old and new adapters.
- [x] An invalid change is rejected with the reason, and the process keeps running on its previous configuration.
- [x] A changed `sources:` is rejected with a restart message.
- [x] The command reports the outcome: applied (exit 0), rejected (exit 1), or no process answered (exit 2, request withdrawn).
- [x] The running process logs every outcome.
- [x] Works the same on Windows and Linux.
- [x] fmt, clippy `-D warnings` and `cargo test --workspace` pass.

## Constraints and assumptions

- Constraint: Thin CLI; core does not print (the front ends get an `on_outcome` callback).
- Constraint: no storage-kind special-casing in `dispatch.rs` or `dependency_graph.rs`. The runner gets a generic `Reloader` hook and knows nothing about files.
- Constraint: `run_sources_with_adapters` is public and used by tests, so it is unchanged and delegates to the new `run_sources_with_reload`.
- Depends on the fix in [2026-10-17-dry-run-no-writes](2026-10-17-dry-run-no-writes.md): the runtime keeps its mappings, so rebuilding adapters works.

## Decisions

- [`kulyrax reload` talks to the running process through control files](../decisions/2026-10-17-reload-via-control-files.md)
- New adapters are built before anything is replaced, and the loop swaps only a fully built, validated `Reloaded`. The `Reloader` hook returns `Some` only in that case.
- `register_backends` on `pipeline::run_source_loop` and `kulyrax_ingest::serve` becomes `Fn + Clone + 'static` instead of `FnOnce`, because a reload registers backends on each new runtime. Function items such as `register_default_backends` already satisfy it.
- `--once` runs get no reloader; they exit as soon as they catch up.

## Verification

| Criterion | Check | Observed result |
|---|---|---|
| Protocol | `cargo test -p kulyrax-core --lib reload` | 2 passed: the result round-trips (including a multi-line reason) and ignores other tokens; an unanswered request is withdrawn |
| Loop behaviour | `cargo test -p kulyrax-core --test reload`: continuous `run_source_loop` on a thread, driven only through files | passes: a mapping change applies to the next event (`email` appears); broken routing is rejected naming routing and the next event still projects with the last good mapping; a changed `sources:` is rejected; 0 events failed; 3 outcomes logged |
| Not flaky | the same test 10 times | 10 of 10 passed |
| `run` end to end | `kulyrax init p`; background `kulyrax run --poll-interval-ms 200`; edit mapping; `kulyrax reload` | `reload applied: 1 adapter(s), 1 routed event type(s)` exit 0; the next event's document has the new `source` field; a bad adapter id in routing gives `reload rejected …` exit 1; both outcomes in the process log |
| Nothing running | `kulyrax reload --timeout-secs 1` | exit 2 with "no running process answered within 1s …" |
| `ingest` end to end | `examples/grpc-producer` copy: background `kulyrax ingest --listen tcp://127.0.0.1:50999`; reload, then reload after breaking routing | applied exit 0, then rejected exit 1; both logged as `kulyrax ingest: reload …` |
| Gates | `cargo fmt --check`; clippy `-D warnings`; `cargo test --workspace --no-fail-fast` | clean; clean; 106 test binaries, 312 passed, 0 failed (the one later change was a lint-only rewrite in the reload test, re-run: passes) |

Not run: reload on Linux or macOS. The protocol uses only `std::fs` (write, rename, remove), and CI runs the reload test on all three OSes once pushed.

## Risks and open questions

- Public API changes for the release note: `pipeline::run_source_loop` takes an extra `on_reload` callback, and its `register_backends` bound and `kulyrax_ingest::serve`'s are now `Fn + Clone + 'static`.
- Reload latency is bounded by the poll interval; `ingest` uses its configured poll interval.

## Outcome

`kulyrax reload --config <file>` makes a running `run` or `ingest` switch to the edited config, routing and mappings between batches. Invalid edits are rejected without disturbing the running process.
