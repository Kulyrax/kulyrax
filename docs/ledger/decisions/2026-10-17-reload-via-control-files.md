---
status: accepted
---

# `kulyrax reload` talks to the running process through control files

Task: [Reload a running process's config, routing and mappings](../tasks/2026-10-17-reload-command.md)

## Context

Config, routing and mappings are read once at startup. A long-running `kulyrax run` or `kulyrax ingest` needed a restart to pick up an edit. The owner asked for an explicit `kulyrax reload` command rather than file watching. The command runs as a separate process, so it needs some channel to the running one, and that channel must work the same on Windows and Linux.

## Options considered

1. **Control files next to the config** (`<config folder>/.kulyrax/control/reload-request` and `reload-result`), checked by the run loop between batches. Works on every platform, needs no port or extra dependency, and both sides find it from the config path alone.
2. **A Unix signal (SIGHUP).** Conventional for daemons, but Windows has no equivalent, and a signal can't carry back whether the reload succeeded.
3. **An admin RPC on the gRPC server.** Fits `ingest`, but `run` has no server, and it would extend the public proto contract for an operator feature.

## Evidence

- Verified: `run` and `ingest` both end in `KulyraxRuntime::run_sources`, so one hook in the source runner covers both (`crates/ingest/src/lib.rs` `serve`, `crates/core/src/pipeline.rs` `run_source_loop`).
- Verified: the top of the runner loop is reached only after every batch polled so far has been committed or the loop has returned, so a swap there never splits a batch across two configurations.
- Verified: source positions are checkpointed per source, so swapping the source list mid-run would need checkpoint handling. Rejecting that change is simpler and safe.

## Decision

Option 1. The command writes a request holding a fresh token and waits for a result carrying the same token. Between batches the process takes the request and loads and validates the project with the same code as startup. It builds new adapters before replacing anything, then swaps config, routing and adapters. On any failure it keeps the old setup and reports the reason. A changed `sources:` is rejected with "restart the process". A request nobody answers is withdrawn when the command times out (exit 2).

## Trade-offs accepted

- Latency is up to one poll interval (1 s by default) plus the time to build adapters.
- If two processes share one config file, only the first to take the request reloads.
- While new adapters are built, the old ones are still open, so a store briefly sees both sets of connections.

## Revisit if

- Users need to reload several processes at once, or across machines; a control file is local to one host.
- A reload has to change `sources:` without a restart.
