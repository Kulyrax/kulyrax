# Kulyrax — Project Instructions

## System Overview

Kulyrax is a multi-crate Rust workspace implementing a deterministic, **projection-only**
engine for CQRS / Event Sourcing architectures. It consumes an event log it does not own
and projects each event into one or more storage models (SQL, Document, Key-Value,
Graph). It does not produce, buffer, or store events.

What's built and what's open: `ROADMAP.md`. Release notes: `RELEASES/`.

## Workspace Structure

Directories drop the `kulyrax-` prefix; package names keep it (`crates/core` is the
package `kulyrax-core`, so `cargo test -p kulyrax-core` and `use kulyrax_core::…`).

```
kulyrax/
├── Cargo.toml, Cargo.lock          — workspace root (virtual manifest, resolver = "3")
├── README.md, architecture.md, ROADMAP.md, CONTRIBUTING.md
├── docs/                           — concepts, mapping-reference, producer-contract, writing-a-backend, ledger/
├── proto/kulyrax/v1/ingest.proto   — the gRPC ingestion contract (the cross-language SDK)
├── RELEASES/                       — per-version release notes
├── examples/                       — runnable examples (excluded from the workspace)
└── crates/
    ├── cli/                        — kulyrax-cli: thin CLI (main.rs only): init, run, sources, ingest, reload, explain, dry-run, replay
    ├── ingest/                     — kulyrax-ingest: tonic gRPC server + GrpcSource (keeps tonic out of core)
    ├── client/                     — kulyrax-client: reference Rust producer client
    ├── backends/                   — kulyrax-backends: registers every networked backend, one Cargo feature each
    ├── adapters/                   — kulyrax-adapter-<name>: one crate per networked backend
    │   ├── postgres/, mysql/       — SqlTxn
    │   ├── redis/                  — KvBackend
    │   ├── mongodb/, elasticsearch/ — DocumentBackend
    │   └── neo4j/                  — GraphBackend
    └── core/                       — kulyrax-core: the engine (no network drivers, no tokio)
        ├── src/
        │   ├── lib.rs              — public API & crate exports
        │   ├── event.rs            — Event envelope (event_id, event_type, payload, metadata, version, sequence)
        │   ├── routing.rs          — event_type → Vec<AdapterId> via the routing table
        │   ├── dispatch.rs         — synchronous dependency-ordered dispatcher
        │   ├── replay.rs           — catch-up replay (sequence-ordered)
        │   ├── pipeline.rs         — high-level orchestration for front ends
        │   ├── upcast.rs           — payload upcasters
        │   ├── adapter/
        │   │   ├── mod.rs          — StorageAdapter trait, AdapterOutcome, SkipReason, decide()
        │   │   ├── sql/            — SqlTxn / exec.rs + the SQLite backend
        │   │   ├── document/       — DocumentBackend + the file-backed document adapter
        │   │   ├── keyvalue/       — KvBackend + the file-backed key-value adapter
        │   │   └── graph/          — GraphBackend + the file-backed graph adapter
        │   ├── execution/          — ExecutionReport / AdapterExecutionReport, virtual time
        │   ├── source/             — EventSource trait, directory/stdin sources, checkpoint, runner
        │   └── runtime/            — config, factory, registry (AdapterRegistry), dependency_graph, validation, error
        └── tests/                  — integration tests + tests/common/ + tests/fixtures/
```

## Core Contracts

1. **`decide()` (`adapter/mod.rs`)** — one pure function gates every adapter write. Given
   `Operation` (upsert/delete), the mapping's `on_existing`, the persisted guard lane
   `{ last_sequence, deleted, permanent }`, and `event.sequence`, it returns a
   `WriteDecision`. All four adapters call it unchanged. **Never special-case a storage
   kind in `dispatch.rs`, `runtime/dependency_graph.rs`, or `decide()`.**

2. **Guard lanes** — idempotency is entity-keyed and sequence-gated (Phase 11), per
   `(target, entity_key, facet)` (Phase 15). SQL: the `conduit_projection_state` table.
   Document / KV / Graph: a per-entity JSON sidecar. Redelivery → `Skipped(StaleSequence)`.

3. **Dependency order** — Kahn's algorithm over `depends_on`, then priority
   (`runtime/dependency_graph.rs`). Cycles fail at startup (`KulyraxError::CyclicDependency`).
   Dispatch is **synchronous** (no tokio) — one adapter at a time, in order.

4. **Sources** — an `EventSource` (`poll` / `commit` / `committed_position`) hands the run
   loop ordered batches with a resumable position. Commit happens only after a batch fully
   resolves. At-least-once source + idempotent sinks ⇒ effectively-once projection.
   `kulyrax reload` swaps config, routing and adapters only at the top of the loop, after
   every batch so far has committed (`source/runner.rs`, `runtime/reload.rs`).

## Coding Standards & Rules

- **Zero Panic Rule** — no `unwrap()` / `expect()` / `panic!()` / `unreachable!()` in
  `kulyrax-core` library code. Return explicit error types.
- **Thin CLI Pattern** — `kulyrax-cli` is argument parsing (clap) + rendering only; every
  engine call goes through `kulyrax_core::pipeline`.
- **Test Coverage** — every engine feature is backed by a unit or integration test under
  `crates/core/tests/` (fixtures in `tests/fixtures/`, shared helpers in `tests/common/`).
- **No behaviour change without a phase** — the four quality gates below must be clean at
  every phase boundary.

## Common Developer Commands

```
cargo build --workspace                              # build everything
cargo test --workspace                               # all unit + integration tests
cargo test -p kulyrax-core --test sql_idempotency    # one integration suite
cargo clippy --workspace --all-targets -- -D warnings # strict lint
cargo fmt --check                                    # formatting
cargo run -p kulyrax-cli -- run --config <c> --mappings <m> --once   # drain sources once
cargo run -p kulyrax-cli -- sources --config <c>     # list sources + committed positions
cargo run -p kulyrax-cli -- reload --config <c>      # running run/ingest re-reads config, routing, mappings
cargo run -p kulyrax-cli -- ingest --config <c> --mappings <m> --listen tcp://0.0.0.0:50051
```

The networked adapter crates (`tokio`, database drivers) make up most of the build;
`cargo test -p kulyrax-core` alone stays light. `kulyrax-ingest` needs a
`protoc` (vendored via `protoc-bin-vendored` in `build.rs` — no system install).

There is no `validate` subcommand — validation runs inside `run` / `dry-run` / `replay`.
