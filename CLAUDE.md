# Kulyrax — Project Instructions

## System Overview

Kulyrax is a multi-crate Rust workspace (`kulyrax-cli`, `kulyrax-core`) implementing a
deterministic, **projection-only** engine for CQRS / Event Sourcing architectures. It
consumes an event log it does not own and projects each event into one or more storage
models (SQL, Document, Key-Value, Graph). It does not produce, buffer, or store events.

Phases 1–18 are complete — see `kulyrax/phases.md`.

## Workspace Structure

```
kulyrax/
├── Cargo.toml, Cargo.lock          — workspace root (virtual manifest, resolver = "3")
├── README.md, architecture.md, phases.md
├── phases/                         — per-phase specs, design-principles.md, producer-contract.md
├── proto/kulyrax/v1/ingest.proto   — the gRPC ingestion contract (Phase 20; the cross-language SDK)
├── RELEASES/v0.5.0-alpha.md, RELEASES/v0.6.0.md
└── crates/
    ├── kulyrax-cli/                — thin CLI (main.rs only): run, sources, ingest, explain, dry-run, replay
    ├── kulyrax-ingest/             — the tonic gRPC server + GrpcSource (keeps tonic out of core)
    ├── kulyrax-client/             — reference Rust producer client
    └── kulyrax-core/               — engine
        ├── src/
        │   ├── lib.rs              — public API & crate exports
        │   ├── event.rs            — Event envelope (event_id, event_type, payload, metadata, version, sequence)
        │   ├── routing.rs          — event_type → Vec<AdapterId> via the routing table
        │   ├── dispatch.rs         — synchronous dependency-ordered dispatcher
        │   ├── replay.rs           — catch-up replay (sequence-ordered)
        │   ├── pipeline.rs         — high-level orchestration for front ends
        │   ├── upcast.rs           — Phase 10 payload upcasters
        │   ├── adapter/
        │   │   ├── mod.rs          — StorageAdapter trait, AdapterOutcome, SkipReason, decide()
        │   │   ├── sql/            — SQLite + Postgres backends behind SqlTxn / exec.rs (Phase 19)
        │   │   ├── document/       — file-backed document adapter
        │   │   ├── keyvalue/       — file-backed key-value adapter (Phase 14)
        │   │   └── graph/          — file-backed graph adapter (Phase 16)
        │   ├── execution/          — ExecutionReport / AdapterExecutionReport, virtual time
        │   ├── source/             — Phase 17: EventSource trait, directory/stdin sources, checkpoint, runner
        │   └── runtime/            — config.rs, factory.rs, dependency_graph.rs, validation.rs, error.rs
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

## Coding Standards & Rules

- **Zero Panic Rule** — no `unwrap()` / `expect()` / `panic!()` / `unreachable!()` in
  `kulyrax-core` library code. Return explicit error types.
- **Thin CLI Pattern** — `kulyrax-cli` is argument parsing (clap) + rendering only; every
  engine call goes through `kulyrax_core::pipeline`.
- **Test Coverage** — every engine feature is backed by a unit or integration test under
  `crates/kulyrax-core/tests/` (fixtures in `tests/fixtures/`, shared helpers in `tests/common/`).
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
cargo run -p kulyrax-cli -- ingest --config <c> --mappings <m> --listen tcp://0.0.0.0:50051
```

The `postgres`/`tokio` build is heavier since Phase 19; `kulyrax-ingest` needs a
`protoc` (vendored via `protoc-bin-vendored` in `build.rs` — no system install).

There is no `validate` subcommand — validation runs inside `run` / `dry-run` / `replay`.
