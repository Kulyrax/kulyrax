---
status: completed
---

# `#[non_exhaustive]` and constructors (step 4, D1), plus R3's carry-overs

## Objective

Runbook D1: `#[non_exhaustive]` on every public enum and growable public
struct. `Event` gets `Event::new(event_id, event_type, sequence, payload)` and
`with_metadata` / `with_version`, and tests and examples migrate to them
(stability §9.6).

The owner also carried three items from R3 (decision
2026-11-25-source-position-contract) into D1/D2:
- refuse an empty gRPC position with `InvalidArgument`;
- remove `PartialOrd`/`Ord` from `SourcePosition`, keeping `Eq`, and derive
  `Hash`;
- `pb::EventEnvelope::from(&Event)` must set a position.

This is tranche 1 of Phase D's split (D1, the R3 items, D2).

Decision: [2026-11-26-non-exhaustive-scope](../decisions/2026-11-26-non-exhaustive-scope.md).

## Acceptance criteria

- [x] Every public enum in the published crates is `#[non_exhaustive]` (48).
- [x] The frozen public structs with public fields are `#[non_exhaustive]` (54).
  Adapter and source configs are left to D4, UNSTABLE internals to D2 (the
  decision).
- [x] Constructors for every type built outside its crate; `Event::new` with
  `with_metadata` / `with_version`.
- [x] Tests, examples and docs migrated (142 `Event` literals, the configs,
  options and contexts). The embedded example runs.
- [x] The R3 items: empty gRPC positions refused; `SourcePosition` unordered
  and `Hash`; `EventEnvelope` built from an event carries its position.
- [x] Four gates green; every Docker suite green; PR merged: #28, `47f2687` (CI 16/16).

## Verification

| Check | Command | Result |
|---|---|---|
| Candidates | public `enum`s, and public `struct`s with a public field, in `crates/{core,ingest,client,backends,sources/postgres,adapters/*}/src` | 48 enums, 81 structs |
| All 129 marked, first build | `cargo build --workspace --all-targets --keep-going` | 293 errors before the adapter crates compiled. The table is in the decision record |
| After the rule | the same | 102 marked (48 + 54) |
| `Event` literals | `event_rewrite.pl` (scratchpad): a brace- and string-aware rewriter | 142 found, 129 rewritten. 13 used `..base` and were rewritten as `let mut o = base; …`. 1 more, written `kulyrax_core::event::Event {`, was missed by the file list and found by the build |
| First build after the rewrite | the same | `E0283` on `"x".into()` passed to an `impl Into<String>` argument. **My rewrite bug**: the `.into()` added nothing. Stripped from the rewritten calls |
| Other literals | `lit_rewrite.pl`: per-type rules | 87 rewritten. By hand: a turbofish comma (`get::<_, i64>`) and a literal with a comment in it, and 6 closures where `payload` moved into `ev(..)` before `payload["id"]` was read (the id is now computed first) |
| Matches outside the crate | build | 16 `E0004`, in the adapters, ingest and the CLI. Wildcard arms fail safe (the decision, item 5) |
| clippy, first run | `cargo clippy --workspace --all-targets -- -D warnings` | `items_after_test_module` ×3: constructors appended after a `mod tests`. Moved above it. Then clean |
| Build | `cargo build --workspace --all-targets` | 0 errors, 0 warnings (`cargo fix` removed the imports the rewrite left unused) |
| Examples | `cargo build --manifest-path examples/*/Cargo.toml` | the embedded example built a `KulyraxConfig` literal, now `KulyraxConfig::new`. All build; `cargo run --manifest-path examples/embedded/Cargo.toml` projects `users.u1` |
| `SourcePosition` without `Ord` | build after removing `PartialOrd`/`Ord`, adding `Hash` | 1 error: `source_directory_split.rs` took `max()` of a batch's positions. It now takes the last, as the run loop does |
| Empty gRPC position | `cargo test -p kulyrax-ingest` | new unit test `an_empty_position_is_refused_with_invalid_argument`. New end-to-end test `an_event_with_an_empty_position_ends_its_stream_with_invalid_argument`: the earlier event is acked, the stream ends with `INVALID_ARGUMENT` naming the event, and a second producer carries on. Ingest: 3 + 7 + 5 passed |
| `EventEnvelope::from(&Event)` | `pb::EventEnvelope::from_event(&event, position)` | the round-trip unit test checks the position travels |
| Docs | `producer-contract.md` §9, the `position` comment in `ingest.proto` (comments only), `stability.md`, `writing-a-backend.md`, the rc.1 notes ("API changes", with before/after code) | updated |
| Gates and Docker suites | fmt; clippy `-D warnings`; `cargo test --workspace`; the embedded example; every Docker suite (Neo4j on 7688, `KULYRAX_V0121_BIN`) | clean; clean; 527 passed, 0 failed; projects `users.u1`; postgres 30, mysql 25, redis 14, mongodb 21, elasticsearch 11, neo4j 20, source-postgres 14, ingest 15, cli 32, all passed |
