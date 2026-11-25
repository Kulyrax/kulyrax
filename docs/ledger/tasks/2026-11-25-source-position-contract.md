---
status: in-progress
---

# Review F-05: the source-position ordering contract (runbook R3)

## Objective

Write down one contract for `SourcePosition`:
- what a position is;
- how positions compare;
- who may construct them;
- what `commit()` means for positions inside a batch;
- what gRPC producers must send.

Put it in `docs/producer-contract.md` and `docs/stability.md`, and enforce it:
- every built-in source has a test for its delivery order;
- ingest refuses positions that violate the contract.

Choose between a typed or fixed-width position and a per-source comparator.
Decision record, then **STOP**.

Decision: [2026-11-25-source-position-contract](../decisions/2026-11-25-source-position-contract.md)
(accepted by the owner, 2026-11-25: A; empty positions refused from 1.0;
`Ord` removed in Phase D).

## Acceptance criteria

- [x] Survey every place positions are made or compared.
- [x] Reproduce what text order breaks.
- [x] Decision record. **STOP** for the owner.
- [x] The run loop commits at each batch's last event in delivery order.
- [x] Directory and stdin use their own order.
- [x] Tests:
  - run loop;
  - directory (space, `!`, `"`; never backward);
  - stdin (never backward; resume; bad checkpoint);
  - gRPC (numeric offsets);
  - the CLI `--once` case;
  - the empty-position warning.

  The outbox's own tests already cover its order.
- [x] Docs:
  - the contract in `producer-contract.md` §9;
  - `stability.md`, with the owner's override recorded;
  - the `SourcePosition` and `commit()` docs.
- [x] 0.12.2 release notes: behaviour change, the empty-position warning,
  `Ord` deprecation, fixes.
- [ ] Gates and every Docker suite green; CI green; merged.
- Carried to Phase D (D1/D2):
  - refuse empty gRPC positions with `InvalidArgument`;
  - remove `PartialOrd`/`Ord` from `SourcePosition`;
  - derive `Hash`.

## Verification

| Check | Command | Result |
|---|---|---|
| Where positions are compared | `grep` in `crates/` (outside tests) | `runner.rs` `max`/`min`; `directory.rs` and `stdin.rs` `<=` on commit; the outbox looks the position up exactly |
| Directory, split file then `a.ndjson<c>b.ndjson` | `cargo test -p kulyrax-core --test source_position_order` | `' '` and `'!'` fail: the second file is delivered 4 times in 6 polls, and the commit stays at `a.ndjson#000000000002`. `'-'` passes |
| The same through the binary | quickstart plus `events.ndjson 2.ndjson`, `timeout 30 kulyrax run --once --max-batch 2` | exit 124 (killed, never finished); checkpoint `events.ndjson#000000000002` |
| Fix, core | `cargo test -p kulyrax-core --test source_position_order` | 7 of 7 pass (`"` runs on Unix only). The run-loop test commits `["10"]`; on main it committed `9` |
| Fix, CLI | `cargo test -p kulyrax-cli --test source_positions` | Passes in 0.5 s. With the core changes stashed, it was killed at 60 s. First run failed before the fix was involved: the test project lacked `mappings/document` and `mappings/graph` (every mapping kind needs its directory); fixed in the test |
| Fix, gRPC | `cargo test -p kulyrax-ingest --test grpc_producers -- --test-threads=1` | 4 of 4 pass, including numeric offsets `9`, `10` acked at `10` |
| `#[deprecated]` on `Ord` | `rustc --edition 2024` on a two-impl example | Error: "`#[deprecated]` attribute cannot be used on trait impl blocks" (deny-by-default `useless_deprecated`). The doc-comment fallback is used |
| Gates, before the owner's decisions | fmt; clippy `-D warnings`; `cargo test --workspace` | clean; 481 passed, 0 failed |
| Gates, final | fmt; clippy `-D warnings`; `cargo test --workspace` | clean; 482 passed, 0 failed |
| Docker suites, final | `cargo test -p <crate> -- --test-threads=1` against local Postgres, MySQL, Redis, MongoDB, Elasticsearch, Neo4j | 0 failures: postgres 27, mysql 21, redis 14, mongodb 17, elasticsearch 11, neo4j 19, source-postgres 14, ingest 14, cli 17. Same script and environment as R2's run, whose `--nocapture` check showed no skips; not re-checked here. A first run was stopped part-way because files changed under it; its finished suites (postgres, mysql, redis) had passed |

## Risks and open questions

- Stdin positions overflow the 12-digit padding after 10^12 lines. Comparing
  parsed line numbers removes the limit.
- `pb::EventEnvelope::from(&Event)` leaves `position` empty. From 1.0 a
  producer that uses it unchanged is refused. The 0.12.2 notes say so, and D
  should give the conversion a way to set the position.
