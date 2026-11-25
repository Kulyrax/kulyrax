---
status: proposed
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
(proposed).

## Acceptance criteria

- [x] Survey every place positions are made or compared.
- [x] Reproduce what text order breaks.
- [x] Decision record. **STOP** for the owner.
- [ ] After approval:
  - the run loop uses delivery order;
  - directory and stdin use their own order;
  - tests per built-in source, including the CLI `--once` case;
  - the contract in `producer-contract.md`, `stability.md` and the
    `SourcePosition` docs;
  - 0.12.2 release notes;
  - gates and every Docker suite.

## Verification

| Check | Command | Result |
|---|---|---|
| Where positions are compared | `grep` in `crates/` (outside tests) | `runner.rs` `max`/`min`; `directory.rs` and `stdin.rs` `<=` on commit; the outbox looks the position up exactly |
| Directory, split file then `a.ndjson<c>b.ndjson` | `cargo test -p kulyrax-core --test source_position_order` | `' '` and `'!'` fail: the second file is delivered 4 times in 6 polls, and the commit stays at `a.ndjson#000000000002`. `'-'` passes |
| The same through the binary | quickstart plus `events.ndjson 2.ndjson`, `timeout 30 kulyrax run --once --max-batch 2` | exit 124 (killed, never finished); checkpoint `events.ndjson#000000000002` |

## Risks and open questions

- Empty gRPC positions (decision, "For the owner" 2).
- Removing `Ord` from a type that `stability.md` freezes (decision, "For the
  owner" 3).
- Stdin positions overflow the 12-digit padding after 10^12 lines. Comparing
  parsed line numbers (option A) removes the limit.
