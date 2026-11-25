---
status: proposed
---

# Review F-01: the batch sort by `sequence` versus delivery order (runbook R1)

## Objective

Establish what order the engine promises, show whether the run loop's (and
replay's) sort by `event.sequence` breaks it, and decide what to do. Owner's
runbook R1: test with two entities whose sequences interleave differently from
delivery order, including a graph edge/node case and an outbox case; then a
decision record and STOP.

Decision: [2026-11-25-batch-order](../decisions/2026-11-25-batch-order.md)
(proposed).

## Acceptance criteria

- [x] The promised order stated from the documents: `sequence` is per entity
  (`producer-contract.md` §1); per-entity delivery in order (§3); nodes before
  edges (§7); no order across sources (§4).
- [x] A reproduction through the real binary: Neo4j graph, Postgres foreign key,
  Postgres outbox; plus a no-reorder control.
- [x] Decision record. **STOP** for the owner.
- [ ] After approval: the fix, the contract text, the release notes, the regression
  tests passing.

## Verification

| Check | Command | Result |
|---|---|---|
| Where the sort is | `grep -n sort_by` in `crates/core/src` | `source/runner.rs:432` (`sorted.sort_by_key(|se| se.event.sequence)`), `replay.rs:410` (`buffered.sort_by_key(|e| e.sequence)`) |
| The tests | `cargo test -p kulyrax-cli --test ordering -- --test-threads=1` against local Neo4j and Postgres | first run: 1 passed, 2 failed, as expected. Neo4j edge after its nodes, one batch: `(0, 1, 2)`, no edge between the labelled nodes and u2 doubled. Postgres child after its parent: projected, but only on retry 1 (the foreign key broke on the sorted order). Outbox (global sequence): passed |
| Control | the Neo4j case with `--max-batch 1` (the test first stopped before it; changed to run every case before asserting) | `(1, 1, 1)`: with no reordering possible the same input is right, so the sort is the cause |
| Edge matched another label's node | `MATCH (n {id: 'u1'}) RETURN labels(n), count(*)` on the shared Neo4j | several `Upgrade…` nodes with id u1 from the step 2 tests; the edge attached to one (F-01b in the decision record) |

## Risks and open questions

- Behaviour change for producers that relied on within-batch repair of one
  entity's out-of-order events (`source_out_of_order_batch.rs`). It is only in the
  contract's one bullet; release notes needed.
- F-01b (Neo4j edge endpoints ignore labels) is separate and unscheduled.
