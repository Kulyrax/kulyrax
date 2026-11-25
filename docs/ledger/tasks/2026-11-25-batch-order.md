---
status: in-progress
---

# Review F-01: the batch sort by `sequence` versus delivery order (runbook R1)

## Objective

Establish what order the engine promises, show whether the run loop's (and
replay's) sort by `event.sequence` breaks it, and decide what to do. Owner's
runbook R1: test with two entities whose sequences interleave differently from
delivery order, including a graph edge/node case and an outbox case; then a
decision record and STOP.

Decision: [2026-11-25-batch-order](../decisions/2026-11-25-batch-order.md)
(accepted by the owner, 2026-11-25).

## Acceptance criteria

- [x] The promised order stated from the documents: `sequence` is per entity
  (`producer-contract.md` §1); per-entity delivery in order (§3); nodes before
  edges (§7); no order across sources (§4).
- [x] A reproduction through the real binary: Neo4j graph, Postgres foreign key,
  Postgres outbox; plus a no-reorder control.
- [x] Decision record; approved by the owner (option B).
- [x] The sort removed from `source/runner.rs` and `replay.rs` (dispatch in delivery
  order); `decide()`, `dispatch.rs`, the dependency graph unchanged.
- [x] Producer contract §3 rewritten; RELEASES/v0.12.2.md "Behaviour changes".
- [x] The F-01 tests pass in one batch; the two tests that assumed the sort
  updated to the documented outcome.
- [ ] Four gates, every Docker suite, PR, merge.

## Verification

| Check | Command | Result |
|---|---|---|
| Where the sort is | `grep -n sort_by` in `crates/core/src` | `source/runner.rs:432` (`sorted.sort_by_key(|se| se.event.sequence)`), `replay.rs:410` (`buffered.sort_by_key(|e| e.sequence)`) |
| The tests | `cargo test -p kulyrax-cli --test ordering -- --test-threads=1` against local Neo4j and Postgres | first run: 1 passed, 2 failed, as expected. Neo4j edge after its nodes, one batch: `(0, 1, 2)`, no edge between the labelled nodes and u2 doubled. Postgres child after its parent: projected, but only on retry 1 (the foreign key broke on the sorted order). Outbox (global sequence): passed |
| Control | the Neo4j case with `--max-batch 1` (the test first stopped before it; changed to run every case before asserting) | `(1, 1, 1)`: with no reordering possible the same input is right, so the sort is the cause |
| Edge matched another label's node | `MATCH (n {id: 'u1'}) RETURN labels(n), count(*)` on the shared Neo4j | several `Upgrade…` nodes with id u1 from the step 2 tests; the edge attached to one (F-01b in the decision record) |
| After the fix: ordering tests | the same command | 3 of 3 pass: Neo4j one batch (1, 1, 1); Postgres child row with `retries 0`; outbox (1, 1, 1) |
| After the fix: core suite, first run | `cargo test -p kulyrax-core --no-fail-fast` | 312 passed, 1 failed: `facet_before_create_replay` asserted that replay re-sorts a facet event filed before its create. That is the trade-off the decision accepts; the test now asserts the documented outcome (delivery order, the facet skipped `EntityAbsent`, the document without the facet field). `source_out_of_order_batch` still passes (under `replace` the guard leaves the highest sequence without the sort); renamed and its doc corrected |
| Gates | build; clippy `-D warnings`; `fmt --check`; `cargo test --workspace --no-fail-fast` | clean; clean; clean; 469 passed, 0 failed |
| Docker suites | `cargo test -p <backend>` against the local kx-* containers; Elasticsearch serial | Postgres 27/0, MySQL 21/0, Redis 14/0, MongoDB 17/0, Neo4j 19/0, Postgres outbox source 14/0, ingest 9/0, Elasticsearch 11/0; ordering 3/0 |
| CI coverage | `ci.yml` | the ordering tests run in the Neo4j job (graph case) and the Postgres job (foreign-key case); the outbox case needs both databases and runs locally only |

## Risks and open questions

- Behaviour change for producers that relied on within-batch repair of one
  entity's out-of-order events (`source_out_of_order_batch.rs`). It is only in the
  contract's one bullet; release notes needed.
- F-01b (Neo4j edge endpoints ignore labels): its own task, 2026-11-25-neo4j-edge-endpoints, unscheduled.
