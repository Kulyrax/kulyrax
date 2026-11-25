---
status: accepted
---

# Dispatch events in delivery order; `sequence` only gates

Task: [2026-11-25-batch-order](../tasks/2026-11-25-batch-order.md)

## Context

Review finding F-01: the run loop stable-sorts each batch by `event.sequence`
before dispatch (`source/runner.rs:432`), and replay sorts the whole history the
same way (`replay.rs:410`). The producer contract makes `sequence` **per entity**:
"Different entities have independent sequences" (`docs/producer-contract.md` §1).
A per-aggregate version is explicitly fine. Sorting a batch by numbers that are
only comparable within one entity reorders events of *different* entities
against their delivery order.

The contract also relies on cross-entity order:
- §7: a producer emits a node before an edge that points at it;
- §3: an entity's events arrive in sequence order. The run loop documents a
  "tolerates local reordering within a batch" on top of that, and that sort is
  the source of this finding.

## Options considered

- **A. Keep the sort and document it.** Cross-entity order inside a batch would
  follow `sequence`, and producers whose cross-entity order matters must use a
  global sequence (a log offset). That contradicts §1's per-aggregate allowance,
  breaks every existing per-aggregate producer's graph and foreign-key cases, and
  leaves results depending on batch size.
- **B. Dispatch in delivery order everywhere; `sequence` only gates (recommended).**
  The run loop and replay stop sorting. `decide()` already resolves per-entity
  order through the guard lanes: in `replace` mode the highest sequence wins, and
  redeliveries are skipped as stale. What is lost is the within-batch repair of
  an entity's own events arriving out of order, which the contract (§3) already
  says it does not guarantee across batches.
- **C. Per-entity stable order.** Keep delivery order across entities, and reorder
  each entity's own events into the slots they occupy. That needs every event's
  entity identity before dispatch. Identity depends on the mapping and the adapter
  (key paths, facets, subgraph records, several adapters per event), so the run
  loop would have to plan every event against every mapping first. It's a large
  change, and its result would still depend on batch boundaries.

## Evidence

All observed on 2026-11-25, through the real `kulyrax` binary against local
Docker. The tests are in `crates/cli/tests/ordering.rs`.

| Case (delivered in an order that obeys the contract) | One batch (the sort applies) | One event per batch (`--max-batch 1`, nothing reordered) |
|---|---|---|
| Neo4j: node u1 (seq 5), node u2 (seq 3), then edge u1→u2 (seq 1, the edge's own first event) | **wrong**: (0, 1, 2), no edge between the two nodes and u2 exists twice. The sort put the edge first; its `MERGE (a {id})` made label-less placeholders, and the node writes then created separate labelled nodes | right: (1, 1, 1) |
| Postgres, a foreign key: parent p1 (seq 7), then child c1 → p1 (seq 1) | the child sorted first and broke the key: the batch failed and passed only on retry 1 (`retries 1`). A chain deeper than the retry budget (3) would dead-letter or halt | (not run; the same mechanism) |
| Postgres outbox: the same graph events; `sequence` is the outbox id (global, in delivery order) | right: (1, 1, 1). Sorting by a global sequence is a no-op | — |

**The decisive point:** the same input gives different results depending on
`--max-batch`, so batch boundaries change the outcome. Kulyrax is meant to be a
deterministic projection engine. With B, the result depends only on the delivery
order.

Code and tests that depend on the sort:
- `crates/core/tests/source_out_of_order_batch.rs` asserts the within-batch repair
  (files named so one entity's events arrive 3, 1, 2);
- replay's comment calls the global sort "equivalent for the guarantee". It is
  for each entity's own order, not across entities.

## Decision

B.
- Remove the sort in `source/runner.rs` and in `replay.rs`. Events dispatch in
  the order the source delivered them (replay: sorted file names, then line
  order, as loaded).
- `decide()`, `dispatch.rs` and the dependency graph are unchanged.
- Contract §3: drop "batch dispatch tolerates local reordering within a batch",
  and state that order is delivery order, within and across batches alike.
- Replay: delivery order is file-name then line order. A history spread over
  files must be laid out in order, as for a live source.
- `source_out_of_order_batch.rs` changes from "repaired" to the documented
  outcome. The release notes call this a behaviour change, with what a producer
  must guarantee.
- The ordering tests stay as regression tests, with the delivery-order cases
  expected right in one batch too.

Ships in 0.12.2 (runbook R4).

## Trade-offs accepted

- A producer whose one entity's events arrive out of order inside one batch no
  longer has them repaired. In `ignore` mode, or for facet lanes, a lower
  sequence delivered after a higher one is skipped, as it already is across
  batches.
- Replay of a history whose files are not in order no longer reorders it.

## Revisit if

- Users depend on within-batch repair (it was undocumented outside the contract's
  one bullet): then C, as an opt-in, scoped to sources that declare per-entity
  keys.

## Also found (not part of this decision)

**F-01b, Neo4j edge endpoints ignore labels.** `MERGE (a {id: $from})` matches a
node with that id under *any* label, and creates a label-less placeholder when
none exists. So, even in delivery order:
- an edge sent before its node (which §7 calls "permissive: resolves once the
  node arrives") does not resolve in Neo4j: the node write creates a second,
  labelled node;
- an edge can attach to a different label's node that shares the id.

The second happened in this test. `u1` was not duplicated because the edge's
`MERGE (a {id: 'u1'})` matched a node from another test: the shared database
holds several nodes with id `u1` under other labels (`Upgrade…`, left by the
step 2 upgrade tests; `MATCH (n {id: 'u1'}) RETURN labels(n)`). Needs its own
reproduction and fix. The leftovers are also the test-isolation problem the
runbook's B2 fixes.
