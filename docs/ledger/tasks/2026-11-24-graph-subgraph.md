---
status: completed
---

# Graph: several records from one event (`kind: subgraph`)

## Objective

The roadmap's "subgraph-per-event": one graph mapping emitting several records,
a node plus its edges, atomically from one event. Until now a graph mapping
produced exactly one node or one edge, and a multi-record event had to be split
upstream.

## Acceptance criteria

- [x] `kind: subgraph` with `records:`, each a node or edge mapping that takes
  `event` and `version` from the subgraph (setting them on a record, or nesting
  a subgraph, is refused when the mapping is parsed).
- [x] Every record keeps its own guard lane and is decided by `decide()` on its
  own, at the event's sequence.
- [x] All of an event's records are applied in one transaction: a failing
  record leaves nothing of the event (file graph store: the per-event overlay;
  Neo4j: the event's or the batch's transaction).
- [x] One outcome per event: Created if any record was created, else Updated,
  else Deleted, else the first record's skip.
- [x] Validation checks every record (structure, facets, capabilities); an
  empty subgraph is refused.
- [x] `dry-run` lists every record's write (`DryRunOutcome::PlannedSubgraph`).
- [x] Docs (mapping reference) and release notes.

## Constraints and assumptions

- `decide()` and the node / edge write paths (`project_node`, `project_edge`)
  unchanged; the subgraph calls them per record.
- Node and edge mappings keep working unchanged: they are a subgraph of one.

## Decisions

No separate record. The choices: records reuse the node / edge mapping types
(so every field and rule is the same), resolved through
`GraphRuntimeBuilder::build_all`; `exec::project_all` runs them in order and
stops at the first error, leaving atomicity to the adapter's existing
per-event transaction; both graph adapters now share `exec::project_record`
instead of each matching on the record kind.

## Verification

| Check | Command | Result |
|---|---|---|
| File graph store | `cargo test -p kulyrax-core --test graph_subgraph` | 4 passed: one event writes the order, the customer and the edge, a guard lane each; redelivery `Skipped(StaleSequence)`; a second order reuses the customer (`on_existing: ignore`) and adds its order and edge (`Created`); a newer event updates (`Updated`); a record that cannot resolve fails the event and writes no node and no guard; parsing refuses a record's own `event` and a nested subgraph; validation names the record (`records[1]: empty label`), requires `upsert` for a replacing record, refuses an empty subgraph |
| Neo4j 5 (Docker) | `KULYRAX_NEO4J_URI=bolt://localhost:7687 cargo test -p kulyrax-adapter-neo4j --test neo4j_backend` | 12 passed, incl. `neo4j_subgraph_projects_every_record_in_one_transaction`: the three records land; redelivery skipped; a map-valued property (refused by Neo4j) fails the second record after the first wrote, and the transaction leaves neither node nor guard |
| Existing graph suites | `cargo test --workspace` (gates) | see the push |

## Risks and open questions

- Two records of one subgraph resolving to the same lane (the same node twice)
  are decided in order at one sequence: the second is skipped as stale.
  Validation cannot see it, since keys are payload paths.

## Outcome

Completed.
