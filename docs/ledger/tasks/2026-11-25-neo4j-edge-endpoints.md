---
status: proposed
---

# F-01b: Neo4j edge endpoints ignore labels

## Objective

Found while reproducing F-01 (task 2026-11-25-batch-order). The Neo4j adapter
writes an edge with `MERGE (a {id: $edge_from}) MERGE (b {id: $edge_to})`, with
no label, while nodes are written as `MERGE (n:<Label> {id: …})`. So:

- an edge written before its nodes creates label-less placeholder nodes; the
  node writes then create separate labelled nodes, and the edge never joins
  them. That contradicts producer contract §7, which calls the case "permissive:
  resolves once the node arrives";
- an edge attaches to every node with that id under *any* label (observed: an
  edge from `u1` attached to an `Upgrade…`-labelled `u1` left in the shared test
  database).

Delivery order (F-01's fix) removes the first case only when the producer
follows §7; the second happens in any order.

## Acceptance criteria

- [ ] A failing test for each case against Neo4j.
- [ ] A decision on how an edge names its endpoints' labels (the edge mapping
  has no endpoint labels today), and on what §7's "permissive" means for Neo4j.
- [ ] Fix, tests, release notes. Not scheduled; the owner decides when.

## Verification

| Check | Command | Result |
|---|---|---|
| Edge write | `crates/adapters/neo4j/src/lib.rs` | `MERGE (a {id: $edge_from}) MERGE (b {id: $edge_to}) MERGE (a)-[r:<type> {key: $edge_key}]->(b)` |
| Cross-label attach | `MATCH (n {id: 'u1'}) RETURN labels(n), count(*)` on the shared Neo4j after the F-01 test | several `Upgrade…` nodes with id u1; the F-01 test's edge attached to one |
