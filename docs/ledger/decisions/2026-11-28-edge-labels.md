---
status: accepted
---

# Edge endpoints name their labels, and the labels are part of an edge's identity (F-01b, G0)

Task: [2026-11-28-g0-schema-and-edge-labels](../tasks/2026-11-28-g0-schema-and-edge-labels.md)

## Context

A graph edge mapping names its endpoints by id only (`from`, `to`). Nodes are
per label. Reproduced (task Verification):
- **Neo4j:** an edge `MERGE`s its endpoints without a label, so it attaches
  to every node with that id, whatever the label. An edge before its nodes
  lands on label-less placeholders that the nodes never join.
- **File graph store:** the incidence index a node delete cascades through is
  keyed by id only, so deleting `Org u1` deleted `User u1`'s edge.
- **Both:** the edge identity, `[edge_type, from, to(, discriminator)]`,
  makes `User u1 → u2` and `Org u1 → u2` of one type the same edge.

## Options considered

The owner was asked on 2026-11-28 (the impact report):
- **A:** `from_label`/`to_label` on edge mappings;
- **B:** infer the labels from node mappings;
- **C:** `MATCH` endpoints instead of `MERGE`.

Then, on how far A goes:
- labels in the edge's identity (a graph format change);
- labels only to resolve endpoints, with no format change and the collision
  above left as a limit.

## Evidence

- The reproductions in `neo4j_edge_labels.rs` and `graph_edge_labels.rs`.
- `GraphEdgeMapping` has no endpoint labels.
- `canonical_edge_key(edge_type, from, to, discriminator)`.
- The file store's `.kulyrax/incident/<id>.json`.

## Decision

The owner, 2026-11-28 (quoted):

> 1. Option A, for ALL graph edge mappings: from_label and to_label required
>    (file graph store and Neo4j), refused at validation if missing (exit 2).
>    Check whether the file graph store has the same label-blind edge bug
>    (incident files keyed by id only); reproduce first, then fix both. An edge
>    arriving before its nodes creates labelled placeholders that the real
>    nodes then merge into; test that on both stores.
> 3. Fix the edge-without-properties failure too, with a test matching the
>    docs' own subgraph example.

Then, asked whether the labels join the edge's identity: **"Labels in
identity"**.

So:
- **Mappings:** `kind: edge` (and edge records of a subgraph) require
  `from_label` and `to_label`. A mapping without them is refused when the
  mappings load (exit 2). They obey the label name rules and the reserved
  label prefixes.
- **Identity:** `[edge_type, from_label, from, to_label, to(,
  discriminator)]`.
- **Endpoints:**
  - Neo4j `MERGE`s `(a:<from_label> {id})` and `(b:<to_label> {id})`. An edge
    before its nodes creates labelled placeholders, which the node writes
    `MERGE` into.
  - The file store's incidence index is per `(label, id)`.
  - The "may not attach to a tombstoned endpoint" check is per label.
- **Formats:** the graph format goes up, because edge guards are keyed
  differently.
  - File graph store: its layout marker `graph` goes from `2` to `3`.
  - Neo4j: `__KulyraxFormat {component: 'guards'}` goes from `1` to `2`.
  - A store at the old version **with no edge guards** is re-stamped in
    place; one **with edge guards** is refused (exit 4) with "clear the store
    and replay the full history". Old keys cannot be re-keyed: the labels
    were never stored. That is the repair the rc.1 notes give anyway.
- **An edge mapping without `properties`** writes an edge with no properties.

## Trade-offs accepted

- Every existing edge mapping must add two keys.
- An existing graph store with edges must be rebuilt by clearing it and
  replaying: no graph rebuild command exists.
- `GraphBackend` changes: `write_edge` / `delete_edge` take the endpoints'
  labels, and `node_is_tombstoned` takes a label. It is a breaking change for
  an out-of-tree graph backend, in the rc.1 notes.

## Revisit if

- A graph rebuild command is added (it could replace "clear and replay").
