---
status: blocked
---

# G0: the Postgres column-type lookup ignores the schema; Neo4j edge endpoints ignore labels (F-01b)

## Objective

Runbook G0 (the owner's deferred item) and the owner's instruction of
2026-11-28:
- reproduce the Postgres schema bug (found in B2) and F-01b (task
  [2026-11-25-neo4j-edge-endpoints](2026-11-25-neo4j-edge-endpoints.md));
- report the impact: could released versions have written wrong data?
- **STOP**: no fix before the owner answers.

Blocked on the owner's answer to the impact report.

## Acceptance criteria

- [x] A failing test for the Postgres lookup: same table name in two
  schemas, different column types (`pg_column_types.rs`).
- [x] A failing test for each F-01b case against Neo4j: an edge before its
  nodes, and an edge meeting another label's node of the same id
  (`neo4j_edge_labels.rs`).
- [x] Impact on released versions, each claim marked as run or read
  (below).
- [ ] The owner's answer; then the fixes, tests green, rc.1 notes under
  Fixes, PR, merge.

## Constraints and assumptions

- Tests only; no fix (the owner's instruction). The branch is pushed to the
  fork with no PR, because the tests fail by design.

## Verification

| Check | Command / source | Result |
|---|---|---|
| The lookup | `crates/adapters/postgres/src/lib.rs`, `column_types` | `SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1`: no schema. Rows of every visible table of that name go into one map; later rows overwrite earlier ones |
| How types are used | `lib.rs` `bind_value`; `bulk.rs` `unnest_args` | one event at a time: driver-typed parameters, so a type mismatch is an error. Batched (`begin_batch` … `commit_batch`, the run loop's default): `INSERT … SELECT * FROM unnest($n::<type>[])`, the array type taken from the map, then Postgres's assignment cast into the real column |
| In which releases | `git log --diff-filter=A -- bulk.rs`, `git tag --contains`; `git grep` at each tag | the lookup is in v0.9.0, v0.12.1, v0.12.2. The batched path (`bulk.rs`, added in `54d9aa8`) is in **v0.12.1 and v0.12.2** only |
| Postgres reproduction | `cargo test -p kulyrax-adapter-postgres --test pg_column_types -- --test-threads=1 --nocapture`, `kx-pg`. The mapped schema (`search_path`): `seen(id text, at text)`; another: `seen(id text, at timestamptz)`; event `at = "2026-11-28T10:00:00+05:00"` | **Mapped table created first:** batched stored `"2026-11-28 05:00:00+00"`, a silently changed value with outcome `Created`; one at a time failed: `error serializing parameter 0`. **Other table created first:** both stored the value exactly. The later-created table's types win |
| Schema-qualified table | the same file, `table: <schema>.seen`, `search_path` elsewhere | fails every event: `column 'at' of mapping is not in table '<schema>.seen'` (the lookup matches `table_name` against the qualified name and finds nothing). Loud |
| The edge write | `crates/adapters/neo4j/src/lib.rs` `write_edge`; `git grep -F 'MERGE (a {{id: $edge_from}})'` at each tag | `MERGE (a {id: $edge_from}) MERGE (b {id: $edge_to}) MERGE (a)-[r:<type> {key}]->(b)`, no labels (nodes: `MERGE (n:<Label> {id})`). The same in **v0.9.0, v0.12.1, v0.12.2**. An edge mapping has no endpoint labels (`GraphEdgeMapping`: `edge_type`, `from`, `to`) |
| Neo4j first run | `cargo test -p kulyrax-adapter-neo4j --test neo4j_edge_labels -- --test-threads=1 --nocapture`, `kx-neo4j-7688` | both tests failed for another reason: the edge write failed, `Neo.ClientError.Statement.TypeError: Expected $properties to be a map, but it was NO_VALUE`. My edge mapping had no `properties`, which mapping-reference says is optional for edges (and its own subgraph example omits). A third bug; the F-01b tests now give the edge a property, and a third test pins this one |
| F-01b, edge before nodes | the same, after | the edge's outcome `Created`; **0 edges between `(:User u1)` and `(:User u2)`; 2 label-less nodes**: the edge sits on placeholders the node writes never join. Silent |
| F-01b, another label's node | the same | `(:User u1)`, `(:Org u1)`, `(:User u2)`, then the edge `u1 → u2`: **FOLLOWS from Org u1: 1, from User u1: 1**. The edge attaches to every node with that id. Silent |
| Edge without `properties` | the same, third test | the edge event fails, `$properties … NO_VALUE`; nothing written. Loud. Released code is the same (`properties` defaults to null and is passed to `SET r = $properties` in v0.9.0 and v0.12.2): **inferred from the code, not run** |

## Impact on released versions

**Postgres (G0): yes, v0.12.1 and v0.12.2 can have written wrong data
silently.**
- **Conditions, all needed:**
  - a table with the mapped table's name in another schema of the same
    database, visible to Kulyrax's user;
  - a column of the same name with a different type, and that table's rows
    winning in the lookup (here: created later);
  - the batched path (`kulyrax run` / `ingest` with sources);
  - a value valid under the wrong type, which Postgres's assignment cast
    accepts into the real column.
- **Silent cases** (the first shown above; the others follow from the same
  casts, not run):
  - into `text`: a timestamp reformatted in UTC, other types reformatted;
  - into `timestamp`: shifted by the session time zone;
  - into `date`: truncated from a timestamp;
  - into `integer`: rounded from a float, where the right outcome was an
    error.
- **The guard records the event as projected,** so it is never projected
  again: the wrong value stays.
- **Every release since v0.9.0 fails loudly** for the same mismatch one event
  at a time, and for a schema-qualified mapped table.
- **Likely exposure:** schema-per-tenant databases, staging copies beside
  production, an app schema next to `public`.
- **Repair after a fix:** `kulyrax rebuild` of the affected tables (it
  projects into shadow tables, whose names are unique, so the lookup cannot
  collide there).

**Neo4j (F-01b): yes, every release (v0.9.0 to v0.12.2) can have written
wrong data silently.**
- **Another label's node.** Any two labels that share an id value (integer
  ids, reused keys) get every edge on both, whatever the order. That is an
  edge that should not exist.
- **An edge before its nodes.** The edge sits on label-less placeholders
  that the nodes never join, so the labelled nodes miss it.
  - Before 0.12.2, the batch's sort by `sequence` (F-01) could put an edge
    first even when the producer sent the nodes first.
  - From 0.12.2, only a producer that sends the edge first.
- **Repair after a fix:** there is no graph rebuild command. Clear the Neo4j
  database and `kulyrax replay` the full history.

**Neo4j edge without `properties`: no wrong data;** every such event fails
(dead-lettered or halts the run), in every release (inferred).

## Decisions needed

- **F-01b: how an edge names its endpoints' labels.** The edge mapping has no
  endpoint labels today. Options:
  - **A (recommended):** add `from_label` and `to_label` to `kind: edge`,
    required for the Neo4j backend at validation. The edge `MERGE`s then name
    the labels. An edge before its nodes then creates labelled placeholders,
    which the node writes merge into, as §7's "permissive" promises.
    New keys, so existing mappings without them are refused on Neo4j: a
    change for users, in the rc.1 notes.
  - **B:** infer each endpoint's label from the node mappings: ambiguous when
    two node mappings share a key path.
  - **C:** `MATCH` the endpoints instead of `MERGE`: no cross-label attach,
    but an edge before its nodes then fails or is dropped (§7 would change).
- **Postgres: fix as the B2 note proposed,** with no design choice: resolve
  the table the way the `INSERT` does (`to_regclass($1)`, so the
  `search_path` and a `schema.table` name both work), and read the types of
  that table only.
- **Whether the rc.1 notes, or an advisory, should tell 0.12.1 / 0.12.2
  Postgres users and all Neo4j users to check their data.** A wrong-data bug
  in released versions, not a security one: the owner's call.
