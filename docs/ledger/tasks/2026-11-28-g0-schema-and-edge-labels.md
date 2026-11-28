---
status: in-progress
---

# G0: the Postgres column-type lookup ignores the schema; Neo4j edge endpoints ignore labels (F-01b)

## Objective

Runbook G0 (the owner's deferred item) and the owner's instruction of
2026-11-28:
- reproduce the Postgres schema bug (found in B2) and F-01b (task
  [2026-11-25-neo4j-edge-endpoints](2026-11-25-neo4j-edge-endpoints.md));
- report the impact: could released versions have written wrong data?
- **STOP**: no fix before the owner answers.

The owner answered (2026-11-28), quoted:

> G0 decisions:
> 1. Option A, for ALL graph edge mappings: from_label and to_label required
>    (file graph store and Neo4j), refused at validation if missing (exit 2).
>    Check whether the file graph store has the same label-blind edge bug
>    (incident files keyed by id only); reproduce first, then fix both. An edge
>    arriving before its nodes creates labelled placeholders that the real
>    nodes then merge into; test that on both stores.
> 2. Postgres: resolve tables with to_regclass (search_path and schema.table
>    both work); make `table: app.users` work, with a test.
> 3. Fix the edge-without-properties failure too, with a test matching the
>    docs' own subgraph example.
> 4. No advisory, no 0.12.3. RELEASES/v1.0.0-rc.1.md opens with a "Data
>    correctness: check your data" section: affected versions and conditions
>    per bug, and the repair (Postgres: kulyrax rebuild the affected tables;
>    Neo4j: clear and replay). Draft a short notice for the v0.12.1 and v0.12.2
>    GitHub release pages and the README's known issues, pointing there;
>    publish the release-page notices only after I say so.
> 5. SECURITY.md wording approved as written.

and chose "labels in identity" for the edge key: decision
[2026-11-28-edge-labels](../decisions/2026-11-28-edge-labels.md).

## Acceptance criteria

- [x] A failing test for the Postgres lookup: same table name in two
  schemas, different column types (`pg_column_types.rs`).
- [x] A failing test for each F-01b case against Neo4j: an edge before its
  nodes, and an edge meeting another label's node of the same id
  (`neo4j_edge_labels.rs`).
- [x] Impact on released versions, each claim marked as run or read
  (below).
- [x] The owner's answer (above).
- [x] The file graph store's label-blind cascade reproduced before the fix
  (below).
- [ ] Fixes, each with its test green: `from_label`/`to_label` required on
  both stores (missing → exit 2); labelled placeholders merged on both
  stores; Postgres `to_regclass` with `table: app.users`; an edge without
  `properties` (the docs' subgraph example) on both stores; earlier graph
  stores with edges refused (exit 4), nodes-only re-stamped, on both.
- [ ] rc.1 notes open with "Data correctness: check your data"; README known
  issues; release-page notices drafted, not published.
- [ ] Gates and Docker suites green; PR; merged with all required checks
  green.

## Constraints and assumptions

- The reproduction commit was tests only, pushed with no PR (it failed by
  design). The fixes follow on the same branch.
- The release-page notices are published only on the owner's word; the draft
  is outside the repository.

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

| File graph store, another label's node (recorded afterwards) | `cargo test -p kulyrax-core --test graph_edge_labels`, before the fix: `User u1`, `User u2`, `Org u1`, edge `u1 → u2`, then delete `Org u1` | FOLLOWS edge files before the delete 1, **after 0**: the cascade read `.kulyrax/incident/u1.json`, keyed by id only. Silent. Same code in v0.9.0, v0.12.1, v0.12.2 (`git show <tag>:…/graph/store.rs`, `incident_path(node_id)`); `node_is_tombstoned` checked every label's guard too, so an edge to `User u1` was skipped while `Org u1` was deleted (read, not run) |
| File graph store, edge without `properties` | `git show upstream/main:…/graph/store.rs` `write_edge` | properties resolved to `null` and were written as the edge file's content: no failure, a `null` edge (read, not run) |

| After the fixes: core | `cargo test -p kulyrax-core --no-fail-fast` | 0 failures; `graph_edge_labels` 6 passed: another label's delete keeps the edge, the endpoint's own delete cascades; an edge before its nodes is indexed under `incident/User/`, then owned by the real nodes; the docs' subgraph example writes the edge as `{}`; a mapping without `from_label` / `to_label` does not parse; a marker-2 graph store with an edge is refused (`AdapterError::Format`, marker left at 2), with nodes only re-stamped to 3 |
| After the fixes: Docker suites | scratchpad `docker-suites.sh`, results `suite-runs/20261128-234418` | postgres 44, mysql 36, redis 28, mongodb 31, elasticsearch 20, neo4j 35, source-postgres 18, ingest 17, cli 41 passed; 0 failed, 0 skipped. Ran: `pg_column_types` (both creation orders, `table: <schema>.seen`), `neo4j_edge_labels` (placeholders merged: 2 `User` nodes with their names, the edge between them, 0 unlabelled; `Org u1` untouched and its delete keeps the edge; the docs' subgraph example), `neo4j_format` (0.12 or format-1 database with an edge guard refused, marker 1 kept; format 1 nodes-only moves to 2; newer `3` refused) |
| After the fixes: CLI exit codes | `cargo test -p kulyrax-cli --test exit_codes` | 9 passed: an edge mapping without `from_label`, without `to_label` (parse, names the file) or with an empty one (validation, names the event) exits 2; `to_label: __KulyraxGuard` exits 2 as reserved |
| Gates | scratchpad `e1-gates.sh` (log `g0-gates.log`) | fmt, clippy `-D warnings`, clippy `unstable-pb`, rustdoc `-D warnings`: ok; workspace tests 169 binaries, 672 passed, 0 failed |
| Allowlist checks | CI's old-name and YAML-parser greps, run locally; `iconv` on every changed file | clean |

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

## Decisions

Decided by the owner (Objective): A for every graph backend, labels in the
edge's identity ([2026-11-28-edge-labels](../decisions/2026-11-28-edge-labels.md));
Postgres as below; no advisory, the rc.1 notes instead. The options as put to
the owner:

- **F-01b: how an edge names its endpoints' labels.** The edge mapping had no
  endpoint labels. Options:
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
