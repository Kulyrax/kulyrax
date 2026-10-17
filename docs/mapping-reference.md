# Mapping reference

Every field a mapping file can declare, per storage kind. See
[`docs/concepts.md`](concepts.md) for the concepts these fields implement
(`decide()`, guard lanes, facets) before reading this as a lookup table.

All four kinds share the same vocabulary for `version`, `operation`,
`on_existing`, `permanent`, `facet`, and `requires_capabilities` — only the
identity/body fields differ (`table`+`primary_key`+`columns` vs.
`collection`+`id`+`document` vs. `namespace`+`key`+`value` vs. the graph
`kind`-tagged node/edge fields).

Mapping files live directly in `mappings/<kind>/` (`sql`, `document`,
`keyvalue`, `graph`) and must end in `.yaml`. Files with any other extension,
including `.yml`, and subdirectories are ignored. An event that routing sends
to an adapter but that has no loaded mapping fails validation at startup with
`Missing<Kind>Mapping`, so a misnamed file stops the run rather than
projecting nothing.

## Shared fields

| field | required | default | meaning |
|---|---|---|---|
| `event` | yes | — | the `event_type` this mapping matches. One mapping file per event type per adapter. |
| `version` | yes | — | target schema version this mapping projects into (≥ 1). Feeds the upcaster chain when an incoming event's `version` differs. |
| `operation` | no | `upsert` | `upsert` (create/update) or `delete` (tombstone). A `delete` mapping's body must resolve only the identity — no columns/document/value/properties beyond what identity needs. |
| `on_existing` | no | `ignore` | `upsert`-only. `ignore`: first write for an entity wins, later ones are `SkipIdempotent`. `replace`: sequence-gated full overwrite. |
| `permanent` | no | `false` | `delete`-only. Marks the tombstone permanent — every later event for this key is rejected forever, no resurrection. |
| `facet` | no | none (default facet) | names an independent partial-update lane on the same entity (see [Facets](concepts.md#facets)). Omit for the mapping that owns the whole entity. |
| `requires_capabilities` | no | `[]` | list of `write` / `idempotent` / `upsert` / `transactions` / `delete` — validated at startup against the adapter this mapping routes to; a missing capability fails config validation, not a runtime write. |

## SQL (`mappings/sql/*.yaml`)

| field | required | meaning |
|---|---|---|
| `table` | yes | target table name. Kulyrax does not create it — you own the schema. |
| `primary_key` | yes | a column name, or a YAML list of column names for a composite key. Every listed column must also be a key in `columns`. |
| `columns` | yes | map of `column: path`. `payload.<field>` / `metadata.<field>` path expressions resolve each column's value from the event. |

```yaml
event: UserRegistered
table: users
primary_key: id
version: 1
columns:
  id: payload.id
  name: payload.name
```

Composite key and a full-replace update:

```yaml
event: OrderLineUpdated
table: order_lines
primary_key: [order_id, line_no]
version: 1
on_existing: replace
columns:
  order_id: payload.order_id
  line_no: payload.line_no
  qty: payload.qty
```

Delete — body is exactly the primary key:

```yaml
event: OrderCancelled
table: orders
primary_key: id
version: 1
operation: delete
permanent: true
columns:
  id: payload.id
```

## Document (`mappings/document/*.yaml`)

The identical mapping file routes to either document backend — `file`
(file-backed) or `mongodb` — chosen by which adapter
`routing.json` points the event type at. Nothing in the mapping itself names
a backend; on MongoDB, `document` becomes the document's fields directly
(the projected value *is* the Mongo document, keyed by `_id: <id>`) and a
named `facet` becomes a server-side `$set` instead of a read-merge-write.

| field | required | meaning |
|---|---|---|
| `collection` | yes | output subdirectory under the document adapter's root (file), or the target MongoDB collection name (MongoDB). |
| `id` | yes | path expression resolving the entity's identity (must be a JSON scalar). |
| `document` | yes for `upsert` | a JSON-like template; leaf values are `payload.`/`metadata.` paths, resolved recursively. Must be empty (`null`/`{}`/`[]`) for `operation: delete`. |

```yaml
event: UserRegistered
collection: users
id: payload.id
version: 1
document:
  id: payload.id
  name: payload.name
  address:
    city: payload.city
```

Named facet — shallow-merges only its own top-level keys into the existing document:

```yaml
event: ProfileUpdated
collection: users
id: payload.id
version: 1
facet: profile
document:
  bio: payload.bio
```

## Key-value (`mappings/keyvalue/*.yaml`)

The identical mapping file routes to either KV backend — `keyvalue` (file-backed)
or `redis` — chosen by which adapter `routing.json` points the event
type at. Nothing in the mapping itself names a backend.

| field | required | meaning |
|---|---|---|
| `namespace` | yes | keyspace segment — the KV analog of a SQL `table` / document `collection`. On Redis this is the value key's prefix (`{namespace}:{key}`); the guard lives at `__kulyrax:guard:{namespace}:{key}`. |
| `key` | yes | path expression resolving the entity's identity. |
| `value` | yes for `upsert` | a JSON-like template, same leaf-path rules as `document`. Empty for `operation: delete`. |

```yaml
event: SessionStarted
namespace: sessions
key: payload.session_id
version: 1
value:
  user_id: payload.user_id
  started_at: payload.started_at
```

## Graph (`mappings/graph/*.yaml`)

A graph mapping is a `kind`-tagged union: exactly one of `node` or `edge`
per mapping file. The identical mapping file routes to either graph backend
— `graph` (file-backed) or `neo4j` — chosen by which adapter
`routing.json` points the event type at. On Neo4j, `label`/`edge_type`
become the node label / relationship type directly, `properties` become the
record's properties, and a node's `key` (or an edge's resolved identity)
becomes its `id` property.

**`kind: node`** — a node *is* an entity, so facets apply the same as SQL/document/KV:

| field | required | meaning |
|---|---|---|
| `label` | yes | node label. |
| `key` | yes | path expression resolving the node id (scalar). |
| `properties` | yes for `upsert` | JSON-like template, same leaf-path rules. |

```yaml
kind: node
event: UserRegistered
label: User
key: payload.id
version: 1
properties:
  name: payload.name
```

**`kind: edge`** — identity is `[edge_type, from, to]` plus an optional
`discriminator` for parallel edges (a multigraph). Edges are not faceted.

| field | required | meaning |
|---|---|---|
| `edge_type` | yes | edge type/label. |
| `from` | yes | path expression resolving the source node id. |
| `to` | yes | path expression resolving the target node id. |
| `discriminator` | no | extra key component distinguishing parallel edges between the same ordered pair. |
| `properties` | no | JSON-like template for edge properties. |

```yaml
kind: edge
event: UserFollowed
edge_type: FOLLOWS
from: payload.follower_id
to: payload.followee_id
version: 1
properties:
  since: payload.followed_at
```

## Next

- [`docs/concepts.md`](concepts.md) for `decide()`, the guard, and facets.
- [`examples/`](../examples/) for complete, runnable configs using several
  of these fields together.
