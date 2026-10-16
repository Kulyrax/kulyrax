# Concepts

The mental model behind Kulyrax, in the order you need it. If you haven't
run anything yet, start with the [README quickstart](../README.md) first —
this doc explains *why* it worked.

## The event envelope

Every event Kulyrax consumes — from a file source, stdin, or a gRPC
producer — is the same shape:

```json
{
  "event_id": "e-42",
  "event_type": "UserRegistered",
  "payload": "{\"id\":\"u1\",\"name\":\"Ada\"}",
  "metadata": {},
  "version": 1,
  "sequence": 1
}
```

- **`event_type`** is the only thing routing looks at. `routing.json` maps
  it to a list of adapter ids; each adapter's mapping directory then has (at
  most) one mapping file per event type. There is no content-based or
  metadata-based routing — this is deliberate (see the [README](../README.md#core-principles)).
- **`payload`** is opaque JSON, parsed by each mapping's `payload.<field>` /
  `metadata.<field>` path expressions (see
  [`docs/mapping-reference.md`](mapping-reference.md)).
- **`version`** is the payload's *schema* version, for the upcaster chain —
  unrelated to `sequence` below. An entity can be on schema `version 3`
  while its guard lane is at `sequence 47`.

## `sequence` vs `position` — two different numbers

These are easy to conflate; Kulyrax treats them completely differently.

**`sequence`** is per-entity and load-bearing. It is what `decide()` (below)
compares against the guard's `last_sequence` to accept or reject a write.
It must strictly increase for successive events about the *same* entity —
a per-aggregate version counter, or a global log offset, both work, as long
as one entity's own events never go backwards.

**`position`** is per-feed and opaque to Kulyrax. It is what a source (or a
gRPC producer) resumes from after a restart or disconnect — a file offset, a
directory-source checkpoint, a Kafka partition-offset map, an outbox row id.
Kulyrax never interprets it, only stores and echoes back the highest
committed one.

**The outbox example.** A transactional outbox table assigns each row a
global auto-increment id — that's a natural `position` (resume: `SELECT ...
WHERE id > last_committed_position`). But two different entities' rows share
that same counter, so it says nothing about per-entity ordering. Each row
still needs its own `sequence` — typically a per-aggregate version column
maintained alongside the write that created the outbox row — for `decide()`
to gate on. Mixing them up (feeding the outbox row id in as `sequence`)
breaks gating the moment two entities interleave in the outbox.

## The guard

The guard is Kulyrax's idempotency and ordering gate — a small persisted
state per `(target, entity_key, facet)` lane:

```rust
struct GuardState {
    last_sequence: u64,
    deleted: bool,
    permanent: bool,
}
```

- **SQL** (SQLite, Postgres, MySQL): one row per lane in a table Kulyrax
  owns, `conduit_projection_state (target_table, entity_key, facet,
  last_sequence, last_event_id, deleted, permanent, processed_at)`,
  primary-keyed on the first three columns — a *flat* guard, one row per
  lane, because a relational table naturally works that way.
- **Document / key-value** (file-backed, MongoDB / Redis): a *nested* guard —
  one record per entity, with named-facet lanes nested inside it (a JSON
  sidecar per entity for the file adapters, e.g.
  `.conduit/entities/<collection>/<entity_id>.done`; a guard document/key of
  the same shape for MongoDB/Redis).
- **Graph** (file-backed, Neo4j): nodes use the same nested shape as
  documents; edges and the file adapter's guard sidecars are flat, one guard
  record per lane. Neo4j's own guard is a dedicated `__ConduitGuard` node —
  flat, and for a structural reason, not a preference: Cypher can only
  compare-and-set a *direct* node property, not a value nested inside one, so
  the guard has to live as its own node to make the atomicity contract work.

Which shape a given backend uses is a storage-engine decision, not something
a mapping author configures — see
[`docs/writing-a-backend.md`](writing-a-backend.md#the-guard-a-model-not-a-requirement)
if you're implementing a new one.

It is a **derived cache**, not source data: delete it and Kulyrax rebuilds
identical state by re-processing every event for that entity from the start
of the log. It is never removed once written — a tombstone (`deleted: true`)
still occupies its lane so a later redelivery is recognized as stale rather
than treated as new.

`decide()` (`adapter/mod.rs`) is the one pure function every adapter calls
with `(operation, on_existing, guard_state, event.sequence)` and gets back
what to do:

| decision | meaning |
|---|---|
| `Insert` | no guard row yet (or a resurrection after a stale-superseding delete) — create it |
| `Update` | `on_existing: replace`, sequence is newer — overwrite |
| `Delete` | tombstone the entity |
| `SkipIdempotent` | `on_existing: ignore` (default) and the entity already exists — first write wins, this one is a no-op |
| `SkipStale` | `event.sequence` is not newer than `last_sequence` — a redelivery |
| `SkipAlreadyDeleted` | a newer delete on an already-tombstoned entity — `last_sequence` still advances, nothing else changes |
| `SkipTombstoned` | the entity's tombstone is `permanent: true` — nothing ever resurrects it |

Every non-error outcome (`Created`/`Updated`/`Deleted`/`Skipped(reason)`)
shows up in the execution report, so a "nothing happened" run is always
explainable, not silent.

## Facets

A mapping's default target is the **whole entity** — one row, one document,
one KV value, one graph node. **Facets** let several event types
independently own *part* of that entity, each gated on its own sequence
lane, without clobbering each other.

Worked example: a `users` table with three mappings —

```yaml
# user_registered.yaml — default facet, creates the row
event: UserRegistered
table: users
primary_key: id
version: 1
columns: { id: payload.id, name: payload.name }
# on_existing: ignore (default) — a faceted entity's create mapping must stay ignore

# profile_updated.yaml — named facet "profile"
event: ProfileUpdated
table: users
primary_key: id
version: 1
facet: profile
columns: { id: payload.id, bio: payload.bio }

# stats_recomputed.yaml — named facet "stats"
event: StatsRecomputed
table: users
primary_key: id
version: 1
facet: stats
columns: { id: payload.id, followers: payload.followers }
```

Each facet gates independently: `ProfileUpdated` events and `StatsRecomputed`
events for the same user can arrive in any interleaving, out of order with
respect to each other, and each facet's own guard lane only compares against
that facet's own prior sequence — a stale `StatsRecomputed` redelivery can't
skip a legitimate later `ProfileUpdated`. A facet update can only apply to an
entity that already exists in its default facet (`EntityAbsent` otherwise) —
only the default-facet create mapping can bring a row into existence or
resurrect a deleted one.

## What Kulyrax creates vs. what you create

**You create:** the read-model tables/collections/namespaces/graphs your
mappings write into (`users`, `orders`, ...) — Kulyrax never issues `CREATE
TABLE`. You own that schema.

**Kulyrax creates:** the guard state alongside it — `conduit_projection_state`
in the same SQL database (via `ensure_guard_table`, called automatically),
or the `.conduit/` sidecar tree next to file-backed document/KV/graph output.
Both are Kulyrax's own bookkeeping, safe to inspect, unsafe to hand-edit —
treat them the way you'd treat a database's own internal indexes.

## Next

- [`docs/mapping-reference.md`](mapping-reference.md) — every mapping field,
  per adapter, with examples.
- [`producer-contract.md`](producer-contract.md) — the full set of
  ordering/delivery assumptions a producer must satisfy.
- [`architecture.md`](../architecture.md) — the three-part structure
  (engine / sources / adapters) and dependency rules.
