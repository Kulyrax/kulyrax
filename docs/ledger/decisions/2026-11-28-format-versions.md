---
status: proposed
---

# Format versions: where each persisted format records its version (step 5, E1)

Task: [2026-11-28-format-versions](../tasks/2026-11-28-format-versions.md)

## Context

Runbook E1: a format version in every persisted format. A newer version is
refused with exit 4 and a clear error. An older one is migrated or refused,
never misread. The config keeps `version: 1` required.

What Kulyrax persists today, read in source on main `e99a19c`:

| Format | Where | Version today | Risk if a newer binary wrote it |
|---|---|---|---|
| Source checkpoint | `{state_dir}/{id}.json`: `{position, committed_at}` (`source/checkpoint.rs`) | none | read as-is; an added field is dropped |
| Outbox state | JSON inside the checkpoint's position (`sources/postgres`, `struct State`) | none | an added field is dropped and lost on the next commit |
| File-store batch log | `.kulyrax/wal/<seq>.log`, magic `KLXWAL01` (`file_store.rs:41`) | the magic, implicitly | a different magic on the **newest** log is taken as torn and **deleted** (`replay_logs`, `file_store.rs:760`) |
| File-store layout marker | `.kulyrax/layout/<kind>` | content written, **never read**. New stores get `1\n` (`file_store.rs:903`); stores migrated from `.conduit/` get `2\n` (`LAYOUT_VERSION`, `legacy.rs:191`) | none detected |
| File-store guard sidecars, graph incidence index | `.kulyrax/{kv,entities,guard,incident}/…` JSON | none; every guard field is `#[serde(default)]` | fields it does not know are dropped and lost on rewrite |
| SQL guard table | `kulyrax_projection_state` (SQLite, Postgres, MySQL) | none | a column it does not know is ignored |
| Redis guards | `__kulyrax:guard:{ns}:{key}` JSON | none | as file sidecars |
| MongoDB guards | `kulyrax_projection_state` collection, `ProjectionGuard` as BSON | none | as file sidecars |
| Elasticsearch guards | `__kulyrax_guard` field inside every projected document | none | as file sidecars |
| Neo4j guards | `:__KulyraxGuard` nodes | none | an unknown property is ignored |

Also order: `FileStore::prepare` replays batch logs **before** `check_layout`
runs. A future layout check placed there would come after an older binary has
already applied a newer store's logs.

Not persisted state that Kulyrax reads back:
- DLQ files: written, never read. Their content is the input event JSON.
- `reload` control files: transient, matched by token.
- Rebuild shadow tables: transient, dropped or swapped.
- gRPC ingest: no state on disk; producers hold their positions.

## Options considered

**A. One format marker per store, plus a field in single-file formats.**
- Each store (a file-store root, a SQL database or schema, a Redis db, a Mongo
  database, an ES cluster, a Neo4j database) holds one marker naming its format
  version. The adapter checks it in `prepare`, before reading or replaying
  anything.
- Single-file formats carry the version inside the file:
  - the checkpoint gets `"format": 1`;
  - the outbox `State` gets its own `"format": 1`, since its shape is the
    outbox source's own;
  - the batch log keeps the version in its magic (`KLXWAL01` is 1).
- Pros: one check per start, no per-record cost, and the guard write path stays
  unchanged (`decide()` untouched).
- Con: each networked backend gains one small object.

**B. A version field in every guard record.**
- Pros: no extra object; each record describes itself.
- Cons: every guard read and write changes, in seven implementations. A store
  could hold mixed versions mid-migration. The refusal comes only when an old
  record is touched, not at start.

## Evidence

- Every guard struct deserializes leniently (`#[serde(default)]`, unknown
  fields ignored): `document/exec.rs:31-49`, and the KV and graph guards
  likewise. A per-record version would still need every reader to check it.
  A store marker checked at start covers every record with one read.
- Batch-log deletion of an unrecognised newest log: `file_store.rs:752-760`.
- Marker inconsistency: `file_store.rs:903` writes `1\n`; `legacy.rs:191`
  writes `LAYOUT_VERSION` (`2\n`). `f3af04b` (the rename) introduced the `2`.
  0.12.1 wrote `1\n` under `.conduit/` (worktree `kx-0121`,
  `file_store.rs:920`).

## Decision

Proposed: **A.** The details for the owner:

1. **Numbering.** Every format starts at `1` = what 1.0.0-rc.1 writes. A store
   or file with no version is a 0.12.x (or dev-build) one: read as format 1 and
   stamped on first write. No migration is needed, because nothing changed
   shape since 0.12.1 apart from the rename, which step 2 already migrates.
   - File stores: the layout marker becomes the store's format marker. It is
     read and compared. Its numbering continues the layout's: `2` (0.12.1's
     `.conduit/` marker was `1`).
   - A `1` under `.kulyrax/` was written only by unreleased builds since
     `f3af04b`, so it reads as `2`.
2. **Markers in networked backends** (each written under the lock the backend
   already takes for its guard setup):
   - SQL (SQLite, Postgres, MySQL): a `kulyrax_format (component, version)`
     table beside `kulyrax_projection_state`.
   - MongoDB: one document `{_id: "guards", version}` in a `kulyrax_format`
     collection.
   - Redis: one key, `__kulyrax:format`.
   - Elasticsearch: `_meta.kulyrax_format` in each target index's mapping (the
     guards live in those indexes; there is no store-wide place).
   - Neo4j: one `(:__KulyraxFormat {component, version})` node, with a
     uniqueness constraint.
3. **Newer → refused.** One typed error in core (`FormatError::Newer { what,
   found, supported }`), carried by `SourceError` and `AdapterError`. The CLI
   maps it to exit 4. The message names the file or store and both versions.
4. **Order.** The file-store marker is checked before batch logs are replayed.
   A batch log whose magic is `KLXWAL` plus a higher number is refused, never
   deleted, whatever its position.
5. **Out of scope** (documented in `stability.md`, not versioned): DLQ files,
   `reload` control files, rebuild shadow tables.

## Trade-offs accepted

- A new small object in each networked backend: one table, collection, key,
  mapping field or node. Users who grant Kulyrax narrow DDL rights need that
  one more create right. The SQL guard table already needs it.
- Elasticsearch is per index, not per store: a newer binary's index is refused
  only when a mapping targets it. This is acceptable, because an index no
  mapping names is never read.

## Revisit if

- A format must change per record while old records stay readable (then a
  per-record version, option B, for that format).
- A backend's users cannot create the marker object.
