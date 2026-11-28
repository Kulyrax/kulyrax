---
status: accepted
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

**A**, approved by the owner on 2026-11-28 with five changes. Their message,
quoted:

> E1 proposal: approved, with these changes (record them in the decision
> record, quoting this message):
> 1. Batch log with an unknown magic number: refused (exit 4), never deleted.
>    This is a data-loss fix in its own commit, with a test.
> 2. SQL format marker (kulyrax_format) lives in the same schema as the guard
>    table; test with a non-default schema.
> 3. Every marker is created insert-if-absent (atomic per backend); test
>    concurrent first starts (as the rename tests did) — one marker, one
>    version.
> 4. If the marker can't be created (permissions), refuse to start, naming the
>    store and the grant needed. Document the required grants per backend in
>    the docs (README or CONTRIBUTING → user docs).
> 5. Define what existing layout-marker values 1 and 2 mean in the new scheme;
>    test a store with each.
>
> Estimate accepted (~2 days, over the 1-day E rule). Stop and report if it
> grows past 3.

How the changes are applied:
- **Change 1, the batch log.**
  - A `.log` reaches its name only by a rename after fsync (`write_log`), so a
    foreign magic cannot be crash damage. `KLXWAL` with a version above `01`
    is refused as a newer format; any other complete 8-byte magic is refused
    as not a Kulyrax batch log. Both are refused whatever the log's position,
    and both exit 4.
  - A file too short to hold the magic, or one with the right magic but a
    short or bad-checksum body, keeps today's rule: dropped if it is the
    newest log, refused otherwise.
- **Change 2, SQL.** The marker is created unqualified, as the guard table
  is: Postgres puts it in `current_schema()`, MySQL in the connection's
  database, SQLite in the file. Tested in a non-default Postgres schema, and
  with a `kulyrax_format` of another store later on the `search_path`.
- **Change 3, SQL.** `CREATE TABLE IF NOT EXISTS` (in a savepoint on
  Postgres, as the guard table's creation race needs), then `INSERT … ON
  CONFLICT DO NOTHING` / `INSERT IGNORE` / `INSERT OR IGNORE`, then read
  back. On SQLite the schema transaction is `BEGIN IMMEDIATE`, and the WAL
  switch at open retries while busy: both were needed for concurrent first
  starts (see the task's Verification).
- **Change 4, exit code.** A marker that cannot be created is a safety refusal
  like a newer format: exit 4.
- **Redis.** The owner, 2026-11-28: "Redis E1 plan: approved. Add: document
  that Kulyrax's Redis store requires maxmemory-policy noeviction (or
  volatile-* with no TTLs on Kulyrax keys), since an evicted marker or guard
  silently loses data. In prepare, try CONFIG GET maxmemory-policy: if it's
  readable and allkeys-*, log a loud warning naming the policy (don't refuse;
  managed Redis often blocks CONFIG). Add a line to the required-grants doc."
  Then, the owner, 2026-11-28: "Redis E1, one addition: when CONFIG GET
  maxmemory-policy is blocked or fails, don't skip silently. Log one line at
  info level per adapter start: "redis adapter <id> (<host>/<db>): could not
  read maxmemory-policy; Kulyrax requires noeviction (or volatile-* with no
  TTLs on Kulyrax keys)". No warning and no refusal. A test that a blocked
  CONFIG still starts and emits that line (or a unit test on the message if
  Docker Redis can't block CONFIG). Note it in the decision record next to
  the eviction rule."
  So the policy check has three outcomes, none of which refuses:
  `allkeys-*` gives a warning naming the policy; unreadable (blocked or
  failed) gives that one info line; anything else is silent. Kulyrax has no
  logging crate, so warning and info are both stderr lines, distinguished by
  their prefix (`WARNING:` for the warning only). "Per adapter start" is
  per adapter instance: a run calls `prepare` more than once, so the check
  runs on the first call only. A reload builds new adapters, which check
  again.
  The plan approved: in `prepare`, `HSETNX __kulyrax:format guards 1` (atomic
  insert-if-absent), then `HGET`; a `NOPERM` reply is `MarkerNotCreated`,
  naming the ACL rights; the store is named by host and database number,
  never the URL.
- **Elasticsearch: the marker moved.** The proposal put it in each target
  index's `_meta.kulyrax_format`. Change 3 rules that out: a mapping update
  replaces `_meta` whole, last writer wins, so it cannot be an atomic
  insert-if-absent. The owner was asked on 2026-11-28 with three options
  (per-cluster index; per-index `_meta` as proposed; a marker document in
  each target index) and chose **"Cluster index"**:
  - one document per cluster, `PUT kulyrax_format/_create/guards {"version":
    1}`, atomic (409 when it exists), then read back and checked;
  - the same shape as MongoDB, Redis and Neo4j (one marker per store), and
    it covers every target index, including ones a mapping names later;
  - the grants are `create_index`, `write` and `read` on the index
    `kulyrax_format`.
  This supersedes "Elasticsearch: `_meta.kulyrax_format` in each target
  index's mapping" in the list below, and its trade-off ("a newer binary's
  index is refused only when a mapping targets it").
- **Reserved names: the gap is closed, not recorded.** The owner,
  2026-11-28 (quoted from the message):
  > Reserved names: don't leave it as a recorded gap. In E1 (same PR), refuse
  > at validation (exit 2, naming the mapping file, key and the reserved name)
  > any mapping target that collides with Kulyrax's own objects, per backend:
  > SQL tables: kulyrax_projection_state, kulyrax_format,
  > conduit_projection_state (the tombstone), and anything ending in
  > __kulyrax_rebuild (rebuild shadows). Compare case-insensitively (Postgres
  > folding, MySQL collation). MongoDB collections: kulyrax_projection_state,
  > kulyrax_format, conduit_projection_state. Neo4j labels: anything starting
  > with __Kulyrax or __Conduit. Redis: the existing __kulyrax prefix rule
  > (check it covers __kulyrax:format). Elasticsearch: indices are
  > user-named; nothing to reserve, but confirm _meta.kulyrax_format can't be
  > clobbered by a mapping field. File stores: already covered by the
  > layout/reserved-entry rules; confirm. One list of reserved names in core,
  > used by every backend's check. A test per backend. Add the rule to
  > docs/mapping-reference.md "Names". Update the decision record: the gap is
  > closed, not just recorded.

  How it was applied (`kulyrax_core::adapter::reserved`, the one list):
  - **Where it is checked.**
    - When mapping files load (the loader knows the file): a refusal names the
      file, the key and the name, and exits 2.
    - Mappings built in code skip the loader, so each backend's own
      `prepare` check uses the same list (SQL `check_identifiers`, MongoDB,
      Elasticsearch, Neo4j, Redis).
  - **Two names added beyond the owner's lists**, both Kulyrax's own:
    - `kulyrax_tombstone_tmp`, the tombstone's temporary name in MySQL and
      MongoDB, for SQL and document targets;
    - for Elasticsearch, `kulyrax_format`: the marker is no longer
      `_meta.kulyrax_format` (the owner chose a cluster index, above), so a
      mapping index of that name would collide with the marker index.
  - **Elasticsearch's guard field.** A mapped field cannot clobber the guard:
    the adapter writes `__kulyrax_guard` after the content fields. But a
    mapped field of that name would be dropped without a word, so top-level
    document fields starting with `__kulyrax` are reserved too.
  - **Document collections: one list.** A document mapping does not know
    whether it feeds MongoDB, Elasticsearch or a file store, so one list
    serves all three.
  - **Redis: confirmed covered.** The `__kulyrax` prefix covers
    `__kulyrax:format`. Redis's `prepare` rule now takes the prefix from the
    core list.
  - **File stores: confirmed covered.** Names may hold only letters, digits,
    `_` and `-` (`check_config_name`), so `.kulyrax` and `.conduit` cannot be
    named, and `Root::claim` keeps adapters apart.
  - **The shadow suffix only at load.** `prepare` checks only the exact
    tables. A rebuild projects through mappings that Kulyrax points at the
    shadows, so a `prepare`-time suffix check broke `rebuild` (the D7 exit-3
    and exit-4 tests caught it). For mappings built in code, the rebuild
    preflight still refuses a shadow-named table (exit 4).
  - **This amends decision `2026-11-26-exit-codes` for one case.** D7 listed
    "a mapped table named like a shadow" under exit 4. A mapping file naming
    one is now refused at load (exit 2). D7's exit-4 CLI test now uses the
    preflight's other refusal, a view depending on the table.
- **Change 4, the grants.** Postgres and MySQL check the create right even
  for `CREATE TABLE IF NOT EXISTS` on a table that exists (probed). So the
  guard table has always needed CREATE on its schema or database at every
  start, and the marker needs no right beyond that. The refusal names that
  grant only, with no "create the marker once as an administrator" remedy:
  it would not help while the guard table still needs CREATE.
- **Change 5, layout-marker values.**
  - A file store's format version is the integer in
    `.kulyrax/layout/<kind>` (one marker per adapter kind, as today).
  - `2` is the current format: the `.kulyrax/` layout, written by every start
    from 1.0.0-rc.1, and by step 2's migration since `f3af04b`.
  - `1` under `.kulyrax/` was written only by unreleased builds between
    `f3af04b` and this change, for stores created fresh (same layout and
    files as `2`). It is read as `2` and rewritten to `2`.
  - `1` under `.conduit/` (0.12.1) is unchanged: step 2's migration moves it
    and writes `2`.
  - An empty marker (an older build killed while writing one in place) is
    read as `2`. The owner, 2026-11-28: "Empty layout marker: treat as
    current, but re-stamp it atomically on open (with a test: empty marker →
    opened → marker holds the current value). An empty marker should never
    survive an open under 1.0."
  - Every accepted marker under the root that does not hold exactly `2\n`
    (empty, `1`, or `2` written differently) is re-stamped atomically on
    open: temp file, fsync, rename, directory fsync. This covers every kind's
    marker on the root, not only the opening adapter's. If any marker is
    refused, none is re-stamped.
  - Above `2` is refused (exit 4); content that is not an integer is refused
    as unrecognised (exit 4).

The proposal as approved:

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
