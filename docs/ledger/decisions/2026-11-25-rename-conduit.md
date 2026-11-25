---
status: accepted
---

# Rename every persisted `conduit` name to `kulyrax`, migrated in place at start-up

Accepted by the owner on 2026-11-25 with three changes, recorded under each heading below:
tombstones as proposed, each proven against a real 0.12.1 binary; the grep scope enforced in CI;
`.kulyrax/` shared, made safe by construction.

Task: [2026-11-25-rename-conduit](../tasks/2026-11-25-rename-conduit.md)

## Context

1.0 freezes the persisted names (`docs/stability.md` §5, §6). Four still carry the
project's old name. The owner's rules for step 2:

- the migration runs in `prepare` / start-up;
- if the old name exists and the new one doesn't, rename it in place, once,
  idempotently, keeping all data;
- if both exist, refuse and name both;
- no dual-read;
- the layout marker added in 0.12.1 records the new layout version;
- a migration killed halfway recovers on the next start.

Redis (`__kulyrax:guard:…`) and Elasticsearch (`__kulyrax_guard`) are already
Kulyrax-named and are not touched.

## Options considered

**How to move each name**

- **A. Rename in place at start-up** (owner's rule). One atomic rename where the
  backend has one, and a resumable move where it doesn't.
- B. Copy to the new name and drop the old one. This doubles storage, and a crash
  between copy and drop leaves both, which the rule refuses. Rejected.
- C. Dual-read (old as a fallback). Ruled out by the owner.

**The file stores' new internal directory**

- **`.kulyrax/`**: the obvious name, the one the inventory planned. It is already
  used by two other things that can share a folder with a store root:
  - `kulyrax reload` uses `<config folder>/.kulyrax/control/`;
  - `kulyrax init` scaffolds `state_dir: ./.kulyrax/state`.

  So a store whose `root` is the project folder shares `.kulyrax/` with them.
  Their entries (`control/`, `state/`) and the store's (`wal/`, `lock`, `layout/`,
  `kv/`, `entities/`, `guard/`, `incident/`) are disjoint, so this works if every
  store check looks at its own entries and never at "is `.kulyrax/` empty".
- `.kulyrax-store/`: no sharing, but a second internal directory name to explain
  forever.

**Old versions started against a migrated store**

After the migration, a 0.12.x process started on the same store or database would,
on its own, recreate the old name empty. Its `prepare` runs
`CREATE TABLE IF NOT EXISTS conduit_projection_state`, MongoDB creates a collection
on first insert, and Neo4j `MERGE` creates `__ConduitGuard` nodes. It would then
re-apply every event as if new. Two answers:

- **Tombstones**: leave an object under the old name that makes old versions
  fail loudly instead.
- Document it only ("stop every 0.12 process before upgrading").

## Evidence

Read in source (branch `security/file-names`, 0.12.1):

- SQL guard table `conduit_projection_state`: SQLite (`sql/sqlite.rs`, 12 uses),
  Postgres (`lib.rs` 14, `bulk.rs` 2), MySQL (`lib.rs` 12). Rebuild writes guard
  rows under the shadow table's name, then updates `target_table`. Only the
  guard table's own name changes, not its rows.
- MongoDB: `GUARD_COLLECTION = "conduit_projection_state"`, unique index on
  `(target, entity_key)` (default index name `target_1_entity_key_1`, no
  `conduit`).
- Neo4j: label `__ConduitGuard` in 6 queries, and an unnamed uniqueness constraint
  `FOR (g:__ConduitGuard) REQUIRE (kind, target, entity_key, facet) IS UNIQUE`.
  Neo4j cannot rename a label; nodes are relabelled with `SET … REMOVE …`.
- File stores: `.conduit/` holds `wal/`, `lock`, `layout/<kind>` (0.12.1),
  `kv/<ns>/`, `entities/<collection>/`, `guard/{nodes,edges}/`, `incident/`.
  **Batch-log entries store root-relative paths that include `.conduit/…`**
  (`guard_path` in `keyvalue/store.rs`, `document/file.rs`), so a log written by
  0.12.x replays correctly only under the old names.
- Code identifiers: `CONDUIT_DIR` and locals in `file_store.rs`. A config test uses
  `state_dir: ./.conduit/sources` (only a test value).
- The crash-injection hook (`KULYRAX_CRASH_AT=<point>:<n>`, feature
  `crash-points`, `file_store.rs`) already drives the CLI crash tests.

Unknown, checked by the tests before anything relies on it: whether SQLite renames
`sqlite_autoindex_conduit_projection_state_1` with the table.

## Decision

### New names

| Backend | Old | New |
|---|---|---|
| SQLite, Postgres, MySQL | table `conduit_projection_state` | `kulyrax_projection_state` |
| Postgres | index `conduit_projection_state_pkey` | `kulyrax_projection_state_pkey` |
| MongoDB | collection `conduit_projection_state` | `kulyrax_projection_state` |
| Neo4j | label `__ConduitGuard` (+ unnamed constraint) | label `__KulyraxGuard`, constraint named `kulyrax_guard_key` |
| File stores | directory `<root>/.conduit/` | `<root>/.kulyrax/`, same entry names inside |
| Redis, Elasticsearch | none | none |

Columns, fields, properties and entry names inside the directory are unchanged.
The layout marker becomes `.kulyrax/layout/<kind>` with content `2` (layout
version 2 = Kulyrax names). Step 5 turns it into the versioned format.

### Migration per backend

Every check runs in `prepare`, before any event, so there is never a dual-read.

**SQLite, Postgres**: transactional DDL, so a kill leaves the old state or the
new, never half.

```
BEGIN
  (Postgres) LOCK TABLE conduit_projection_state IN ACCESS EXCLUSIVE MODE
  old? new?                  -- sqlite_master / to_regclass in the current schema
  old only  → ALTER TABLE conduit_projection_state RENAME TO kulyrax_projection_state
              (Postgres) ALTER INDEX conduit_projection_state_pkey RENAME TO kulyrax_projection_state_pkey
              create the tombstone (below)
  both      → refuse: "both conduit_projection_state and kulyrax_projection_state exist in <db>"
  new only  → nothing (old may be the tombstone, recognised by its type)
  neither   → create new as today
COMMIT
```

The Postgres lock serialises several Kulyrax processes upgrading one database.

**MySQL**: DDL is not transactional, but `RENAME TABLE a TO b` is atomic and fails
if `b` exists. Check in `information_schema.tables` (current database), then
`RENAME TABLE`. If a concurrent process won the race, the rename fails; re-check
once, and expect "new only".

**MongoDB**: `renameCollection` with `dropTarget: false` (atomic on one replica
set, fails if the target exists, keeps the indexes). It cannot run inside a
transaction, so it runs in `prepare` beside `ensure_guard_index`. Sharded
collections cannot be renamed. Kulyrax doesn't support them as guard storage
today, and the error says so.

**Neo4j**: no atomic label rename. The migration resumes from wherever it stopped.

1. Create `kulyrax_guard_key` on `__KulyraxGuard` first (`IF NOT EXISTS`).
2. Relabel in batches, each its own transaction, until none are left:
   `MATCH (g:__ConduitGuard) WITH g LIMIT 10000 SET g:__KulyraxGuard REMOVE g:__ConduitGuard`
3. Drop the old constraint (found with `SHOW CONSTRAINTS` by label).

Each node moves atomically and keeps its identity. So old-label and new-label
nodes with *different* keys mean an interrupted migration: continue. A key under
*both* labels violates the new constraint on `SET`: refuse, naming the key and
both labels.

**File stores**: one lock is held for the whole migration, and each entry moves
atomically.

1. Take `.kulyrax/lock`. If `.conduit/lock` exists, also take it, so a 0.12.x
   process on this store is excluded or refused ("in use by another process").
2. Replay `.conduit/wal/` with the 0.12.1 recovery, under the old paths its
   entries name. Delete the empty log directory.
3. For each remaining entry of `.conduit/` (`layout`, `kv`, `entities`, `guard`,
   `incident`): if the entry is absent under `.kulyrax/`, rename it there (a
   directory rename, atomic). If it is present in both, refuse and name both
   paths. Disjoint entries in both are an interrupted migration: continue.
4. Rewrite the layout marker to `2`. Release and delete `.conduit/lock`, remove
   `.conduit/`, write the tombstone.

Then the 0.12.1 layout check runs as today, under the new names.

On Windows a directory with an open handle inside it cannot be renamed. Moving the
entries, not `.conduit/` itself, keeps the old lock's handle out of every renamed
directory.

### Tombstones (recommended)

After a successful migration, leave an object under the old name that a 0.12.x
process cannot use:

- **File stores**: a regular *file* `.conduit` saying "moved to .kulyrax by
  Kulyrax 1.0". 0.12.x then fails on `create_dir_all(".conduit/…")`.
- **SQLite, Postgres, MySQL**: a view `conduit_projection_state` that cannot be
  inserted into (a constant column). `CREATE TABLE IF NOT EXISTS` skips it, and
  the first guard write fails.
- **MongoDB**: an empty collection `conduit_projection_state` whose validator
  rejects every document.
- **Neo4j**: none possible on Community edition (property-existence constraints
  are Enterprise-only). Documented: stop every 0.12 process first.

The migration recognises a tombstone as "already migrated", not as "old exists".

Owner (accepted): a test per backend runs a real 0.12.1 `kulyrax` binary, built
in a worktree, against a migrated store and shows that it fails loudly. The
release notes give the manual reverse rename per backend.

Every tombstone is created before its rename becomes visible, or under a
temporary name and then renamed into place, so a kill cannot leave a migrated
store without its tombstone:

- SQLite and Postgres: same transaction as the rename.
- MySQL: one atomic multi-table `RENAME TABLE`.
- MongoDB: the tombstone is created as `kulyrax_tombstone_tmp` first, then the
  renames run. A leftover temporary is finished on the next start.
- File stores: `.conduit/` is renamed to `.kulyrax-tmp-conduit`, then the
  `.conduit` file is written. A leftover temporary is finished on the next start.

### Sharing `.kulyrax/` (owner's change 3)

One module, `kulyrax_core::kulyrax_dir`, lists every owner's entries:

- the file stores: `wal`, `lock`, `layout`, `kv`, `entities`, `guard`, `incident`;
- `kulyrax reload`: `control`;
- the scaffolded checkpoints: `state`.

`control` and `state` are reserved. A file store refuses to commit a batch that
writes any path under `.kulyrax/` outside its own entries. No store check
tests "is `.kulyrax/` empty": emptiness is judged by the store's own guard
entries. A test runs with store root = config folder = the parent of `state_dir`,
running reload, a store and a checkpoint together, then a restart.

### Stores last opened by 0.12.0

A 0.12.0 key-value store keeps guards in `.conduit/<namespace>/`. Those names are
not store entries; a namespace could even be `state` or `control`. Moving them
into `.kulyrax/` could therefore land in another owner's entry. The migration
refuses any entry it does not own and asks the operator to open the store once
with 0.12.1, which moves those guards into `.conduit/kv/`. 0.12.0 is affected by
GHSA-6chh-gx4c-448g anyway.

### Code and docs

`CONDUIT_DIR` → `KULYRAX_DIR` and the other identifiers, comments, docs, examples
and test names. Afterwards, `grep -ri conduit` (excluding `target/`) hits only:

- the migration code and its tests;
- the new release notes;
- `docs/ledger/` and older `RELEASES/` notes. These are history: completed
  records are edited only to correct errors, and old release notes describe old
  versions.

Owner (accepted): CI enforces this. A step runs `grep -ri conduit` outside
`target/` and fails on any hit outside an allowlist: the migration code, its
tests, the new release notes, `docs/ledger/` and older `RELEASES/`.

### Tests

- Per backend (SQLite and the file stores locally; Postgres, MySQL, MongoDB, Neo4j
  in Docker):
  - a 0.12.1-shaped store (old names, guard rows, at least one deleted and one
    permanent lane) opens with every lane intact, and an older redelivery is
    `Skipped(StaleSequence)`;
  - both names present → refused, naming both;
  - a second start is a no-op;
  - the tombstone makes a 0.12-shaped write fail.
- Interrupted migration:
  - File stores: CLI with crash points after each entry move.
  - Neo4j: crash point between relabel batches; core exposes `crash_point`
    `#[doc(hidden)]` under its `crash-points` feature for backend crates.
  - SQL and MongoDB: crash points just before and just after the rename
    statement. The next start completes it.
- Redis and Elasticsearch: their suites run unchanged, proving nothing moved.

## Trade-offs accepted

- Tombstones keep one object under the old name for good, because that is what
  protects a store from an old version. Removal could come in 2.0.
- A store root that is the project folder shares `.kulyrax/` with `control/` and
  `state/`. Mitigated by per-entry checks.
- Neo4j's migration is resumable, not atomic, and has no tombstone.
- Ledger and old release notes keep "conduit".
- No downgrade: going back to 0.12.x after the migration needs a manual reverse
  rename (documented per backend in the release notes).

## Revisit if

- A backend we add has persisted names of its own (decide them before release).
- Sharded MongoDB guard collections become supported.
- Operators report that sharing `.kulyrax/` with the project folder confuses
  them (move the store's entries to `.kulyrax-store/` then, as a layout version 3).
