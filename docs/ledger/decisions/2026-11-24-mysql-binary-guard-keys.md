---
status: accepted
---

# MySQL: binary-collated guard keys

Task: [2026-11-24-phase-4-batching](../tasks/2026-11-24-phase-4-batching.md)

## Context

MySQL now commits once per batch, like SQLite and Postgres. A batch decides
every event against a guard cache (`exec::GuardCache`) that keys lanes by exact
string: `(table, entity_key, facet)`.

MySQL's `conduit_projection_state` was created with the server's default
collation, which on MySQL 8 is `utf8mb4_0900_ai_ci`: case- and
accent-insensitive. There, `K` and `k` are one guard row. One event at a time,
an event for `k` reads `K`'s lane. In a batch, the cache holds two separate
lanes, so the same events can decide differently. Observed: a batch with
`Up K @5` then `Up k @1` created `k`, where one-at-a-time skipped it as stale
(`mysql_batch_matches_one_event_at_a_time`, with the old collation put back).

The same conflation also means two distinct entities whose keys differ only in
case share one guard lane on MySQL, but not on SQLite or Postgres.

## Options considered

1. **Binary key columns.** `target_table`, `entity_key` and `facet` become
   `utf8mb4_bin`, in `CREATE TABLE` for new stores and by an `ALTER TABLE` in
   `prepare` for existing ones. Lanes match exactly, as on SQLite and Postgres.
2. **Keep the collation, and stop caching on MySQL.** Every guard read would go
   to the database inside the batch, which sees the collation. But
   `prefetch_guards` and `KnownGuards` also key by exact string and mark lanes
   absent without a read, so this means MySQL-specific paths through the shared
   batch code, which CLAUDE.md rules out for storage kinds.
3. **Fold keys to the collation in the cache.** The collation is the server's
   choice, so the adapter would have to reimplement MySQL's comparison rules.

## Evidence

- Verified (test): the mutation above fails the batch-equivalence test; the
  binary columns pass it, five runs on fresh databases.
- Verified (test, `mysql_guard_collation.rs`): `prepare` converts a 0.10-style
  table (`utf8mb4_0900_ai_ci`) to `utf8mb4_bin`, keeps its rows, and afterwards
  `a` is a lane of its own beside `A`. Running `prepare` again changes nothing.
- Inferred: the conversion cannot fail on existing rows. Keys that are unique
  under a case-insensitive collation are also unique under a binary one.

## Decision

Option 1. New guard tables get binary key columns. `prepare` checks
`information_schema.COLUMNS` and runs the `ALTER TABLE` only when a key column
is not `_bin`.

## Trade-offs accepted

- **Behaviour change for existing MySQL stores.** Keys differing only in case
  become separate entities from 0.11 on. Before, an event for `k` could be
  skipped as stale because of `K`'s lane, or the other way round. Release notes
  call it out.
- The one-time `ALTER TABLE` rebuilds the guard table and waits for open
  transactions on it. With several Kulyrax processes on one MySQL database,
  the others' writes can fail with ERROR 1213 while it runs; those events go
  through the usual retry path. Upgrade them together, or expect a few retries.

## Revisit if

- A user depends on case-insensitive entity identity. That would be a mapping
  option (normalize the key) rather than a property of the guard table.
