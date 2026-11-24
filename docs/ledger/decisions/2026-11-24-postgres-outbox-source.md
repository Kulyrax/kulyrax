---
status: proposed
---

# Postgres outbox source: poll a user-named sequence column, hold gaps, read only

Task: 2026-11-24-postgres-outbox-source (written once this is approved)

## Context

Many producers already write their events to an outbox table in their own Postgres
database, in the same transaction as their state change. Kulyrax should read that
table as an `EventSource`, like `directory` and `stdin`, with the same at-least-once
contract: `poll` hands the run loop ordered batches, `commit` records a position only
after a batch fully resolves, and the guards make redelivery harmless.

Constraints (owner, 2026-11-24): polling first; ordering from a monotonic column the
user names, never a timestamp or the database clock; Kulyrax's own checkpoint, not
the producer's table; Kulyrax never deletes or updates outbox rows. A new crate,
`crates/sources/postgres` (`kulyrax-source-postgres`), keeps the driver and tokio
out of `kulyrax-core`. `decide()`, dispatch and the dependency graph unchanged.

The hard part is the **gap**. A `bigserial` id is allocated when the row is
inserted, but the row becomes visible only when its transaction commits. Reading
`WHERE seq > last ORDER BY seq`, a reader can see id 12 while id 11 is still
uncommitted, advance past 11, and never deliver it. A gap is either such an
in-flight row (it will appear) or a rolled-back insert, a failed `ON CONFLICT`, or a
sequence cache jump (it never will). The reader cannot tell which by looking.

## Options considered

1. **Poll by the sequence column; hold a gap until it fills or a timeout passes,
   then skip it and keep watching for it.** Exact while gaps fill within the
   timeout; a row that commits later still arrives, late.
2. **Poll by the writing transaction's id** (`xid8` column defaulting to
   `pg_current_xact_id()`, read only below `pg_snapshot_xmin`). Exact with no
   timeout, but the order then comes from the database's transaction counter
   rather than a column the user controls, it needs a column the producer may not
   have, and it is Postgres 13+ only. Ruled out by the ordering constraint.
3. **Never skip a gap.** Exact, but one rolled-back insert stops the source
   forever.
4. **Logical replication** (`pgoutput`, a replication slot). Push, in commit order,
   no gaps, but needs `wal_level = logical`, a slot that pins WAL while Kulyrax is
   down, and replication privileges. Out of scope: see Revisit if.

## Evidence

- Verified (source, `source/runner.rs`): the run loop sorts a batch by
  `event.sequence`, then commits the batch's **greatest `SourcePosition` by string
  order**, and puts the position verbatim in DLQ file names. A position must sort
  as text in delivery order and be safe in a file name.
- Verified (source, `source/directory.rs`): sources re-poll from their committed
  position each time; nothing is buffered between `poll` and `commit`.
- Verified (source, `source/checkpoint.rs`): `write_checkpoint` writes temp, fsync,
  rename, directory fsync, since v0.11.
- Verified (docs, `docs/producer-contract.md` §1, §3): `Event.sequence` must strictly
  increase per entity; gaps are fine. A lower-sequence event delivered after a
  higher one for the same entity is skipped as stale, and under `on_existing:
  ignore` a late create is skipped.
- Inferred: a global, strictly increasing column satisfies the per-entity rule, so
  it can serve as both delivery order and `Event.sequence`.

## Decision

Option 1.

**Reading.** Each `poll(max_batch)`: `SELECT … FROM <table> WHERE <sequence> >
$after ORDER BY <sequence> LIMIT $max_batch`, plus `WHERE <sequence> = ANY($watched)`
for skipped gaps (below). One `postgres` (sync) connection, opened at start-up,
reopened after an error. No locks, no `FOR UPDATE`: plain reads.

**Ordering.** The user names the sequence column (`sequence_column`, required): a
`bigint` that strictly increases in insert order, typically the table's
`bigserial` / identity key. It is both the delivery order and `Event.sequence`.
Kulyrax never orders by a timestamp, `now()`, `xmin` or any other database clock.

**Gaps.** Rows are delivered in sequence order up to the first missing value:
after `after`, if the next row read is `after + k` with `k > 1`, the values in
between are a gap and nothing past it is delivered. The source notes, on its own
monotonic clock (`Instant`, never the database's), when it first saw the gap.
- If the missing rows appear, delivery resumes in order.
- After `gap_timeout_ms` (default **10,000**, configurable), each still-missing value
  is **skipped**: delivery moves past it, and the value is added to a **watch
  list**. Every later poll also reads the watched values; one that appears (a
  producer transaction that took longer than the timeout) is delivered then, late,
  and leaves the list. The guards decide it by its sequence, like any other event:
  for an entity that has since moved on, it is stale and skipped (see Trade-offs).
- The watch list keeps the most recent **10,000** skipped values; the oldest drop
  off first, logged.
- One gap holds the whole source for at most `gap_timeout_ms` once; gaps are
  timed independently.

**Position.** Stored in **Kulyrax's own checkpoint** (`state_dir`, the checkpoint
file every source uses, written temp → fsync → rename), never in the producer's
database. The checkpoint holds the source's whole state as one JSON document:
`after` (every value up to it delivered or skipped), the watch list, and a counter
of late deliveries. One file, so state and position cannot diverge after a crash.
The `SourcePosition` each event carries is `<after, 20 digits>.<late counter, 10
digits>`, e.g. `00000000000000001234.0000000002`: text order equals delivery order
(a late row reuses the current `after` with the next counter value), it has no
characters a file name forbids, and the source keeps the state belonging to each
position it handed out until `commit` writes it. `committed_position` returns it;
`kulyrax sources` shows it.

**Ownership.** Read only. Kulyrax runs `SELECT` statements only: it never inserts,
updates or deletes outbox rows, creates nothing in the producer's database, and
works with a role that has `SELECT` on the table alone. Cleaning the outbox is the
producer's job; rows at or below the committed `after`, and not in the watch list,
are safe to delete.

**Start-up checks.** The factory connects and reads `information_schema.columns`:
the table must exist, and every configured column must exist with a usable type
(`sequence_column`: an integer type; `event_id`, `event_type`: text; `payload`:
`json`, `jsonb` or text; `metadata`: `json` / `jsonb`; `version`: an integer type).
A missing or mistyped column, or a missing `sequence_column`, stops start-up with
the column named.

**Config and mapping to the `Event` envelope.**

```yaml
sources:
  - type: postgres_outbox
    id: orders-outbox
    state_dir: ./.kulyrax/state      # Kulyrax's checkpoint, as for other sources
    config:
      url: ${OUTBOX_DATABASE_URL}    # ${ENV} expanded
      table: public.outbox
      sequence_column: id            # required
      columns:                       # optional; defaults shown
        event_id: event_id
        event_type: event_type
        payload: payload
        metadata: metadata           # may be ~ (no column): empty metadata
        version: version             # may be ~ (no column): version 1
      gap_timeout_ms: 10000
```

| `Event` field | From |
|---|---|
| `sequence` | `sequence_column` (cast to `u64`; a negative value fails the row) |
| `event_id` | `columns.event_id`, text |
| `event_type` | `columns.event_type`, text |
| `payload` | `columns.payload`: `json` / `jsonb` as its JSON text, or text as is |
| `metadata` | `columns.metadata`: a JSON object of strings; null, or no column, is empty; a non-string value becomes its JSON text |
| `version` | `columns.version`; null, or no column, is 1 |

A row that cannot become an `Event` (a null `event_id`, say) fails at `poll` with
its sequence value named; the run loop's usual retry and dead-letter handling
applies.

**Registration.** `kulyrax-core` gains a source registry beside the adapter
registry: `SourceConfig` accepts an unknown `type:` as `Custom`, resolved at
start-up by a factory registered on the runtime (`register_source_factory`).
`kulyrax-backends` registers `postgres_outbox` behind a feature, `postgres-outbox`
(in `all`), as it registers adapters.

## Trade-offs accepted

- **Latency at gaps.** A rolled-back insert stalls the source by up to
  `gap_timeout_ms`. Producers that roll back often see that delay often.
- **A row slower than the timeout arrives late**, and the guards judge it by its
  sequence: if its entity has a newer event, it is skipped as stale; under
  `on_existing: ignore`, a late create is skipped. Its data then never lands. The
  timeout should exceed the producer's longest transaction.
- **The watch list is bounded** (10,000). A row skipped longer ago than that many
  later gaps is not watched for any more.
- Polling costs a query per poll interval even when idle.
- One global sequence column per table: a table whose ids are not insert-ordered
  (UUIDs, an id assigned by the application from several sequences) cannot be
  read correctly.

## Revisit if

- Gap stalls hurt (frequent rollbacks): read by transaction id (option 2) as an
  opt-in, or tell in-flight gaps from final ones with `pg_snapshot_xmin`.
- Polling load or latency matters: logical replication (option 4), with commit
  order and no gaps.
- Producers need several sources feeding one entity, or ordered multi-table
  outboxes.
