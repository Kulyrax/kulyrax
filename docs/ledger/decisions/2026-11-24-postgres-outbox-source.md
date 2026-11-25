---
status: accepted
---

# Postgres outbox source: poll a user-named sequence column, hold gaps, read only

Task: [2026-11-24-postgres-outbox-source](../tasks/2026-11-24-postgres-outbox-source.md)

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
Approved by the owner on 2026-11-24 with six changes, all folded in below.

The hard part is the **gap**. A `bigserial` id is allocated when the row is
inserted, but the row becomes visible only when its transaction commits. Reading
`WHERE seq > last ORDER BY seq`, a reader can see id 12 while id 11 is still
uncommitted, advance past 11, and never deliver it. A gap is either such an
in-flight row (it will appear) or a rolled-back insert or failed `ON CONFLICT` (it
never will).

## Options considered

1. **Poll by the sequence column; hold a gap until it fills or is provably final;
   keep watching skipped values for a while.** Exact in the common case; a row
   that arrives after its gap was skipped is still delivered, late.
2. **Poll by the writing transaction's id** (an `xid8` column, read only below
   `pg_snapshot_xmin`). Exact, but the order then comes from the database's
   transaction counter rather than a column the user controls, and it needs a
   column the producer may not have. Ruled out by the ordering constraint.
3. **Skip gaps on a timeout alone.** Simple, but every rollback costs the full
   timeout, and a slow producer transaction loses its row to the timeout.
4. **Never skip a gap.** Exact, but one rolled-back insert stops the source
   forever.
5. **Logical replication** (`pgoutput`, a replication slot). Push, commit order,
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
- Verified (Postgres docs, functions-info, "Snapshot Information Functions"): a
  snapshot's `xmax` is one past the highest completed transaction id; every
  transaction id at or above it was not yet completed when the snapshot was
  taken. `xmin` is the lowest transaction id still active; every id below it is
  committed or rolled back. Both are `xid8` (64-bit, no wraparound), Postgres 13+.
- Inferred: a transaction inserting a row has a transaction id by the time the row
  exists. If a gap's row was inserted by a transaction still running when the gap
  was seen, that id is below the gap's recorded `xmax`; once the current `xmin`
  reaches that `xmax`, that transaction has ended, and had it committed, the row
  would be visible to the same read. So the gap is final.
- Inferred: a global, strictly increasing column satisfies the per-entity rule, so
  it can serve as both delivery order and `Event.sequence`.

## Decision

Option 1. Postgres 13 or newer.

**Reading.** Each `poll(max_batch)`, in one statement (one snapshot): the rows after
`after` in sequence order (`LIMIT max_batch`), the rows whose sequence is on the
watch list, and `pg_snapshot_xmin(pg_current_snapshot())` /
`pg_snapshot_xmax(pg_current_snapshot())`. One `postgres` (sync) connection,
opened at start-up, reopened after an error. Plain reads: no locks, no `FOR UPDATE`.

**Ordering.** The user names the sequence column (`sequence_column`, required): a
`bigint` that strictly increases in insert order, typically the table's
`bigserial` / identity key. It is both the delivery order and `Event.sequence`.
Kulyrax never orders by a timestamp, `now()`, or any other clock.

**Gaps.** Rows are delivered in sequence order up to the first missing value: after
`after`, if the next row read is `after + k` with `k > 1`, the values between are a
gap, and nothing past it is delivered yet.
- **Primary rule.** When a gap is first seen, the source records the `xmax` of the
  snapshot that saw it. On every later poll, the gap is **final** once that poll's
  `pg_snapshot_xmin(pg_current_snapshot())` is at or above the recorded `xmax`:
  every transaction that could have inserted the missing rows has ended, and the
  same read shows they did not commit.
- **Grace** (owner, 2026-11-24). A gap is skipped only when it is final **and** has
  been held at least `gap_grace_ms` (default **1,000**, `0` allowed, on the source's
  monotonic clock). `xmax` cannot account for a producer that took its value with
  `nextval()` before it had a transaction id (it gets one at its first write, at or
  above the recorded `xmax`), so the gap looks final at once; the grace gives such
  a producer that long to insert, and its row is then delivered in order.
- **Backstop.** `gap_timeout_ms` (default **10,000**, on the source's own monotonic
  clock, `Instant`) skips a gap that has not become final, for instance while a
  long transaction on the database holds `xmin` back.
- If the missing rows appear, they are delivered and delivery resumes in order.
- A skipped value goes on the **watch list**. Every poll also reads the watched
  values; one that appears is delivered then, late, and leaves the list.
- The watch list drops a value after `watch_retention_ms` (default **3,600,000**,
  one hour, measured from the skip on the Kulyrax host's wall clock, which survives
  a restart) or when more than **10,000** values are watched (oldest first). Each
  dropped value is logged.

**Late rows.** Each late delivery is logged with its sequence value, and, once the
run loop has resolved it, its decision: the event's status and, per adapter,
applied or skipped and why. The checkpoint state counts **late rows** and
**late rows skipped** (every adapter skipped it), and `kulyrax sources` shows both.
For this the `EventSource` trait gains `on_resolved(event, report)`, which the run
loop calls for every event before `commit` (default: nothing), and `details()`,
which `kulyrax sources` prints (default: none).

**Position.** Stored in **Kulyrax's own checkpoint** (`state_dir`, the checkpoint
file every source uses, written temp → fsync → rename), never in the producer's
database. The checkpoint holds the source's whole state as one JSON document:
`after` (every value up to it delivered or skipped), the open gaps, the watch list,
and the late-row counters. One file, so state and position cannot diverge after a
crash. The `SourcePosition` each event carries is `<after, 20 digits>.<late
deliveries, 10 digits>`, e.g. `00000000000000001234.0000000002`: text order equals
delivery order (a late row reuses the current `after` with the next counter value),
and it has no characters a file name forbids. The source keeps the state belonging
to each position it handed out until `commit` writes it.

**Single reader.** At start-up the source takes an exclusive OS lock
(`File::try_lock`, as the file stores do) on `<state_dir>/<id>.lock`, held for the
process's life. A second process with the same source id and state directory stops
at start-up with an error naming the lock. `kulyrax sources` reads the checkpoint
without taking the lock or connecting.

**Ownership.** Read only. Kulyrax runs `SELECT` statements only: it never inserts,
updates or deletes outbox rows, creates nothing in the producer's database, and
works with a role that has `SELECT` on the table (the sequence check reads the
`pg_sequence` system catalog, which every role can read). Cleaning the
outbox is the producer's job; rows at or below the committed `after`, and not on
the watch list, are safe to delete.

**Start-up checks.** The factory connects and checks, refusing to start with the
object named:
- the table exists, and every configured column exists with a usable type
  (`sequence_column`: an integer type; `event_id`, `event_type`: text; `payload`:
  `json`, `jsonb` or text; `metadata`: `json` / `jsonb`; `version`: an integer
  type); `sequence_column` is set;
- the **sequence behind `sequence_column`** (`pg_get_serial_sequence`, which covers
  `serial` and identity columns) has `increment_by = 1` and `cache_size = 1`
  (the `pg_sequence` catalog). A step other than 1 would make every value a gap; a cache
  above 1 hands each session its own block of values, so values stop following
  insert order across sessions, and unused blocks become permanent gaps;
- if the column has **no sequence** (ids the application assigns), start-up refuses
  unless `allow_unverified_sequence: true`. That setting gives up the guarantee
  that values arrive in insert order: an id assigned below `after` after the
  source passed it is delivered only if it was a gap still on the watch list, and
  is otherwise never read. The gap rules still apply to the values the source
  does see.

**Config and mapping to the `Event` envelope.**

```yaml
sources:
  - type: postgres_outbox
    id: orders-outbox
    state_dir: ./.kulyrax/state      # Kulyrax's checkpoint and lock
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
      gap_grace_ms: 1000             # a final gap is still held this long
      gap_timeout_ms: 10000          # backstop for a gap that never turns final
      watch_retention_ms: 3600000
      allow_unverified_sequence: false
```

| `Event` field | From |
|---|---|
| `sequence` | `sequence_column` (cast to `u64`; a negative value fails the row) |
| `event_id` | `columns.event_id`, text |
| `event_type` | `columns.event_type`, text |
| `payload` | `columns.payload`: `json` / `jsonb` as its JSON text, or text as is |
| `metadata` | `columns.metadata`: a JSON object of strings; null, or no column, is empty; a non-string value becomes its JSON text |
| `version` | `columns.version`; null, or no column, is 1 |

A row that cannot become an `Event` (a null `event_id`, say) fails `poll` with its
sequence value named.

**Registration.** `kulyrax-core` gains a source registry beside the adapter
registry: `SourceConfig` accepts an unknown `type:` as `Custom`, resolved at
start-up by a factory registered on the runtime (`register_source_factory`).
`kulyrax-backends` registers `postgres_outbox` behind a feature, `postgres-outbox`
(in `all`), as it registers adapters.

## Trade-offs accepted

- **A gap waits for every older transaction.** The primary rule waits until the
  database's oldest running transaction is newer than the gap; an unrelated long
  transaction holds gaps back until it ends or `gap_timeout_ms` passes.
- **Every final gap costs `gap_grace_ms`.** A rolled-back insert now holds the source
  for the grace (1 s by default) before it is skipped; `0` restores skipping on the
  next poll, and reopens the window below.
- **A rare row can still arrive late.** The grace narrows the `nextval()`-before-
  transaction-id window, it does not close it: a producer that waits longer than
  `gap_grace_ms` between taking its value and inserting still has its gap skipped as
  final. So can a transaction slower than the backstop. Such a row is delivered
  late from the watch list, and the guards judge it by its sequence: if its entity
  has a newer event, it is skipped as stale; under `on_existing: ignore`, a late
  create is skipped. Late rows and late skips are logged and counted. A producer
  that inserts with the column's default takes its value and its transaction id
  in the same statement, microseconds apart, well inside the grace.
- **The watch list is bounded** (one hour, 10,000 values). A value dropped from it
  is not read again.
- Polling costs a query per poll interval even when idle.
- Postgres 13 or newer, for `pg_current_snapshot()`.

## Revisit if

- Polling load or latency matters: logical replication (option 5), with commit
  order and no gaps.
- Producers need several outboxes feeding one entity, or ordered multi-table
  outboxes.
- Long-running transactions on the producer's database routinely hold gaps back:
  read by transaction id (option 2) as an opt-in.
