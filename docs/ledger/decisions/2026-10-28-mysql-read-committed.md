---
status: accepted
---

# MySQL writes run at `READ COMMITTED`

Task: [2026-10-26-sql-schema-out-of-transaction](../tasks/2026-10-26-sql-schema-out-of-transaction.md)

## Context

Once the guard table's DDL left the event path, each MySQL projection became one
real transaction: guard read (`SELECT … FOR UPDATE`), target write, guard write.
Run in parallel, the MySQL suite then deadlocked intermittently (ERROR 1213).
`SHOW ENGINE INNODB STATUS` showed two transactions writing guard rows for
*different* target tables, each holding a gap lock on the same gap of
`conduit_projection_state` and waiting for an insert-intention lock in it. Under
`REPEATABLE READ`, a locking read of a guard row that does not exist yet locks the
gap where it would go, and every projection in a database shares that table.

## Options considered

1. **Retry on 1213/1205.** Rejected by the owner: with one writer per projection a
   deadlock means a bug, not contention; a retry would hide it.
2. **`READ COMMITTED`.** InnoDB takes no gap locks for locking reads at this level;
   an existing guard row is still row-locked by `FOR UPDATE`. The same guarantees
   as the Postgres adapter, which already runs at `READ COMMITTED`.
3. **Insert a placeholder guard row before reading.** Takes a record lock instead
   of a gap lock, at the cost of an extra statement per event and a changed write
   path in core. More change for the same result.

## Evidence

- InnoDB status for one deadlock: the task record above.
- `mysql_new_guard_rows_take_no_gap_lock` (deterministic: a raw connection holds
  the gap, the adapter projects into it, the raw connection inserts) fails with
  1213 under `REPEATABLE READ` and passes under `READ COMMITTED`; the parallel suite
  then passed 10 of 10 runs.
- In `mysql:8.4.13`, a `READ COMMITTED` write with `binlog_format=STATEMENT` fails
  with ERROR 1665 ("InnoDB is limited to row-logging when transaction isolation
  level is READ COMMITTED"); with `MIXED` it succeeds.

## Decision

Option 2. The MySQL adapter starts every write transaction at `READ COMMITTED`.

**Requirement:** when the server's binary log is on, `binlog_format` must be `ROW`
(the MySQL 8 default) or `MIXED`. `prepare` reads `@@log_bin` and
`@@SESSION.binlog_format` and fails with a message naming `binlog_format=ROW` if it
is `STATEMENT`, so `run` and `ingest` stop before ingesting instead of failing every
write. Tested by `mysql_binlog.rs`, which sets the global format to `STATEMENT` and
restores it.

## Trade-offs accepted

- A server on `binlog_format=STATEMENT` with the binary log on cannot run the MySQL
  adapter. `STATEMENT` is deprecated in MySQL 8.0.34 and later.
- Two writers creating the *same* new entity at once are no longer serialized by a
  gap lock; the second blocks on the first's guard insert and then upserts over it.
  That is the Postgres adapter's behaviour too; the supported model is one writer
  per projection.

## Revisit if

- Concurrent writers to the same projection become a supported deployment.
