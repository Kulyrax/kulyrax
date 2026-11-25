---
status: completed
---

# Rebuild a SQL projection in shadow tables

## Objective

The roadmap's "dedicated rebuild path": re-project a SQL adapter's full history
into shadow tables while readers keep the current ones, then swap them in
atomically. Owner (2026-11-24): other writers are paused for the rebuild; no
benchmarking for now.

## Acceptance criteria

- [x] `kulyrax rebuild --config --mappings --events --adapter` (through
  `pipeline::rebuild`) rebuilds every table the adapter's routed SQL mappings
  write.
- [x] Readers keep the old tables until one transaction swaps in each table and
  its guard lanes; no shadow table is left after a successful rebuild.
- [x] Indexes, keys and defaults carry over.
- [x] A writer that projected into the tables during the rebuild makes the swap
  refuse; nothing changes.
- [x] A failing event stops the rebuild before the swap.
- [x] Only the chosen adapter is written.
- [x] SQLite and Postgres; MySQL and non-SQL adapters refused with a clear error.
- [x] Docs (README, release notes) and a decision record.
- [x] Gates green before the push.

## Constraints and assumptions

- `decide()`, the batch path and dispatch unchanged; the rebuild replays through
  them.
- Other writers are stopped by the operator; the rebuild detects, not prevents.

## Decisions

- [2026-11-24-shadow-rebuild](../decisions/2026-11-24-shadow-rebuild.md)

## Verification

| Check | Command | Result |
|---|---|---|
| SQLite, end to end through `pipeline::rebuild` | `cargo test -p kulyrax-core --test sql_rebuild` | 4 passed: a mapping change (new column) rebuilt, the stray row gone, guard lanes the rebuild's under `users`, no shadow left, the index carried over and its name alternating on a second rebuild, the file adapter untouched, live replay carrying on; a writer during the rebuild refused; a failing event leaves the old table; a non-SQL or unknown adapter refused |
| First run | same | 3 of 4 failed on my test setup: `replay` reads `*.json` from a directory, so my `.ndjson` history there was skipped (0 events); the file adapter is refused by its missing rebuild support, not by routing. Fixed the tests |
| Postgres 16 (Docker) | `KULYRAX_PG_URL=postgres://postgres:kulyrax@localhost:55432/kulyrax_ci cargo test -p kulyrax-adapter-postgres`, ×2 | 19 passed (4 unit, 15 integration) both runs, incl. `pg_rebuild_swaps_the_shadow_table_in` (rebuild equals the live projection without the stray row, primary key and index on the swapped table, no shadow, no shadow lanes) and `pg_rebuild_refuses_to_swap_when_another_writer_projected` |
| First run | same, port 5432 | every test failed on password authentication: a local Windows PostgreSQL service also listens on 5432. The container moved to 55432; the local service was left alone |

## Risks and open questions

- Detection, not prevention: an operator who leaves a writer running gets a
  refused swap after the whole replay, not a refusal up front.
- Postgres: dependent views or foreign keys, and `serial` columns, make the swap's
  `DROP` fail (decision record, trade-offs).

## Outcome

Completed: `kulyrax rebuild` for SQLite and Postgres.
