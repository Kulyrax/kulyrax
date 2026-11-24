---
status: completed
---

# Postgres outbox source

## Objective

Owner's step 3 (2026-11-24): read a producer's Postgres outbox table as an
`EventSource`, by polling, in the order of a column the user names, without ever
changing the table. Design approved with six changes on 2026-11-24.

## Acceptance criteria

- [x] New crate `crates/sources/postgres` (`kulyrax-source-postgres`); the driver
  and tokio stay out of `kulyrax-core` (`cargo tree -p kulyrax-core` unchanged).
- [x] Registered by `kulyrax-backends` behind a feature (`postgres-outbox`, in
  `all`), through a new source registry in core
  (`KulyraxRuntime::register_source_factory`, `SourceConfig::Custom`).
- [x] `EventSource::poll` / `commit` / `committed_position`, plus the new
  `on_resolved` / `details` hooks.
- [x] Gaps: final once `pg_snapshot_xmin` reaches the `xmax` recorded at first
  sight, skipped once also held `gap_grace_ms` (default 1,000, `0` allowed; owner's
  follow-up, 2026-11-24); `gap_timeout_ms` as backstop; watch list with
  `watch_retention_ms` (default 3,600,000) and a 10,000 cap, expiries logged.
- [x] Late rows logged with sequence and decision; late rows and late rows
  skipped counted in the checkpoint and shown by `kulyrax sources`.
- [x] Start-up checks: table, columns and types; the sequence behind
  `sequence_column` must have increment 1 and cache 1; no sequence refused unless
  `allow_unverified_sequence: true`.
- [x] Single reader: an OS lock on `<state_dir>/<id>.lock`; a second process
  refused. `kulyrax sources` reads without locking or connecting.
- [x] Read only: `SELECT` only, in a read-only transaction.
- [x] Tests against Postgres: ordered delivery; redelivery after a crash before
  commit skipped by the guards; resume from the committed position; a gap held,
  then released; an empty table; a column missing from config refused; a
  rolled-back insert skipped without waiting for the timeout; `INCREMENT 2` and
  `CACHE 10` refused; watch-list expiry; a second reader refused; a late row
  delivered, logged and counted.
- [x] CI job, README section, v0.12 release notes (draft), ROADMAP.
- [x] Gates green, commit, push.

## Constraints and assumptions

- `decide()`, `dispatch.rs` and the dependency graph unchanged. The run loop
  (`source/runner.rs`) gains one call, `on_resolved`, before each commit.
- Postgres 13+ (`pg_current_snapshot`).

## Decisions

- [2026-11-24-postgres-outbox-source](../decisions/2026-11-24-postgres-outbox-source.md)

## Verification

| Check | Command | Result |
|---|---|---|
| Outbox suite, Postgres 16 (Docker, port 55432) | `KULYRAX_PG_URL=postgres://postgres:kulyrax@localhost:55432/kulyrax_ci cargo test -p kulyrax-source-postgres`, ×3 | 10 passed each run |
| First run | same | all 10 failed at start-up: `error serializing parameter 0`. The sequence lookup's `$1::regclass` made Postgres infer the parameter as `regclass`, which the driver will not send from a string. Fixed with `$1::text::regclass`, as the rebuild preflight does |
| Late row log (from `a_late_row_is_delivered_logged_and_counted`, `--nocapture`) | same | `skipping 1 missing value(s) 2..=2 (no transaction that could insert them is still running); watching for them` · `late row 2 (evt-late) delivered after its gap was skipped` · `late row 2 (evt-late) resolved Succeeded, skipped by every adapter: sql: Skipped { reason: StaleSequence }`; then `late rows: 1`, `late rows skipped: 1` |
| Rolled-back gap skipped without the timeout | `a_rolled_back_insert_is_skipped_without_waiting_for_the_timeout` | `gap_timeout_ms` 60,000; the gap held on first sight and skipped on the next poll, well under 10 s. Since `gap_grace_ms`: skipped after the 1 s default grace (asserted ≥ 1 s and < 10 s) |
| Grace, follow-up: `nextval()` before the insert | `a_value_taken_before_the_insert_is_held_by_the_grace` (`gap_grace_ms` 5,000) | a producer takes its value with `nextval()` in an open transaction and inserts later; a later row commits meanwhile. The gap is held on first sight and still held 300 ms later (final, as the producer has no transaction id, but within the grace); after the producer's insert commits, the next poll delivers both rows **in order**, nothing watched |
| The window is real without a grace | `without_a_grace_a_value_taken_before_the_insert_arrives_late` (`gap_grace_ms` 0) | the same producer: the gap is skipped as final on the second poll, and the row arrives later with a late position (`….0000000001`) |
| Outbox suite after the grace | `KULYRAX_PG_URL=… cargo test -p kulyrax-source-postgres --test outbox`, ×3 | 12 passed each run, first run included; the tests that need an immediate skip (`watched_values_expire`, `a_late_row_is_delivered_logged_and_counted`) set `gap_grace_ms: 0` |
| `kulyrax sources` | `cargo run -p kulyrax-cli -- sources --config <scratch config with a postgres_outbox source>` | `outbox  committed: (never)`, `watched gaps: 0`, `late rows: 0`, `late rows skipped: 0`, with no lock taken |
| Core stays driver-free | `cargo tree -p kulyrax-core -e normal --depth 1` | unchanged: crc32fast, once_cell, rusqlite, serde, serde_json, serde_yaml |
| Gates | `cargo fmt --check`; `cargo build --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace --no-fail-fast` | fmt, build, clippy clean; 122 binaries, 418 passed, 0 failed (the outbox suite skips here without `KULYRAX_PG_URL`; it ran against Postgres above). After the grace follow-up: clean; 122 binaries, 420 passed, 0 failed |

## Risks and open questions

- A row whose transaction took its sequence value before having a transaction id
  (`nextval()` before the insert) is covered for `gap_grace_ms`; a producer that
  waits longer than the grace between `nextval()` and its insert still has its gap
  judged final, and the row arrives late from the watch list and, if its entity
  has moved on, is skipped as stale. Counted and logged, not prevented.
- A long-running transaction anywhere on the producer's database holds gaps back
  until it ends or `gap_timeout_ms` passes.
- The watch list's retention is measured on the Kulyrax host's wall clock: a clock
  jump forward expires values early (never used for ordering).
- The tests share one database and run serially (`SERIAL` mutex) so open
  transactions in one test do not hold back another's gaps; CI's job runs this
  suite alone.

## Outcome

Completed.
