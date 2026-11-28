---
status: in-progress
---

# A format version in every persisted format (step 5, E1 + E2)

## Objective

Runbook E1: a format version in every persisted format (checkpoints, batch log
header, sidecars and guards, outbox state, layout marker, DLQ naming if
relevant). A newer version is refused with exit 4 and a clear error. An older
one is migrated or refused, never misread. The config keeps `version: 1`
required.

E2: tests that a future version is refused, that each format round-trips, and
that each migrates from 0.12.1.

## Acceptance criteria

- [x] Inventory of every persisted format, read in source (decision record,
  Context).
- [x] The owner approves the design: approved 2026-11-28 with five changes,
  quoted in the decision record.
- [x] **Change 1:** a batch log with a foreign magic is refused (exit 4),
  never deleted, whatever its position. Its own commit, with a unit test and a
  CLI test. Docker suites and the workspace run follow at the PR's gates.
- [ ] Each format in the inventory records its version as decided.
- [ ] A newer version is refused before anything is read, replayed or written.
  The error names the file or store and both versions, and the CLI exits 4
  (a CLI test).
- [ ] An unversioned (0.12.x) file or store is read as format 1 and stamped.
- [ ] **Change 5:** the layout marker's content is read. A store holding `1`
  and a store holding `2` are each tested; new and migrated stores write `2`.
  An empty marker is read as current and re-stamped atomically on open (the
  owner's addition), tested.
- [ ] **Change 2:** the SQL `kulyrax_format` table is in the guard table's
  schema; tested with a non-default Postgres schema.
- [ ] **Change 3:** every marker is created insert-if-absent, atomically per
  backend. Concurrent first starts leave one marker with one version (a test
  per backend).
- [ ] **Change 4:** a marker that cannot be created refuses the start (exit 4),
  naming the store and the grant. The grants per backend are in the user docs.
- [ ] E2 tests: future version refused, round-trip, and migration from 0.12.1
  (the `upgrade from 0.12.1` CI job), for each format and backend.
- [ ] `stability.md`: the formats, their versions, and what is out of scope.
  rc.1 notes: the change.
- [ ] Gates: fmt, clippy `-D warnings`, workspace tests, examples, every Docker
  backend suite.
- [ ] PR merged with the 12 required checks green.

## Constraints and assumptions

- `decide()`, `dispatch.rs` and the dependency graph stay unchanged. Version
  checks live in each backend's `prepare` and in the source's checkpoint read.
- Zero panic in kulyrax-core; thin CLI (the exit-4 mapping is a match on the
  typed error).
- Assumed, to be verified by the upgrade job: no persisted shape changed
  between 0.12.1 and main except the rename step 2 already migrates. R4
  recorded none for Phase R; Phase D changed APIs, not serde shapes.
- Budget: about 2 days, accepted. Past 3 days: stop and report.

## Decisions

- [2026-11-28-format-versions](../decisions/2026-11-28-format-versions.md):
  one marker per store, a field in single-file formats (accepted, with the
  owner's five changes).
- The typed path, chosen in the code:
  - `kulyrax_core::format::FormatError` names the store and the problem
    (newer / unrecognised / marker not created).
  - `AdapterError::Format` carries it.
  - `PrepareFailure::refused` marks it.
  - The CLI maps a refused prepare failure to 4.
  - A format refusal during `handle` reports as `write_failed` in the JSON
    report, so the report's schema does not change.

## Verification

| Check | Command / source | Result |
|---|---|---|
| Disk before work | `df -h /c` | 203 GB free |
| Branch base | `git log formats/e1-format-versions` | `e99a19c` = `upstream/main` (#36 merge), no commits yet |
| Layout marker content | `file_store.rs:903` vs `legacy.rs:191`, `LAYOUT_VERSION` at `file_store.rs:35` | new stores write `1\n`; migrated stores write `2\n`; content never read (only `marker.exists()`, `file_store.rs:868`) |
| 0.12.1 marker | `kx-0121` `crates/core/src/adapter/file_store.rs:920` | `1\n` under `.conduit/` |
| Batch log with unknown magic | `file_store.rs:159-163`, `752-760` | `decode_log` returns `None`; the newest such log is deleted as torn |
| Replay before layout check | `file_store.rs:938-947` | `recover` runs before `check_layout` |
| Exit 4 reserved | decision `2026-11-26-exit-codes`, line 77 | "Step 5 adds the newer-format refusal: map it to 4 here, with a CLI test." |
| Logs are renamed only after fsync | `file_store.rs` `write_log` | temp file, `sync_all`, rename, directory fsync: a complete foreign magic cannot be crash damage |
| Change 1, unit tests, first run | `cargo test -p kulyrax-core --lib -- file_store format` | 8 pass, 1 fail: `foreign_magic_is_refused_not_torn` listed `KLX` as foreign, but it is a prefix of the magic, so the torn-log rule rightly applies (a test bug). Replaced with `KLY`; then 9 pass |
| Change 1, CLI test | `cargo test -p kulyrax-cli --test exit_codes` | 6 pass, including `exit_4_when_a_batch_log_is_not_this_versions` (`KLXWAL02` and `NOTALOG!`: exit 4, message, log kept byte for byte) |
| Change 1, the test catches the bug | the same CLI test with `file_store.rs` from `HEAD` (before the fix), then restored | fails at `exit_codes.rs:248`: no refusal; the log was replayed as torn |
| clippy | `cargo clippy --workspace --all-targets -- -D warnings` | clean |
| fmt | `cargo fmt --check` | first run: 2 diffs in the new tests; `cargo fmt` applied, then clean |
| Change 1, core and CLI tests | `cargo test -p kulyrax-core -p kulyrax-cli` (log summed with awk) | 402 pass, 0 fail, 105 test binaries, exit 0. The first run's summary was cut by a `head -20` in my pipeline, so it was rerun; only the rerun counts |
| Change 5, file-store marker, first run | `cargo test -p kulyrax-core --lib -- file_store` | 9 pass, 1 fail: `concurrent_first_starts_write_one_marker`, 1 of 8 starts `os error 2`. Cause: adapters in one process share a root and a pid, so two wrote the same marker temp file and one rename found it gone. The run also exposed that an adapter joining a root another adapter had just opened went straight to its layout check, possibly mid-recovery (since before step 5). Fixed: `Root::setup`, a mutex holding "set up", taken around migration, format check, recovery and layout check |
| Change 5, after the fix | the same; then the concurrency test alone, 30 runs | 10 pass; 30 of 30 |
| Owner: empty marker re-stamped on open | `an_empty_marker_is_restamped_on_open` (empty `kv` and `graph`, `2` with no newline for `document`; opened by a kv adapter), `a_refused_store_restamps_nothing` | 12 pass. Mutation check: with the re-stamp disabled (scratch copy), the first test fails at its marker assertion; file restored, `cmp` identical |
| Change 5 commit gates | fmt; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test -p kulyrax-core -p kulyrax-cli` | clean; clippy exit 0; 408 pass, 0 fail, 105 binaries. Committed `ccd71e9` |
| 0.12.1 checkpoint and outbox shapes | `kx-0121` (tag `v0.12.1`): `source/checkpoint.rs`, `sources/postgres/src/lib.rs:155` | checkpoint: `position`, `committed_at`, pretty JSON; outbox `State`: `after`, `watch`, `late_seq`, `late_rows`, `late_skipped`. The test fixtures use exactly these |
| Checkpoint format | `cargo test -p kulyrax-core --test format_versions` | 4 pass: round trip with `format: 1`; a 0.12.1 checkpoint reads as 1 and is stamped on the next commit; `format: 2` refused as newer; `"one"`, `0`, `-1` refused as unrecognised; the file is never changed by a refusal |
| Checkpoint, CLI | `exit_4_when_a_checkpoint_is_newer` in `exit_codes.rs` | 7 pass: exit 4, "written by a newer Kulyrax", checkpoint kept, no row projected |
| Outbox state format | `cargo test -p kulyrax-source-postgres --lib` | 2 pass: a 0.12.1 body reads as 1 and round-trips with `format: 1`; `format: 2` refused as newer, `"two"` as unrecognised |
| Checkpoint + outbox commit gates | fmt; workspace clippy `-D warnings`; `cargo test -p kulyrax-core -p kulyrax-cli -p kulyrax-source-postgres` | clean; exit 0; 431 pass, 0 fail, 111 binaries. The outbox crate's database tests skip without `KULYRAX_PG_URL` (not set here) and count as passes; they run in the Docker suites at the PR gates |
| SQLite marker, first run | `cargo test -p kulyrax-core --test format_versions` (5 SQLite cases) | 8 pass, 1 fail: the read-only database failed at open (the WAL switch writes), before the marker, as a plain write error. Fixed: a refused write is classified by reading the marker on the open connection (none: `MarkerNotCreated`, naming the grant; newer: refused as newer; else an ordinary error) |
| SQLite marker, second run | the same | 7 pass, 2 fail. (a) The read-only case's second half (marker present) was reported as "no marker": `CREATE TABLE IF NOT EXISTS` also fails read-only when the table exists; now the same classifier. (b) `concurrent_first_starts_leave_one_marker`: `database is locked` in 2 to 6 of 8 threads, 19 of 20 and then 29 of 30 runs, in under 1 s (so not the 5 s busy timeout) |
| Where the lock error comes from | experiment: the same test with the database put in WAL mode before the threads (scratch copy of the test, restored, `cmp` identical) | 0 of 10 runs fail: the WAL switch at open (`open_connection`) on a new database, which SQLite refuses at once without the busy handler. Present before step 5 for any concurrent first open of a new SQLite database |
| SQLite fixes | `set_wal` retries the switch while busy, up to the 5 s busy timeout; `ensure_schema` runs in `BEGIN IMMEDIATE` (a deferred read-then-write under WAL also fails at once) | 9 pass; the concurrency test 30 of 30 |
| Postgres marker, first run | `cargo test -p kulyrax-adapter-postgres --test pg_format` against `kx-pg` | 5 pass, 1 fail: the permission test's second half (admin creates the marker, grants SELECT/INSERT, app starts) failed with `db error` (the driver's Display hides the server text) |
| Why | the same steps by hand as the role (`docker exec kx-pg psql`, schema `kx_probe`, dropped after) | the advisory lock and the marker insert pass; `CREATE TABLE IF NOT EXISTS kulyrax_projection_state` on the existing table fails: `permission denied for schema kx_probe`. Postgres checks CREATE even when the table exists, so the guard table has needed CREATE on its schema at every start since before step 5. The "create the marker once with a privileged user" remedy was wrong: removed from the message and from both SQL markers (they no longer check existence first) |
| Postgres marker, after | the same | 6 pass: beside the guard table in a non-default schema; a `kulyrax_format` later on the `search_path` (version 9) is not taken for this schema's; 0.12 schema stamped, lane kept; newer refused, no guard table created; 8 concurrent first starts, one row; no CREATE: refused, `grant` = `CREATE on schema <s>`, then starts once granted |
| MySQL marker | `cargo test -p kulyrax-adapter-mysql --test mysql_format` against `kx-mysql` | 5 pass on the first run: the same cases (the database is the guard table's); the permission case names `CREATE on \`<db>\`.*`, the message holds no password, and the user starts once granted |
| SQL marker commit gates | workspace clippy `-D warnings`; then `cargo test -p <crate> -- --test-threads=1` with `KULYRAX_PG_URL` / `KULYRAX_MYSQL_URL` on the local containers | clippy exit 0; core 379, cli 39, postgres 41, mysql 35, source-postgres 18: all pass, 0 fail (the existing rename, upgrade-free and backend suites included) |
| Redis: local server | `docker exec kx-redis redis-cli INFO server`, `CONFIG GET maxmemory-policy` | 7.4.11 (ACLs available); `noeviction` |
| Redis eviction notice, unit | `cargo test -p kulyrax-adapter-redis --lib` | 6 pass: unreadable policy gives exactly the owner's info line; `allkeys-lru/lfu/random` give a `WARNING:` naming the policy; `noeviction` and `volatile-*` say nothing |
| Redis marker | `cargo test -p kulyrax-adapter-redis --test redis_format` against `kx-redis`, database 13 (flushed before and after each test) | 5 pass on the first run: one marker on a new database, idempotent; `2` refused as newer and `one` as unrecognised, value left; 8 concurrent first starts, one value; an ACL user without `HSETNX` refused with the rights named and no password in the message; an ACL user without `CONFIG` (checked blocked) starts |
| Redis info line through the binary | `cargo test -p kulyrax-cli --test redis_eviction_notice` (database 14, ACL user `-config`, deleted after) | first run: exit 2, the test project lacked `mappings/document` (test setup). Second: the line printed twice: one `run --once` calls `prepare` twice. Fixed: the adapter checks the policy on its first `prepare` only. Then 1 pass: exit 0, the event projected, exactly one info line, no `WARNING`, no password |
| Redis commit gates | workspace clippy `-D warnings`; `cargo test -p kulyrax-adapter-redis` and `-p kulyrax-cli` with the Redis, Postgres and MySQL URLs | clippy exit 0; redis 27, cli 40: all pass, 0 fail |
| MongoDB marker, first run | `cargo test -p kulyrax-adapter-mongodb -- --test-threads=1` against `kx-mongo` | `mongo_format` 4 pass (new, beside the guards, idempotent; 0.12 stamped, guards kept; `2` newer and `"two"` unrecognised refused with only `kulyrax_format` in the database; 8 concurrent first starts, one document). `mongo_rename` 3 fail, as expected: their exact collection lists now include `kulyrax_format`. Lists updated |
| MongoDB, after | the same | 30 pass, 0 fail, 7 binaries |
| MongoDB permission refusal: **not tested** | `docker exec kx-mongo mongosh … getCmdLineOpts` | `{ net: { bindIp: '*' }, replication: { replSet: 'rs0' } }`: no authorization, so `Unauthorized` (13) cannot be produced locally (CI's container is the same image setup). The mapping is code-reviewed only |
| `FormatError` in the D6 error test | `cargo test -p kulyrax-core --test error_traits` | 7 pass: `Send + Sync + 'static`, and `AdapterError::Format`'s `source()` is the `FormatError` |

## Risks and open questions

- Estimate: about 2 days.
  - Core: typed error, checkpoint, outbox state, file stores, exit 4 (~0.75 day).
  - Six networked backends plus SQLite, with tests (~1 day).
  - Upgrade-job cases and docs (~0.25 day).
- Elasticsearch has no `prepare` today. Its per-index marker check needs one.
- **For the owner: SQL least privilege.** Postgres and MySQL check the CREATE
  right even for `CREATE TABLE IF NOT EXISTS` on an existing table. So the
  guard table has always needed CREATE on its schema (Postgres) or database
  (MySQL) at every start, and a DBA cannot pre-create the tables for an
  app user that has DML rights only. This predates step 5; the marker needs
  no new right. Supporting it would mean checking existence before each
  `CREATE`, a new feature, so it is reported, not done. The required grants
  go in the user docs as they are.

## Outcome

In progress.
