# Changelog

Notable changes per release. Full notes, including upgrade steps, are in
[`RELEASES/`](RELEASES/).

## v0.10.0

### Breaking
- Adapters you build yourself need `prepare()` before `handle()`. SQL, MongoDB and
  Neo4j adapters now set up their store (guard table, index, constraint) in
  `StorageAdapter::prepare`; an unprepared one fails every event with "adapter not
  prepared". Migration: call `adapter.prepare()?` once after construction.
  `KulyraxRuntime` and the run, replay and `execute_event` entry points already do.
  Custom adapters that don't override `prepare` are unaffected.
  ([notes](RELEASES/v0.10.0.md#upgrading-from-09))
- New error variants `SourceError::Prepare` and `PipelineError::AdapterPrepare`.
- New public fields: `ReplayRunOptions::max_batch`, `BatchSummary::fell_back`,
  `SourceRunOptions::max_batch_delay` and `kulyrax_ingest::ServeOptions::max_batch_delay`.
  Struct literals that list every field need them (or `..Default::default()`).
- Directory source checkpoints for a file larger than the batch are
  `<file>#<index>` instead of the file name.

### Changed
- SQLite and Postgres commit once per batch instead of once per event: one
  transaction per adapter per batch, guard lanes loaded in one query per batch,
  and (Postgres) one pooled connection held for the batch with its statements
  prepared once. A failing event rolls the batch back and it is re-dispatched one
  event at a time, as before. Other backends still commit per event.
- Postgres writes each batch in bulk: `decide()` still runs per event, in memory;
  each row and guard lane collapses to its final state and the batch is sent as a
  few multi-row statements (`unnest`) before the commit.
  ([decision](docs/ledger/decisions/2026-10-28-batch-transactions.md))
- Default batch size 5,000 events (was 256). In continuous `run` and `ingest` a
  batch also closes after `--max-batch-delay-ms` (default 100 ms), so a quiet
  stream is not held back waiting for 5,000 events. `replay` gains `--max-batch`.
- A rebuild into an empty table skips guard lookups for entities it has not written
  (`SqlTxn::has_guard_rows`, `sql::exec::KnownGuards`); assumes one writer per
  projection.
- The directory source lists its files once per backlog instead of on every poll.
- The directory source splits a file larger than the batch instead of taking it
  whole.

### Added
- `StorageAdapter::begin_batch` / `commit_batch` / `rollback_batch`, with no-op
  defaults; `SqlTxn::read_guard_lanes` and `SqlTxn::has_guard_rows`, with defaults
  that read every lane.
- Time per stage in the run report (`timings`) and in `kulyrax run` output.
- `examples/benchmark` covers every backend, with a projection-lag probe and a
  rebuild-while-ingesting run.

### Fixed
- MySQL: the guard read, target write and guard write were not atomic, because the
  guard table's DDL ran inside each event's transaction and MySQL commits on DDL.
- MySQL: writers adding new entities could deadlock each other (ERROR 1213), even
  on different target tables, through gap locks on the shared guard table. The
  MySQL transaction now runs at `READ COMMITTED`, like Postgres. Needs
  `binlog_format=ROW` or `MIXED` (the MySQL 8 default is `ROW`) if the binary log
  is on; `prepare` fails with a clear error on `STATEMENT`.

## Earlier releases

See [`RELEASES/`](RELEASES/): v0.9.0, v0.8.0, v0.7.0, v0.6.0, v0.5.0-alpha.
