# Changelog

Notable changes per release. Full notes, including upgrade steps, are in
[`RELEASES/`](RELEASES/).

## Unreleased

### Breaking
- Adapters you build yourself need `prepare()` before `handle()`. SQL, MongoDB and
  Neo4j adapters now set up their store (guard table, index, constraint) in
  `StorageAdapter::prepare`; an unprepared one fails every event with "adapter not
  prepared". Migration: call `adapter.prepare()?` once after construction.
  `KulyraxRuntime` and the run, replay and `execute_event` entry points already do.
  Custom adapters that don't override `prepare` are unaffected.
  ([notes](RELEASES/v0.10.0.md#upgrading-from-09))
- New error variants `SourceError::Prepare` and `PipelineError::AdapterPrepare`.

### Added
- Time per stage in the run report (`timings`) and in `kulyrax run` output.
- `examples/benchmark` covers every backend, with a projection-lag probe and a
  rebuild-while-ingesting run.

### Fixed
- MySQL: the guard read, target write and guard write were not atomic, because the
  guard table's DDL ran inside each event's transaction and MySQL commits on DDL.

## Earlier releases

See [`RELEASES/`](RELEASES/): v0.9.0, v0.8.0, v0.7.0, v0.6.0, v0.5.0-alpha.
