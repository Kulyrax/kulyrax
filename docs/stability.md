# Stability

What Kulyrax promises from 1.0.0, and what it does not. §0 is the promise. §1
to §5 list every surface it covers, item by item. §6 to §9 are the history of
how the list was made (the 1.0 plan's step 1, reviewed by the owner).

---

## 0. The promise

Kulyrax follows [semantic versioning](https://semver.org/) from 1.0.0. In a
minor or patch release, nothing covered below changes in a way that breaks a
user who relied on it: code that compiled still compiles, a project that
loaded still loads, a store or checkpoint still opens, and a script still sees
the same exit codes.

**Covered by semver:**
- **The public Rust API** of the 11 library crates (§1): every item that is
  public, documented and not `#[doc(hidden)]`. Its record is the 1.0 snapshot
  in [`stability/api-1.0/`](stability/api-1.0/).
- **The YAML files** (§2): `config.yaml` (`version: 1`), mappings, routing.
  Every key and its meaning. Unknown keys are refused.
- **The CLI** (§3): command names, flags and their meaning, the `--output json`
  field names, and the exit codes.
- **The gRPC contract** (§4): `proto/kulyrax/v1/ingest.proto`, on the wire.
- **Persisted formats** (§5): what Kulyrax writes to your stores and state
  directories, and their format versions. A minor release may add a format
  version, but it reads every older one; a store written by a newer version is
  refused, never misread.

**Not covered**, and free to change in any release:
- the CLI's human-readable output (anything without `--output json`), and its
  progress lines;
- log and stderr text, warnings and info lines included;
- the text of error messages (`Display`). Match on the error types and their
  variants, which are covered;
- items that are `#[doc(hidden)]`, or that a module's docs or §1 mark
  UNSTABLE: the engine's internals (`dispatch`, `batch`, the file store, the
  loaders, `execute`, `factory`, …) and the re-exports kept for convenience;
- the generated gRPC code behind the `unstable-pb` feature (below);
- performance, the batching of writes, and the order of independent writes
  that the [producer contract](producer-contract.md) does not promise.

**Deprecation.** An item is removed only in a major release. Before that, it
is marked `#[deprecated]` (with what to use instead) for at least one minor
release.

**MSRV.** The minimum supported Rust version is **1.89**, set as
`rust-version` in the workspace `Cargo.toml`. CI builds the workspace with it
on every pull request (the `MSRV (1.89)` job). Raising it is a minor release,
never a patch release, and the release notes say so.

**`unstable-pb`.** `kulyrax-client` and `kulyrax-ingest` keep their
tonic/prost-generated code private. The `unstable-pb` feature exposes it as
`#[doc(hidden)]` modules, for code that must reach the generated types. It is
outside semver: a tonic or prost upgrade may change it in any release. The
frozen contract is the `.proto` (§4).

**How it is checked.**
- `cargo-semver-checks` runs on every pull request. It reports against the
  last release, and becomes a required, strict check against `v1.0.0-rc.1`
  once that is tagged.
- `buf breaking` checks the `.proto` against v1, and `deny_unknown_fields`
  holds the YAML.
- The format versions of §5 are checked by a test per backend, and by the
  `upgrade from 0.12.1` job with a real 0.12.1 binary.

Each surface below marks its items:
- **FREEZE**: covered by semver.
- **UNSTABLE**: kept but outside the promise (`#[doc(hidden)]` or documented as
  unstable), or made private (`pub(crate)`).
- **CHANGE**: a change the 1.0 plan made before freezing; every one is done.

The inventory was taken at `d80b3cd` (0.12.0). Where a row below and §9
disagree, §9 is the decision.

---

## 1. Public Rust API

Generated with `cargo +nightly public-api -p <crate> --simplified`
(cargo-public-api 0.52.0), one item per line. Two snapshots:
- [`stability/api-1.0/`](stability/api-1.0/): **the record for 1.0**, taken
  for 1.0.0-rc.1 (main `56413bc`).
- [`stability/api/`](stability/api/): the 0.12.0 inventory, kept unchanged as
  history. The tables below were made from it.

| Crate | 0.12.0 lines | 1.0 lines | What it exposes |
|---|---|---|---|
| `kulyrax-core` | 5,449 | 3,972 | 1.0: 49 public modules, 101 structs, 49 enums, 10 traits, 919 functions, 16 consts, 7 type aliases, 1,997 trait impls ([1.0 list](stability/api-1.0/kulyrax-core.txt)). 0.12.0: 67 modules, 145 structs, 70 enums, 15 traits, 1,186 functions, 11 consts, 9 type aliases, 2,643 trait impls ([0.12.0 list](stability/api/kulyrax-core.txt)) |
| `kulyrax-ingest` | 208 | 90 | `serve`, `start`, `ServeOptions`, `GrpcServer`, `GrpcSource`, `IngestError`, `ServeError`. 0.12.0 also exposed the generated `pb` module, now behind `unstable-pb` |
| `kulyrax-client` | 198 | 175 | `Producer`, `EventEnvelope`, `Ack`, `AckOutcome`, `ClientError`, `RpcError`, `StatusCode` (Kulyrax's own types). 0.12.0 re-exported generated `pb` types, now behind `unstable-pb` |
| `kulyrax-backends` | 2 | 2 | `register_default_backends` |
| `kulyrax-source-postgres` | 70 | 79 | `factory`, `OutboxConfig` (`new` + `with_*`), `OutboxColumns`, `PostgresOutboxSource` |
| `kulyrax-adapter-postgres` | 71 | 50 | `factory`, `PostgresConfig`, `PostgresAdapter::from_config` and its trait impls. 0.12.0 also exposed `PostgresTxn` and a positional `new` |
| `kulyrax-adapter-mysql` | 64 | 48 | `factory`, `MySqlConfig`, `MySqlAdapter::from_config`. 0.12.0 also exposed `MySqlTxn` |
| `kulyrax-adapter-redis` | 40 | 48 | `factory`, `RedisConfig`, `RedisAdapter::from_config` |
| `kulyrax-adapter-mongodb` | 42 | 49 | `factory`, `MongoDbConfig`, `MongoDbAdapter::from_config` |
| `kulyrax-adapter-neo4j` | 43 | 50 | `factory`, `Neo4jConfig`, `Neo4jAdapter::from_config` |
| `kulyrax-adapter-elasticsearch` | 42 | 52 | `factory`, `ElasticsearchConfig`, `ElasticsearchAdapter::from_config` |

`kulyrax-cli` is a binary only; its surface is the CLI (§3).

### `kulyrax-core`, by module

| Module | Items | Status |
|---|---|---|
| `event` (`Event`) | 75 | **FREEZE**: `#[non_exhaustive]`, built with `Event::new` / `with_metadata` / `with_version` (step 4, D1, §9.6) |
| `adapter` (`StorageAdapter`, `AdapterResult`, `AdapterOutcome`, `AdapterError`, `SkipReason`, `GuardState`, `decide`, `WriteDecision`, `OnExisting`, `Operation`, `Prepared`, `prepare_adapters`, `PrepareFailure`) | 173 | **FREEZE** the extension surface; **CHANGE** (step 4): `#[non_exhaustive]` on `AdapterError`, `AdapterOutcome`, `SkipReason`, `WriteDecision` |
| `adapter::sql` (`SqlTxn`, `SqlWrite`, `SqlPlan`, `GuardRow`, `GuardCache`, `KnownGuards`, `prefetch_guards`, `project`, `project_in`, `plan_event`, `outcome_result`, `ShadowTable`, `GuardFingerprint`, rebuild helpers, `SqlMapping`, `SqlRuntimeBuilder`, `SqliteAdapter`) | 372 | **FREEZE** what `docs/writing-a-backend.md` tells backend authors to use (`SqlTxn` and the `exec` functions, mapping and runtime types); **UNSTABLE**: `sql::preview`, `sql::loader` (CLI / pipeline internals) |
| `adapter::document`, `adapter::keyvalue`, `adapter::graph` (backend traits `DocumentBackend` / `KvBackend` / `GraphBackend`, `exec`, mappings, runtime builders, file adapters) | 133 / 137 / 206 | **FREEZE** traits, `exec`, mappings, runtime builders; **UNSTABLE**: `*::preview`, `*::loader` |
| `adapter::file_store` (`FileStore`, `FileTxn`) | 15 | **UNSTABLE**: shared plumbing of the three file adapters, not an extension point |
| `execution` (`ExecutionReport`, `AdapterExecutionReport`, `ExecutionStatus`, `AdapterFailure` (0.12: `AdapterReportError`), `stages`) | 187 | **FREEZE** the report types (they are also the JSON the CLI prints, §3); **UNSTABLE**: `execution::stages` (benchmark timing), `execution::time`, `execution::time_option` (serde helpers) |
| `report` (re-exports of the execution report) | 6 | **CHANGE** (step 4): one path per type. `AdapterReportError` is reachable at four paths today (`kulyrax_core::`, `execution::`, `execution::report::`, `report::`) |
| `routing` (`AdapterId`, `StorageKind`, `route`, `route_with_rules`, `load_routing`) | 99 | **FREEZE** `AdapterId`, `StorageKind` (`#[non_exhaustive]`, step 4); **UNSTABLE** the routing functions (pipeline internals) |
| `dispatch`, `batch` (`dispatch`, `dispatch_batch`, `BatchResult`, `RollbackReason`) | 8+ | **UNSTABLE**: the engine's internals; `KulyraxRuntime` and `pipeline` are the API |
| `runtime` (`KulyraxRuntime`, `config`, `registry`, `validation`, `dry_run`, `reload`, `dependency_graph`, `factory`, `execute`, `result`, `error`, `engine`) | ~520 | **FREEZE** `KulyraxRuntime`, `config::*`, `registry::{AdapterRegistry, SourceRegistry, AdapterBuildContext, SourceBuildContext, MappingSet, …Factory}`, `dry_run` report types, `ValidationReport` / `ValidationIssue`; **UNSTABLE**: `dependency_graph` functions, `execute`, `result`, `factory` (build plumbing), `reload` internals except `request_reload` / `ReloadOutcome` |
| `pipeline` (`load_*`, `run`, `dry_run`, `explain`, `replay`, `rebuild`, `run_source_loop`, `build_sources`, `list_sources`, `SourceStatus`, `PipelineError`) | 59 | **FREEZE**: the documented front-end entry points (the CLI uses only these) |
| `rebuild` (`rebuild`, `RebuildReport`, `ShadowRebuild`, `SHADOW_SUFFIX`) | 18 | **FREEZE** |
| `replay` (`ReplayReport`, `ReplayRunOptions`, `ReplayContext`, `events_from_path`, `DEFAULT_REPLAY_BATCH`) | 111 | **FREEZE** reports and options; **UNSTABLE** `ReplayContext` (superseded by `KulyraxRuntime::replay`) |
| `source` (`EventSource`, `SourcedEvent`, `SourcePosition`, `SourceError`, `lock_source`, `checkpoint`, `directory`, `stdin`, `runner`) | 160+ | **FREEZE** `EventSource`, `SourcedEvent`, `SourcePosition` (opaque and unordered since step 4, D1: `Eq` and `Hash`, no `PartialOrd`/`Ord`), `SourceError`, `lock_source`, `checkpoint::{read,write}_checkpoint`, the runner's options and reports (`SourceRunOptions`, `RunMode`, `SourceRunReport`, `BatchSummary`, `StoppedReason`); **UNSTABLE**: `DirectorySource` / `StdinSource` constructors (built through config). `SourcePosition` is an opaque, unordered token ([producer contract §9](producer-contract.md#9-positions)); each source orders its own positions. Owner override of this table's earlier FREEZE-as-is entry: decision 2026-11-25-source-position-contract |
| `upcast` (`Upcaster`, `UpcasterRegistry`, `UpcastError`) | 56 | **FREEZE** |
| `scaffold` (`init_project`, `InitError`) | 10 | **UNSTABLE**: exists for `kulyrax init`; also re-exported as `pipeline::InitError` (one path, step 4) |
| crate-root re-exports (`validate_*`, `run_sources`, `SourceRunOptions`, …) | — | **CHANGE** (step 4): one canonical path per item; the root keeps the most-used ones |

**Errors** (step 4, D6, done; decision 2026-11-26-error-chains): every public
error implements `std::error::Error + Send + Sync + 'static`, and a test holds it
to that.
- `DependencyOrderError` implements `Error`.
- `AdapterReportError` is renamed `AdapterFailure`: it is report data, not an
  error.
- `source()` returns the typed cause where it is a YAML, JSON or I/O error, or
  another Kulyrax error:
  - `PipelineError::ConfigParse` holds a `YamlError`, and `EventParse` a
    `serde_json::Error`;
  - `Mapping` / `Routing` hold a `pipeline::LoadError`;
  - `ReplayLoadError::Json` holds a `serde_json::Error`.
- Driver and database causes stay redacted strings (`WriteFailed(String)`,
  `ExecutionFailed(String)`, `FactoryConfigInvalid(String)`): a raw driver
  error can print the connection URL (F-06).

### Other crates

| Item | Status |
|---|---|
| Each adapter crate's `factory` and `<Name>Config` | **FREEZE**: what `kulyrax-backends` and embedders use |
| `<Name>Adapter::new(…)` (positional arguments, up to 9) | **CHANGE** (step 4, D4, §9.4): `new` is `#[doc(hidden)]`. The public constructor is `<Name>Adapter::from_config(&cfg, &AdapterBuildContext)`; `cfg` is `#[non_exhaustive]`, built with `new(<required>)` and `with_*` setters (decision 2026-11-26-adapter-from-config) |
| `PostgresTxn`, `MySqlTxn` (pub, implement `SqlTxn`) | **UNSTABLE**: internals of their adapters |
| `kulyrax_ingest::pb`, `kulyrax_client::pb` (tonic/prost-generated) | **UNSTABLE** (step 4, D3, §9.3): private, exposed as `#[doc(hidden)]` only with the `unstable-pb` feature, outside semver. `kulyrax_client` owns `Producer`, `EventEnvelope`, `Ack`, `AckOutcome`, `ClientError`, `RpcError`, `StatusCode`; none names a tonic or prost type. The frozen contract is the `.proto` (§4) |
| `kulyrax_source_postgres::PostgresOutboxSource::{open, inspect}`, `WATCH_CAP` | **FREEZE** `factory`, `OutboxConfig`, `OutboxColumns`; **UNSTABLE** the rest |
| `kulyrax_backends::register_default_backends` and its features | **FREEZE** (feature names included) |

---

## 2. YAML

### Config (`config.yaml`)

| Key | Notes | Status |
|---|---|---|
| `version` | **already required**, must be `1` (`KulyraxConfig::validate`) | **FREEZE**, required (§9.2) |
| `routing.file` | path, relative to the config | FREEZE |
| `adapters[]`: `type`, `id`, `priority`, `capabilities` (`write`, `idempotent`, `upsert`, `transactions`, `delete`), `depends_on`, `config` | built-in types `sqlite` (`path`), `file` (`root`), `keyvalue` (`root`), `graph` (`root`); registered types `postgres` (`url`, `pool_size`), `mysql` (`url`, `pool_size`), `redis` (`url`, `pool_size`), `mongodb` (`url`, `database`, `pool_size`), `neo4j` (`uri`, `user`, `password`, `database`), `elasticsearch` (`url`, `username`, `password`, `timeout_ms`) | FREEZE; `${ENV}` expansion in `url` / `password` FREEZE |
| `failure_policy` | `fail_fast` (default), `continue_on_error` | FREEZE |
| `migration_policy` | `strict` (default), `ignore_unmatched` | FREEZE |
| `sources[]` | `directory` (`id`, `path`, `state_dir`), `stdin` (`id`, `state_dir`), `postgres_outbox` (`id`, `state_dir`, `config`: `url`, `table`, `sequence_column`, `columns.{event_id,event_type,payload,metadata,version}`, `gap_grace_ms`, `gap_timeout_ms`, `watch_retention_ms`, `allow_unverified_sequence`) | FREEZE |

Unknown keys: in 0.12 no built-in config or mapping struct rejected unknown keys, so
a misspelt key was silently ignored; only `OutboxConfig` rejected them. **CHANGE**
(step 4, D5, done): every built-in config, mapping and routing struct rejects
unknown keys. That covers:
- `config.yaml`: the top level, `routing:`, each adapter and source entry (custom
  ones too: core checks the keys it reads), and the `config:` blocks of the
  built-in and Kulyrax-shipped backends;
- the SQL, document, key-value and graph mapping files.

The error names the key, the file and the key's own line. A Kulyrax-shipped
backend's `config:` block is parsed by its factory after the file is read, so its
error names the key and the adapter id, but not the line. `routing.yaml` has no
fixed keys: each key is an event type. A third-party backend or source parses its
own `config:` block and documents its own rule (`docs/writing-a-backend.md`).

### Mappings (`mappings/{sql,document,keyvalue,graph}/*.yaml`, `version: <n>`)

`version` here is the **target payload schema version** (for upcasting), not a
file-format version: a mapping file has no format version of its own.

| Kind | Keys | Status |
|---|---|---|
| SQL | `event`, `table`, `version`, `primary_key` (one or many), `columns`, `on_existing` (`ignore` / `replace`), `operation` (`upsert` / `delete`), `permanent`, `facet`, `requires_capabilities` | FREEZE |
| Document | `event`, `collection`, `version`, `id`, `document`, `on_existing`, `operation`, `permanent`, `facet`, `requires_capabilities` | FREEZE |
| Key-value | `event`, `namespace`, `version`, `key`, `value`, … same tail | FREEZE |
| Graph | `kind: node` (`label`, `key`, `properties`, `facet`), `kind: edge` (`edge_type`, `from`, `to`, `discriminator`, `properties`), `kind: subgraph` (`event`, `version`, `records`) | FREEZE |
| Path expressions | `payload.<field>`, `metadata.<field>` | FREEZE |

### Routing (`routing.yaml`)

`EventType: [adapter-id, …]`. **FREEZE**.

All YAML is read by `kulyrax_core::yaml`, configured to read it exactly as 0.12
did (decision `2026-11-25-yaml-parser`, step 3).

---

## 3. CLI

Commands: `init [DIR]`, `run`, `sources`, `ingest`, `reload`, `explain`,
`dry-run`, `replay`, `rebuild`, plus `--version` / `--help`.

| Command | Flags |
|---|---|
| `run` | `--config`, `--mappings`, `--event`, `--once`, `--max-batch`, `--max-batch-delay-ms`, `--poll-interval-ms`, `--dlq`, `--output` |
| `ingest` | `--config`, `--mappings`, `--listen`, `--max-batch`, `--max-batch-delay-ms`, `--dlq`, `--output` |
| `replay` | `--config`, `--mappings`, `--events`, `--output`, `--max-per-event`, `--progress-every`, `--validate-routing`, `--max-batch` |
| `rebuild` | `--config`, `--mappings`, `--events`, `--adapter`, `--output`, `--progress-every`, `--max-batch` |
| `sources` | `--config`, `--output` |
| `reload` | `--config`, `--timeout-secs` |
| `explain` | `--config`, `--event`, `--mappings`, `--output` |
| `dry-run` | `--config`, `--mappings`, `--event`, `--output` |

| Item | Status |
|---|---|
| Command and flag names, defaults | FREEZE |
| Exit codes. 0.12: `0` success, `1` for everything else | **CHANGED** (step 4, D7, done) and FREEZE: the table below, tested through the CLI (`crates/cli/tests/exit_codes.rs`) |
| `--output json` documents (execution report, replay report, source-run report, dry-run report, `sources` list with `details`, `rebuild` `{tables, replay}`) | FREEZE their field names; they are what scripts parse |
| `--output text` and everything on stderr (progress, `kulyrax: source …` log lines) | **UNSTABLE**: human-readable |

### Exit codes (frozen at 1.0)

| Code | Meaning | When |
|---|---|---|
| `0` | Success | Everything the command was asked to do was done. |
| `1` | Internal or unexpected error | I/O failures, a database that failed, an adapter that could not be prepared (a server unreachable), `reload` that no running process answered. |
| `2` | Invalid input | Bad arguments (clap). `config.yaml`, a mapping or the routing file does not parse, has an unknown key or fails validation. A mapping file that targets a name Kulyrax keeps for itself ([mapping reference, Names](mapping-reference.md#names-kulyrax-keeps-for-itself)). A bad `--event` / `--events` file. An adapter or source factory refusing its config (an unknown `type:`, a bad `config:` block). A `rebuild` of an adapter that is not in the config, has no SQL mapping, or cannot rebuild. A `reload` refused because the new config is invalid. |
| `3` | Events failed or were dead-lettered | `run --event`, `run --once`, `replay` or `rebuild` with an event that failed. A `--once` run that dead-lettered any event. A halt after the retry budget. `dry-run` with an adapter that would fail. A continuous `run` or `ingest` ends with 3 only for failures it did not dead-letter. |
| `4` | Refused by a safety check | A source's lock, or a file store's lock, is held by another process. A `rebuild` refused before it starts (a table the swap would break; a mapped table named like a shadow when the mappings were built in code, since a mapping file naming one is refused at load, exit 2) or at the swap (another writer touched the tables). `init` would overwrite a file. A store or checkpoint in a format this version does not read, or a format marker that could not be created (§5). Nothing was changed. |

---

## 4. gRPC (`proto/kulyrax/v1/ingest.proto`)

`package kulyrax.v1`; `service Ingest { rpc Stream(stream EventEnvelope) returns
(stream Ack) }`; `EventEnvelope` fields 1–7 (`event_type`, `payload`, `metadata`,
`version`, `sequence`, `position`, `event_id`); `Ack` fields 1–4 (`position`,
`batch_events`, `dlq_events`, `ack_protocol_version`). **FREEZE** under protobuf
rules (no field renumbered or retyped; new fields only); `buf breaking` in CI
(step 4).

---

## 5. Persisted formats

Every format Kulyrax writes and reads back records its version (step 5,
decision [`2026-11-28-format-versions`](ledger/decisions/2026-11-28-format-versions.md)).
The rules, frozen at 1.0:

- **Where the version is.** One marker per store (a file-store root, a SQL
  schema or database, a MongoDB database, a Redis database, an Elasticsearch
  cluster, a Neo4j database), or a field inside a file that stands alone (a
  source checkpoint, the outbox state, a batch log).
- **No version means format 1.** Kulyrax 0.12.x wrote none. Such a store or
  file is read as format 1 and stamped on its next write; nothing else is
  migrated.
- **Newer is refused.** A version above the one this build reads refuses
  the start before anything is read, replayed or written. The error names the
  store or file and both versions, and the CLI exits `4` (§3). Content that is
  not a Kulyrax format is refused the same way, and left as it is.
- **The marker is created insert-if-absent**, atomically per backend:
  concurrent first starts leave one marker with one version. If it cannot be
  created, the start is refused (exit `4`), naming the store and the right it
  needs ([required grants](required-grants.md)).

| Format | Where | Shape | Version kept in | Version |
|---|---|---|---|---|
| SQL guard table | SQLite, Postgres, MySQL: `kulyrax_projection_state` | `target_table`, `entity_key` (JSON array text, e.g. `["u1"]`), `facet`, `last_sequence`, `last_event_id`, `deleted`, `permanent`, `processed_at`; PK `(target_table, entity_key, facet)`; MySQL key columns `utf8mb4_bin` | table `kulyrax_format (component, version)`, row `guards`, in the guard table's schema or database | 1 |
| Rebuild shadow suffix | SQL | `<table>__kulyrax_rebuild` | not versioned: transient, dropped or swapped | — |
| MongoDB guards | collection `kulyrax_projection_state`, unique index `(target, entity_key)` | `ProjectionGuard` BSON + `target`, `entity_key` | collection `kulyrax_format`, document `{_id: "guards", version}` | 1 |
| Neo4j guards | label `__KulyraxGuard`, uniqueness constraint `kulyrax_guard_key` | `kind`, `target`, `entity_key`, `facet`, `last_sequence`, `last_event_id`, `deleted`, `permanent` | node `(:__KulyraxFormat {component: 'guards', version})`, unique on `component` (constraint `kulyrax_format_component`) | 1 |
| Redis | values `<ns>:<key>`, guards `__kulyrax:guard:<ns>:<key>` (JSON `KvGuard`) | JSON | hash `__kulyrax:format`, field `guards`, one per Redis database | 1 |
| Elasticsearch | guard inside each document, field `__kulyrax_guard` | JSON | index `kulyrax_format`, document `guards` `{version}`, one per cluster | 1 |
| File document, key-value and graph stores | `<root>/<collection>/<id>.json`, `<root>/<ns>/<key>.json`, `nodes/<label>/<id>.json`, `edges/<type>/<hex key>.json`; guards `.kulyrax/{entities,kv,guard}/…`; `.kulyrax/incident/<id>.json` | JSON | the layout marker `.kulyrax/layout/<kind>` (`kv`, `document`, `graph`): text, the version. Every marker on a root is checked before its batch logs are replayed. `1` (unreleased builds) and an empty marker (an older build killed mid-write) read as `2` and are re-stamped atomically on open | 2 |
| Batch log | `.kulyrax/wal/<seq>.log` (+ `.log.tmp`) | magic `KLXWAL<nn>`, body length u64 LE, CRC-32, entries (tag, path, content) | the magic's two digits. A complete magic that is not this version's is refused, never deleted as torn | 1 (`KLXWAL01`) |
| File-store lock | `.kulyrax/lock` | empty file, OS lock | not versioned: no content | — |
| Shared `.kulyrax/` | store root and config folder | entries owned by `kulyrax_core::kulyrax_dir`: stores `wal`, `lock`, `layout`, `kv`, `entities`, `guard`, `incident`; reserved `control`, `state` | — | — |
| Source checkpoint | `<state_dir>/<id>.json` | `{format, position, committed_at}`; positions: directory `<file>#<index:012>`, stdin zero-padded line, outbox: its state (below), each ordered only by its own source ([producer contract §9](producer-contract.md#9-positions)) | field `format` | 1 |
| Outbox state | inside the checkpoint's `position` string | JSON `{format, after, watch[{seq, skipped_at_ms}], late_seq, late_rows, late_skipped}` | its own field `format` (its shape is the outbox source's) | 1 |
| Source lock | `<state_dir>/<id>.lock` | OS lock | not versioned: no content | — |
| DLQ | `<dlq_dir>/<event id stem>~<position stem>.json` (stems as in [mapping-reference, Names](mapping-reference.md#names); `~2`, `~3`, … when the name is taken; a hash when too long) | `{event, report}` | not versioned: Kulyrax writes it and never reads it back | — |
| Reload control | `<config dir>/.kulyrax/control/reload-request`, `reload-result` | text, `<token>\n<applied\|rejected\|not-picked-up>\n<message>` | not versioned: **UNSTABLE**, a protocol between two Kulyrax processes of the same version | — |

---

## 6. The old name

Step 2 renamed every persisted, user-visible and code name to `kulyrax`, with an
in-place migration at start-up and a tombstone under each old name: see
`RELEASES/v1.0.0-rc.1.md` and the step 2 decision record in `docs/ledger/`. CI fails on the old name
outside the migration code, its tests, the release notes and the ledger.

---

## 7. Findings

- **F1**: the file stores' internal layout is restructured before 1.0 so
  that Kulyrax's own files and the directories named by mappings never share a
  path, and mapping names that could reach them are refused at validation.
- **F2**: the config file already has a required `version: 1` (§2).
- **F3**: v0.11.0 was never tagged, and `CHANGELOG.md` stops at v0.10.0.
- **F4**: error types flatten their causes into strings; a `source()` chain (step 4)
  means changing those variants, which is itself an API change.

---

## 8. Order of changes

| Step | Changes from this page |
|---|---|
| 2 | rename to `kulyrax` (§5, §6); done |
| 3 | YAML parser replacement (§2); done |
| 4 | `#[non_exhaustive]` (including `Event`), one path per item, UNSTABLE items hidden, adapter constructors from config, `pb` hidden, errors (`Error` on all, `source()`, `AdapterReportError` renamed), `deny_unknown_fields` everywhere built in, `missing_docs`, `cargo-semver-checks` and `buf breaking` in CI, the exit codes of §3; done |
| 5 | format versions (§5), reserved names; done |
| 6 | this page final (§0), the 1.0 API snapshot (`stability/api-1.0/`), `SECURITY.md`, the README's status and known limits, CONTRIBUTING's testing section |

---

## 9. Decisions (owner, 2026-11-24)

1. **LICENSE**: both files name "Kulyrax Contributors" (done).
2. **Config `version`**: stays required; step 5 adds nothing to it.
3. **Generated gRPC code**: hidden. The frozen contract is
   `proto/kulyrax/v1/ingest.proto`, not the generated Rust. `Producer`, `Ack`,
   `AckOutcome` and `EventEnvelope` become Kulyrax-owned types that convert to and
   from `pb` internally. `pb` stays reachable only as `#[doc(hidden)]` behind an
   `unstable-pb` feature, documented as outside semver. A tonic or prost upgrade
   must never force a major release.
4. **Adapter constructors**: `new` is hidden. Each adapter gets
   `Adapter::from_config(cfg)`; its config struct is `#[non_exhaustive]`, built with
   `Default` plus setters or a builder. That is the only public way to build one.
5. **Exit codes**: split before freezing, documented here and tested through the
   CLI (§3).
6. **`Event`**: `#[non_exhaustive]`, with `Event::new(event_id, event_type,
   sequence, payload)` and `with_metadata` / `with_version`. It is the struct most
   likely to grow (causation id, tenant, timestamp); a field added after 1.0 would
   otherwise force 2.0. The tests and examples migrate to the constructor.

Further decisions:

- **Unknown keys**: before 1.0, every built-in config, mapping and routing struct
  rejects unknown keys (`deny_unknown_fields`); the error names the key and the file,
  with a line number when the parser gives one. Extension configs (custom sources and
  backends) are parsed by their own crate, which documents its own rule. Step 4, with
  a test for a misspelt key in each file kind.
- **Errors**: `source()` chains in step 4. `DependencyOrderError` implements
  `std::error::Error`. `AdapterReportError` is report data and is renamed to a name
  that does not end in `Error`.
- **Duplicate export paths**: one canonical path per type. The others are removed, or
  kept as `#[doc(hidden)]` re-exports noted as removed at 1.0.
- **`docs/stability/api/`**: reference only. Gates stay on stable Rust; CI runs
  `cargo-semver-checks` on stable, not nightly.
- Every other proposal on this page is approved where it does not conflict with the
  above.
