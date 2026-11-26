---
status: in-progress
---

# Adapters built with `from_config`; config structs `#[non_exhaustive]` (step 4, D4)

## Objective

Runbook D4: "Adapters: `from_config(cfg)` with `non_exhaustive` config structs
(builder or `Default` + setters); positional `new()` hidden." `stability.md` §9.4
makes `from_config` the only public way to build an adapter. Decision
[2026-11-26-non-exhaustive-scope](../decisions/2026-11-26-non-exhaustive-scope.md)
item 4 also left the other adapter, source and outbox configs to D4. This is the
last item of tranche 2 (D3, D4, D9).

Decision: [2026-11-26-adapter-from-config](../decisions/2026-11-26-adapter-from-config.md).

## Acceptance criteria

- [x] Each of the 10 adapters has `from_config(&cfg, &AdapterBuildContext) ->
  Result<Self, ConfigError>`, and its positional `new` is `#[doc(hidden)]`:
  - the 6 networked ones: Postgres, MySQL, Redis, MongoDB, Elasticsearch, Neo4j;
  - the 4 in core: SQLite, the file document store, the file key-value store,
    the file graph store.
- [x] Each factory builds through `from_config`, so YAML and code validate the
  same way:
  - the 6 registry factories: parse the block, copy `id` / `priority` in, call
    `from_config`;
  - core's `build_adapters_reporting_failures`.
- [x] `#[non_exhaustive]` with `new` + `with_*` on:
  - the 6 networked configs, with `id` / `priority` `#[serde(skip)]`;
  - `Sqlite`/`File`/`KeyValue`/`GraphAdapterConfig` and their `config:`
    structs;
  - `CustomAdapterConfig`;
  - `Directory`/`Stdin`/`CustomSourceConfig`;
  - `OutboxConfig` and `OutboxColumns` (`Default` + `with_*`).
- [x] Tests and examples migrated from struct literals to the builders.
- [x] New tests:
  - `tests/from_config.rs` in each networked crate (from_config, the factory,
    `id` inside the block ignored, an empty URL refused);
  - `crates/core/tests/adapter_from_config.rs` (the 4 built-ins; the builders
    against YAML);
  - `crates/sources/postgres/tests/config_builder.rs` (the outbox builders
    against YAML).
- [x] Docs:
  - `docs/writing-a-backend.md`, the factory example;
  - `docs/stability.md` §1;
  - the rc.1 notes, with before/after code.
- [x] Gates: fmt, clippy `-D warnings`, workspace tests, examples, every Docker
  backend suite.
- [ ] PR merged with the 12 required checks green.

## Constraints and assumptions

- No behaviour change: `from_config` runs the checks the factory already ran,
  and YAML keys are unchanged.
- The built-in adapters cannot fail to build today. Their `from_config` still
  returns `Result`, and the core factory sends an `Err` down the existing
  `AdapterBuildFailure` path.
- Sources get no `from_config`: D2 hid their constructors (decision item 5).

## Verification

| Check | Command | Result |
|---|---|---|
| Build, first run | `cargo build --workspace --all-targets` | first-run failures, all from the migration itself: E0639 on ~57 struct literals (expected; rewritten by script), then E0283 on `"x".into()` arguments to the `impl Into<String>` builders (cause: the rewrite kept the literals' `.into()`; stripped), then 6 literals the script could not read (an `Option` capabilities variable; done by hand) |
| fmt | `cargo fmt --check` | clean |
| clippy | `cargo clippy --workspace --all-targets -- -D warnings` | clean |
| Workspace tests | `cargo test --workspace` | 558 pass, 0 fail (529 before D4; the new tests add 29) |
| Examples | `cargo build` for the 6 example manifests; `cargo run` of `examples/embedded` | 0 errors each; `projected: users.u1.name = "Ada"` |
| Docker suites (Neo4j on 7688) | `docker-suites.sh` | postgres 34, mysql 29, redis 18, mongodb 25, elasticsearch 15, neo4j 24, source-postgres 16, ingest 15, cli 32: all pass, 0 fail |

## Risks and open questions

- Once D5 adds `deny_unknown_fields`, an `id:` written inside a `config:`
  block is rejected. Today it is ignored. `an_id_inside_the_config_block_is_not_the_adapter_id`
  then changes from "ignored" to "rejected".

## Outcome

In progress.
