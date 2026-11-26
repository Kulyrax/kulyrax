---
status: accepted
---

# Adapters are built with `from_config(cfg, &AdapterBuildContext)` (step 4, D4)

Task: [2026-11-26-api-adapter-config](../tasks/2026-11-26-api-adapter-config.md)

## Context

`stability.md` §9.4 (owner, 2026-11-24): "`new` is hidden. Each adapter gets
`Adapter::from_config(cfg)`; its config struct is `#[non_exhaustive]`, built with
`Default` plus setters or a builder. That is the only public way to build one."

Today each adapter has a positional `new(…)` with 6 to 9 arguments. Its
dependencies come from two places:
- **its entry in `config.yaml`:**
  - `id` and `priority`, at the entry level;
  - the backend's own settings, in the `config:` block;
- **the run:**
  - the mappings, the upcasters and the migration policy, which registry
    factories already receive as `AdapterBuildContext`.

A literal one-argument `from_config(cfg)` would have to carry the run's wiring
inside a YAML config struct.

## Options considered

The owner was asked on 2026-11-26, with three options:
- **`from_config(&cfg, &AdapterBuildContext)`** (recommended);
- a literal one-argument `from_config(cfg)`, with the mappings, upcasters and
  policy as setters on the config;
- `from_config(id, priority, &cfg, &ctx)`.

## Decision

The owner chose **`from_config(&cfg, &AdapterBuildContext) -> Result<Self,
ConfigError>`** for every adapter: the 4 built into core and the 6 networked
ones.

1. **`cfg` is the adapter's typed config:**
   - **Networked adapters (`PostgresConfig`, `MySqlConfig`, `RedisConfig`,
     `MongoDbConfig`, `ElasticsearchConfig`, `Neo4jConfig`):** the `config:`
     block, plus `id` and `priority`. Those two fields are `#[serde(skip)]`, so
     YAML still sets them on the entry, where they are today. The factory copies
     them in from the entry.
   - **Built-in adapters (`SqliteAdapterConfig`, `FileAdapterConfig`,
     `KeyValueAdapterConfig`, `GraphAdapterConfig`):** the list entry itself,
     which already holds `id`, `priority` and `config`.
2. **A builder.** Every config struct is `#[non_exhaustive]`. `new(…)` takes
   only the fields without a default, and each optional field gets a
   `with_<field>` setter. Examples:
   - `RedisConfig::new(url).with_id("cache").with_priority(2).with_pool_size(8)`;
   - `SqliteAdapterConfig::new(id, priority, SqliteConfig::new(path))`.

   There is no `Default` where a field is required: an empty URL or path is not
   a usable default. `OutboxColumns` keeps its existing `Default`.
3. **It returns `Result`**, including for adapters that cannot fail today. A
   check added later is then not a breaking change. `from_config` runs the
   checks the factory ran (an empty or unparsable URL), so the programmatic path
   and the YAML path validate the same way. Each factory becomes: parse the
   block, copy `id` and `priority` in, call `from_config`.
4. **The positional `new` is `#[doc(hidden)]`**, not removed. The tests keep
   using it. It is outside semver.
5. **The other config structs** become `#[non_exhaustive]` with a `new` and
   setters, as decision
   [2026-11-26-non-exhaustive-scope](2026-11-26-non-exhaustive-scope.md)
   item 4 deferred to D4:
   - `CustomAdapterConfig`;
   - the `Directory` / `Stdin` / `Custom` source configs;
   - `OutboxConfig` and `OutboxColumns`.

   The sources do not get a `from_config`: D2 hid their constructors (stability
   §1, UNSTABLE), and a new public constructor would be new API.

## Trade-offs accepted

- **Two arguments, not one.** The context is the same one a registry factory
  already gets, so it adds no new type.
- **`id` and `priority` on a networked config are invisible to serde.** Writing
  `id:` inside a `config:` block does not set the adapter's id. Once D5 adds
  `deny_unknown_fields`, it is rejected as an unknown key.
- **The built-in and networked shapes differ slightly**: an entry struct for one,
  a config block for the other. Both are "the adapter's own typed config", and
  making them alike would move YAML keys.

## Revisit if

- A third source of adapter inputs appears (beyond the entry and the run). It
  goes in `AdapterBuildContext`, which is `#[non_exhaustive]`.
