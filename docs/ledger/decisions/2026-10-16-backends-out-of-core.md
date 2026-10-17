---
status: proposed
---

# Networked backends live in their own crates and resolve through the registry

Task: [Finish the adapter crate split and make the workspace navigable](../tasks/2026-10-16-adapter-crate-restructure.md)

## Context

Backends are reached through two different mechanisms. SQLite, file, KV, graph, MySQL, Redis, MongoDB and Neo4j are hard-wired into `AdapterConfig` and `runtime/factory.rs`. Postgres and Elasticsearch are separate crates registered through `AdapterRegistry`. A reader cannot find every adapter in one place, and core still pulls in four network drivers plus `tokio` (for Neo4j), even though CLAUDE.md says dispatch is synchronous with no tokio.

## Options considered

1. **Finish Phase 28: move every networked backend into its own crate**: one extension path, which is the same one a third-party backend uses; core builds faster and stays sync. Cost: public API break, plus four mechanical extractions.
2. **Move Postgres and Elasticsearch back into core behind Cargo features**: no new crates. Cost: core becomes harder to read again, and the registry path stays mostly unused.
3. **Leave it as is and document it**: no work. Cost: the confusion that prompted this task remains.

## Evidence

- Verified: `kulyrax-backends/Cargo.toml` says mysql/redis/mongo/neo4j "join `all` as each backend's own crate lands". The split was intended and is unfinished.
- Verified: `kulyrax-core/Cargo.toml` depends on `redis`, `mongodb`, `neo4rs`, `tokio` and `mysql`.
- Verified: an unmerged branch, `claude/phase-28-mysql-crate`, already did the MySQL extraction under the old crate names.
- Assumed: no external embedder matches on `AdapterConfig::MySql` and the other networked variants.

## Decision

Option 1. SQLite and the file-backed adapters stay in core as reference implementations. All networked backends move to `crates/adapters/<name>/`. Factories receive an `AdapterBuildContext`, so they no longer depend on being registered after mappings load.

## Trade-offs accepted

- Embedders that matched on the removed enum variants must update their code.
- A config typo such as `type: msyql` now fails as "unregistered adapter type" at build time. Today it fails as a serde parse error.

## Revisit if

- A backend needs compile-time integration that the registry cannot express. For example, if validation or a dry-run preview needs knowledge specific to one backend.
