# Kulyrax — Architecture

## 1. Overview

**Kulyrax** is an **event-first projection engine** that deterministically applies events to one or more storage models (SQL, document, key-value, graph) using **explicit mappings** and **strict runtime semantics**.

Kulyrax is designed to be:

* deterministic
* explicit
* replay-safe
* side-effect aware

It is intentionally **not** a streaming platform, a workflow engine, a database, an ETL/ELT tool, or a schema-inference system.

---

## 1.1 Scope boundary

Kulyrax's job is **projection only**. It takes an event payload, resolves where it goes, verifies it is safe to write, orders execution deterministically, and projects it into the target storage models without corrupting state. It does **not** produce, ingest, buffer, or store events — producing the event log is out of scope. (An event-store substrate that would own the log was considered and rejected; Kulyrax consumes a log it does not own.)

| Owner | Responsibilities |
|-------|------------------|
| **Application / command side** | business logic, command validation, state transitions, and emitting events — whether by synchronously invoking Kulyrax as a library or by pushing to a socket / broker |
| **Transport / external world** | durability, wire protocols, network transport, and consumer offsets — everything behind the `EventSource` trait |
| **Kulyrax** | resolve where an event goes (routing), verify it is safe to write (validation), order execution deterministically (dependency graph), project it into target storage models without corrupting state (`decide()` + per-lane guards) |

Everything Kulyrax relies on from the application and transport — a monotonic per-entity `sequence`, a stable canonical entity id, per-entity delivery order — it assumes and cannot verify. Those assumptions are catalogued in [`docs/producer-contract.md`](docs/producer-contract.md).

---

## 1.2 The three parts

Kulyrax is three parts, and each has a different dependency rule:

| Part | What it is | Dependency rule |
|------|-----------|-----------------|
| **1. Storage adapters** | speak each target store's own wire protocol (SQLite, Postgres, MySQL, Redis, MongoDB, Neo4j) | **Dependencies are forced** — the client already runs these databases; Kulyrax must speak them |
| **2. Mapping & verification** | declarative YAML — routing, mappings, capabilities, versions — validated before runtime | **Technology-agnostic, dependency-free** — `serde` over YAML, no engine lock-in |
| **3. Producer ingestion** | how events reach the engine | **No technology lock-in** — Kulyrax's own contract (a gRPC `.proto`); never "you must use Kafka / a database / a broker" |

Part 3's ingestion contract is Kulyrax-defined and neutral. Concrete transports — a gRPC stream, a file directory, later Kafka or Kinesis — are `EventSource` implementations behind that contract; none is privileged and none is a core dependency.

---

## 1.3 Deployment modes

| Mode | How events arrive | Use |
|------|-------------------|-----|
| **Embedded crate** — `kulyrax-core` linked into a larger service | direct function call (`execute_event`) — no protocol | the host service already owns the command path and wants projection in-process |
| **Sidecar binary** — one statically-linked `kulyrax` process (e.g. a Kubernetes pod sidecar) | the gRPC ingestion service (Unix domain socket in-pod, or TCP); the app streams events via generated stubs | language-agnostic; the app and Kulyrax are decoupled processes |

Both modes run the identical core engine. Ingestion is stateless — Kulyrax acks a position after each batch's guards commit; the producer owns replay from the last ack.

---

## 1.4 Distribution

Kulyrax separates **the engine** from **the operational product built on it**, and draws that line on capability, not on features:

- **Open source (MIT / Apache-2.0) — the complete engine.** Every storage kind, with a real backend, lives in this repo: SQL (SQLite, Postgres, MySQL), key-value (file, Redis), document (file, MongoDB), graph (file, Neo4j) — and a public extension point (`SqlTxn` / `KvBackend` / `DocumentBackend` / `GraphBackend` plus `KulyraxRuntime::register_adapter_factory`; see [`docs/writing-a-backend.md`](docs/writing-a-backend.md)) for anyone to add a store this repo doesn't ship. Every source — `file`/`stdin`/`directory`, the gRPC ingestion contract, and native broker/outbox connectors (Kafka, Kinesis, Postgres-outbox) when they're built. Multi-node coordination, if Kulyrax ever needs it, is engine capability and belongs here too. **Nothing that changes what Kulyrax can project is held back for a paid tier** — capability lives entirely in the open-source engine, and growing it doesn't require the core team.
- **The product is purely operational.** It runs the open-source engine as a managed service and adds nothing the engine itself lacks: hosted/managed Kulyrax instances, a control-plane UI (topology, execution monitoring, guard-state inspection — visualizing `kulyrax explain`, not extending it), multi-tenant provisioning, backups/DR of guard state, one-click deploy, SLAs and support. If it ever hosts ingestion, it's still just running `kulyrax-ingest` for you.
- **A possible further self-hosted add-on, undecided:** compliance-oriented features (SSO, audit logging, at-rest encryption) could exist as a separate self-hosted layer without contradicting this split. Not designed now; noted so it isn't confused with the operational product above.

---

## 1.5 Design principles

- Event-first — routing is by `event_type`, never content or metadata heuristics.
- Explicit over implicit — mappings are declared, not inferred; no hidden defaults.
- Deterministic execution — the same event log always produces the same projected state.
- Schema-driven mappings — every projection is a YAML declaration, not code.
- Fail fast — configuration errors surface at startup, not mid-run.

Every design decision is evaluated against one question: **can the behavior be explained deterministically by reading the code and configuration?** If the answer is no, the feature does not belong in Kulyrax.

---

## 2. Core Architectural Principles

### 2.1 Event-First

Events are the **only input** to Kulyrax.

* Routing decisions are based on `event_type`
* Storage adapters never inspect transport metadata
* No implicit behavior based on payload shape

```text
event → route → adapters → side effects
```

---

### 2.2 Explicit Mappings

All projections are declared explicitly:

* SQL mappings define tables, columns, and keys
* Document mappings define collection-level structure
* Key-value mappings define a namespace, key path, and value template
* Graph mappings define a node (`label` + `key`) or an edge (`edge_type` + `from`/`to`)

There is:

* no schema inference
* no auto-migration
* no dynamic interpretation

Configuration errors are surfaced **before runtime**.

---

### 2.3 Deterministic Execution

Given:

* the same event
* the same mappings
* the same routing rules

Kulyrax will:

* select the same adapters
* execute them in the same order
* produce the same side effects

Determinism is a **hard requirement**, not a best effort.

---

## 3. High-Level Components

### 3.0 Runtime Factory

Adapter construction is centralized in a **runtime factory** (`runtime::factory`).

Responsibilities:

* assemble concrete adapters from validated mappings
* own adapter defaults (paths, priorities)
* ensure single ownership of runtime builders

Non-responsibilities:

* routing decisions
* runtime execution
* dynamic configuration

The factory is a **composition boundary only**; it introduces no new behavior and does not affect runtime semantics.

---

### 3.1 CLI Layer

The CLI is a **control surface**, not a runtime container.

Commands:

* `run` — real execution: one-shot with `--event <file>`, or the continuous source loop (`--once` to drain and exit) when the config declares `sources:`
* `sources` — list configured sources and their committed positions
* `explain` — routing + dependency order for one event, no execution
* `dry-run` — full simulated execution, no writes
* `replay` — drain a directory / NDJSON file once (superseded by `run --once`; kept for compatibility)
* `ingest` — run the gRPC ingestion service, projecting whatever a connected producer streams in

The CLI performs:

* path resolution
* config loading
* error classification

It does **not** perform business logic (thin-CLI pattern — all engine logic lives in `kulyrax-core`).

---

### 3.2 Routing

Routing is:

* pure
* deterministic
* event-type driven

Events route by `event_type` through a config-backed routing table to a list of **adapter ids**, not storage kinds — one event type can target several adapters of the same kind (e.g. a master and a replica):

```rust
fn route_with_rules(event: &Event, rules: &HashMap<String, Vec<AdapterId>>) -> Vec<AdapterId>
```

Routing does **not**:

* read mappings
* inspect payloads
* depend on adapter availability

---

### 3.3 Dispatch

Dispatch is responsible for:

* ordering adapters by priority
* invoking adapters sequentially
* collecting results

Dispatch semantics:

* adapters run in dependency order (a topological sort over `depends_on`, then priority), not just priority
* on failure, `FailurePolicy` decides: `fail_fast` (default) stops the batch; `continue_on_error` runs every routed adapter
* downstream adapters of a failed dependency are skipped
* results are always reported

There is **no rollback** and **no cross-adapter transaction**.

---

## 4. Adapters

### 4.1 Adapter Contract

All adapters implement the same interface:

```rust
trait StorageAdapter {
    fn kind(&self) -> StorageKind;
    fn id(&self) -> &str;
    fn priority(&self) -> u32;
    fn handle(&self, event: &Event) -> AdapterResult;
}
```

Adapters are:

* synchronous
* blocking
* side-effecting

Every adapter's write is gated by one shared pure function, `decide()` (`adapter/mod.rs`): given the `Operation` (`upsert` / `delete`), the mapping's `on_existing` mode, the persisted guard state for that entity's lane (`last_sequence`, `deleted`, `permanent`), and the event's `sequence`, it returns a `WriteDecision` (`Insert` / `Update` / `Delete` / one of the skip variants). Every adapter — file-backed or backed by a real database — calls it unchanged: sequence gating, redelivery, tombstones, and resurrection are decided once, not once per backend.

The result is an `AdapterResult` carrying an `AdapterOutcome` — `Created` / `Updated` / `Deleted` / `Skipped(SkipReason)` / `Failed(AdapterError)`. A skip is not a failure; `SkipReason` (`AlreadyProjected`, `StaleSequence`, `AlreadyDeleted`, `Tombstoned`, `EntityAbsent`, `UnsupportedVersion`) is machine-readable and round-trips into the JSON execution report.

Adding a real database backend for a storage kind — or a kind this repo doesn't ship — means implementing one small trait (`SqlTxn`, `KvBackend`, `DocumentBackend`, or `GraphBackend`) and registering it; see [`docs/writing-a-backend.md`](docs/writing-a-backend.md).

---

### 4.2 SQL Adapter

Backends: SQLite, Postgres, MySQL.

Responsibilities:

* build SQL from mappings
* execute exactly one statement per event
* manage its own database connection

Non-responsibilities:

* migrations
* retries
* cross-adapter transactions

Idempotency:

* enforced via the `conduit_projection_state` guard table, primary-keyed on `(target_table, entity_key, facet)`
* stored in the same database, atomic with projection execution
* `on_existing: replace` builds `INSERT … ON CONFLICT (<pk>) DO UPDATE SET …` (`ON DUPLICATE KEY UPDATE` on MySQL, which has no `ON CONFLICT` syntax); a named facet restricts the `SET` list to its own columns

Postgres and MySQL hold a pessimistic row lock (`SELECT … FOR UPDATE`) across the guard read and write, so concurrent writers to the same entity serialize instead of racing.

---

### 4.3 Document Adapter

Backends: file, MongoDB.

Responsibilities:

* build document JSON via mappings
* determine where the document lives — keyed by `(collection, entity_id)`, not `event_type`
* write deterministically

Idempotency:

* enforced via a per-entity guard (`last_sequence` / `deleted` / `permanent`, plus a `facets` map for named-facet lanes) — a JSON sidecar file for the file backend, a guard document for MongoDB
* the file backend's guard is adapter-local, single-writer; MongoDB wraps the guard-and-document write pair in a real client-session transaction

---

### 4.4 Key-Value Adapter

Backends: file, Redis.

A flat namespace → key → value store: `{root}/{namespace}/{key}.json` (file) or `{namespace}:{entity_key}` (Redis), each with its own guard. The simplest storage kind — no query language, no schema. Same `decide()`, same guard shape, `facets` on the value.

---

### 4.5 Graph Adapter

Backends: file, Neo4j.

A directed property graph. Nodes and edges are *separately keyed records*, each an independent `decide()` lane: a node keyed by `node_id`, an edge by the canonical `[edge_type, from, to(, discriminator)]`. `operation: delete` on a node is a `DETACH DELETE` — it removes the node and every incident edge in one adapter operation (the file backend tracks a per-node incident index for this; Neo4j's own graph structure makes a separate index unnecessary). Nodes are entities (facets apply); edges are not.

---

## 5. Builders

### 5.1 Runtime Builders

Builders are **pure projection engines**.

Example:

```rust
DocumentRuntimeBuilder::build(event) -> serde_json::Value
```

Builders:

* do not perform I/O
* do not resolve paths
* do not manage idempotency

They are intentionally opaque.

---

## 6. Idempotency Model

### 6.1 Definition

Idempotency is **entity-keyed and sequence-gated**, never `event_id`-keyed alone.

A guard lane is `(target, entity_key, facet)`. Its state is `{ last_sequence, deleted, permanent }`. For each event, `decide()` compares the event's `sequence` to `last_sequence`:

* a **redelivery or superseded** event (`sequence <= last_sequence`) is `Skipped(StaleSequence)` — no write
* a **newer** `upsert` under `on_existing: replace` overwrites; under `ignore` an already-projected entity is `Skipped(AlreadyProjected)`
* a **newer** `delete` writes a tombstone (`deleted = true`); `permanent` tombstones reject every later event forever
* a newer `upsert` on a non-permanent tombstone **resurrects** the entity

---

### 6.2 Scope

* idempotency is adapter-local — no global coordination, no exactly-once *delivery*
* combined with an at-least-once source and a committed checkpoint, it yields **effectively-once projection**: the state after any sequence of crashes / restarts / redeliveries equals a single clean pass
* each adapter enforces the guard using storage-native mechanisms — a table row, a JSON sidecar, a document, a graph node

---

### 6.3 Failure Semantics

If an adapter:

* fails before recording the guard → retry is possible (the source re-polls the uncommitted batch)
* records the guard → replay of that event becomes a `Skipped(*)` no-op

There is no attempt to reconcile partial success across adapters within one event; the source-loop retry / dead-letter / halt decision is across *batches*.

---

## 7. Error Model

### 7.1 Adapter Results

Each adapter returns:

```rust
struct AdapterResult {
    adapter_id: String,
    kind: StorageKind,
    outcome: AdapterOutcome, // Created | Updated | Deleted | Skipped(SkipReason) | Failed(AdapterError)
    source_version: Option<u32>,
    projected_version: Option<u32>,
}
```

Errors are **observable and explicit**.

---

### 7.2 Outcome & Error Types

* `Created` / `Updated` / `Deleted` — a write happened
* `Skipped(SkipReason)` — no write; `SkipReason` says why (stale, already projected, tombstoned, entity absent, unsupported version)
* `Failed(AdapterError)` — `WriteFailed` or `UnsupportedVersion`

Errors are never swallowed. Skips are not errors.

---

## 8. Intentional Limitations

Kulyrax explicitly does **not** provide:

* distributed transactions or cross-adapter rollback
* exactly-once *delivery* (it provides effectively-once *projection* via idempotent sinks)
* async execution
* an event store / broker — it consumes a log it does not own (§1.1)
* schema inference or auto-migration
* backpressure, rate limiting, or flow control
* distributed / multi-instance coordination — one projector owns its sources' positions
* implicit behavior

The `run` loop does one deliberate exception, scoped to the source-consumption boundary: across-*batch* retry with a bounded budget, and a dead-letter queue for poison events. These are architectural constraints, not missing features.

---

## 9. Design Philosophy

Kulyrax prioritizes:

* correctness over convenience
* explicitness over magic
* local reasoning over global coordination

Every design decision is evaluated against one question:

> **Can the behavior be explained deterministically by reading the code and configuration?**

If the answer is no, the feature does not belong in Kulyrax.

---

## 10. Status

* Four storage kinds — SQL, Document, Key-Value, Graph — each with a real database backend alongside a file-backed default (nine backends total: SQLite, Postgres, MySQL, file + Redis, file + MongoDB, file + Neo4j)
* Three event sources — `directory`, `stdin`, and a gRPC ingestion service with a versioned, technology-agnostic contract
* A public extension point for adding further backends without forking the crate (see [`docs/writing-a-backend.md`](docs/writing-a-backend.md))
* Core architecture locked: the `decide()` contract, the guard model, and the adapter/source traits are the stable surface any future backend or source must preserve

See [`RELEASES/`](RELEASES/) for the version-by-version changelog and [`ROADMAP.md`](ROADMAP.md) for open work.

This document defines the **architectural contract** of Kulyrax.
