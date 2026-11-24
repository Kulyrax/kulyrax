# Roadmap

Where Kulyrax stands and what's still open. Not a commitment or a timeline — a map of
what's built, and candidate future work.

## Where things stand

All four storage kinds — SQL, document, key-value, graph — have a real database
backend (SQLite, Postgres, and MySQL for SQL; Redis for key-value; MongoDB for
document; Neo4j for graph), every one of them behind the identical sequence-gated
idempotency model (`decide()`, `docs/concepts.md`). The engine has a symmetric source
side (file, stdin, directory, and a technology-agnostic gRPC ingestion contract), and
the four backend traits behind every storage kind are a public, documented extension
point (`docs/writing-a-backend.md`) — adding a database Kulyrax doesn't ship is a new
crate, not a fork.

Everything above ships in the open-source engine. See `architecture.md` §1.4 for how
that's separated from a possible future hosted/managed product.

## Open work

- **Native broker / outbox sources** — Kafka, Kinesis, and Postgres-outbox
  `EventSource` implementations. Stays open source under the engine/product split —
  built when there's real demand, not on a fixed schedule.
- **Multi-node coordination** — not designed yet; would stay open source if built.
- **The hosted product** — a managed offering (control-plane UI, backups,
  provisioning) running the open-source engine as a service. A separate build, not
  part of this repo.
- **A real out-of-tree community connector** — the extension point has been proven
  with an in-tree backend built the same way an external one would be; an actual
  independent crate (a DynamoDB or Elasticsearch backend, say) exercising the registry
  from outside this repo is still open. See `docs/writing-a-backend.md` if you want to
  be the one to build it.
- **Subgraph-per-event** — a single mapping emitting several graph records (a node plus
  its edges) atomically from one event. Currently a graph mapping produces exactly one
  node or one edge; a multi-record event is decomposed into separate events upstream.
- **TTL for the key-value adapters** — deferred because time-based eviction isn't
  reproducible from a replayed event log; would need an explicit non-deterministic-by-
  design opt-in if ever added.
- **I/O throughput, after v0.11.0.** v0.10.0 batches SQLite and Postgres (one
  transaction per batch; Postgres writes each batch in bulk). v0.11.0 makes the
  file stores crash-safe and batched (`docs/file-store-durability.md`), and
  batches every other backend: MySQL, MongoDB and Neo4j commit one transaction
  per batch, Redis one `MULTI`/`EXEC`, Elasticsearch one `_bulk`. Still open, in
  order:
  - a dedicated rebuild path: `COPY` into a shadow table, then an atomic swap,
    instead of rebuilding through the per-event guards;
  - benchmarks on a clean Linux VM with NVMe. Numbers so far are internal
    baselines from a Windows laptop, where Postgres spends ~80 ms per 1,000-row
    flush outside the server (most likely Docker Desktop's networking; to be
    measured there).

## Contributing to any of this

See [`CONTRIBUTING.md`](CONTRIBUTING.md), and specifically
[`docs/writing-a-backend.md`](docs/writing-a-backend.md) for adding a database backend.
