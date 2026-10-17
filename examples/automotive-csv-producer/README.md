# Automotive CSV Producer

A daily full-inventory CSV from a vehicle data provider, turned into
`VehicleCreated` / `VehiclePriceChanged` / `VehicleUpdated` / `VehicleSold`
events and projected by Kulyrax into **three real destinations**: PostgreSQL,
MongoDB, and Elasticsearch. The C# .NET Worker Service here is a producer —
it validates, diffs, and submits events over Kulyrax's existing gRPC
contract; it does not route events or write to any destination store itself.

## Capability assessment

This is what was actually true about Kulyrax *before* any of this example's
code was written, established by reading the repository (not assumed):

| Capability | Finding |
|---|---|
| Event contract | `EventEnvelope` (`proto/kulyrax/v1/ingest.proto`): `event_type`, `payload` (JSON string), `metadata`, `version`, `sequence` (monotonic **per entity**, gates every write), `position` (per-feed resume token), `event_id` (optional, non-load-bearing). |
| Event submission | **Existing and real.** Bidirectional gRPC `Ingest.Stream`, demonstrated end-to-end in `examples/grpc-producer/` and tested in `crates/ingest/tests/grpc_integration.rs`. A C# producer is a normal `Grpc.Tools`-generated client against the same `.proto` — no new service was needed. |
| Idempotency | Sequence-gated per `(target, entity_key, facet)`. A redelivered event with a sequence ≤ the stored one is silently dropped, never double-applied. Entity-keyed, not event-id-keyed (`event_id` is observability only). |
| PostgreSQL projection | **Existing, real.** `kulyrax-adapter-postgres`, pessimistic locking, real multi-writer safety. |
| MongoDB projection | **Existing, real.** `kulyrax-adapter-mongodb`, session-transaction based. |
| Elasticsearch projection | **Missing entirely** before this example — only named in a doc-comment as a hypothetical. **New work**: `kulyrax-adapter-elasticsearch`, a real `DocumentBackend` implementation (see "New Kulyrax capabilities" below). |
| Integration gaps | The gRPC `Ack` carried only `position` + an unlabeled `batch_events` count — a batch is acked whether every event in it succeeded or some were dead-lettered after exhausting Kulyrax's own retry budget. There was no way for a producer to tell those two cases apart. **New work**: an additive `Ack.dlq_events` field (see below). |

### New Kulyrax capabilities this example required

Two small, additive changes to the shared Kulyrax codebase were made —
*not* unrelated refactoring, specifically because this example's
correctness guarantees depend on them:

1. **`Ack.dlq_events`** (`proto/kulyrax/v1/ingest.proto` + the two in-crate
   copies, `kulyrax-core`'s `EventSource` trait, `kulyrax-ingest`). A new,
   non-breaking proto3 field. `dlq_events == 0` on the ack covering a
   position is the only condition under which every event up to that
   position was genuinely projected to every adapter it was routed to,
   rather than silently dead-lettered while the position still advanced.
   Existing sources (`directory`, `stdin`) needed zero changes — the new
   `EventSource::commit_with_dlq_count` method has a default body that
   just calls the existing `commit`.
2. **`kulyrax-adapter-elasticsearch`**, a new crate implementing the
   existing, public `DocumentBackend` extension trait (the same one
   MongoDB implements) — registered through the same factory-registry
   mechanism Postgres already uses. `kulyrax-core` itself was not touched
   for this. See its own module doc-comment for the storage-shape and
   concurrency design (guard + document live in one Elasticsearch
   document, since Elasticsearch has no multi-document transactions;
   concurrency uses `if_seq_no`/`if_primary_term` the way the others use a
   transaction-abort label or a conditional `SET`).

### What this producer can and cannot claim

- **Can claim:** an event is verified-projected once its ack's
  `dlq_events` (covering that event's own batch) is `0` — see
  `KulyraxIngestClient`'s doc-comment for why sending **strictly
  sequentially** (one event in flight at a time) is what makes a
  batch-level count attributable to one specific event at all.
- **Can claim:** a resend after *any* failure (a dropped connection, a
  timeout, an ambiguous "might have landed") is always safe to issue
  blindly, never a double-apply risk — Kulyrax's own per-entity sequence
  gate recognizes and skips the duplicate. This producer does exactly
  that rather than trying to first inspect whether the original attempt
  landed.
- **Cannot claim:** cross-database atomicity. If `VehicleUpdated` routes
  to all three stores and Postgres and MongoDB succeed but Elasticsearch
  fails, the *event* is reported as dead-lettered (not partially
  succeeded) — Kulyrax's own `ExecutionStatus` already treats any routed
  adapter's failure as the whole event failing, which is the most useful
  signal available, but the two successful writes are not rolled back.
  There is no multi-database transaction here or anywhere else in Kulyrax.

## Minimal architecture

```
Daily CSV → C# Worker (validate → diff → persist → submit) → Kulyrax (gRPC) → Postgres + MongoDB + Elasticsearch
                                      ↓
                         producer_ledger (Postgres) — the producer's OWN state
```

## Prerequisites

- Docker and Docker Compose
- (Only if you want to build/test the C# project outside Docker) the .NET 8 SDK

## One-command startup

```bash
cd examples/automotive-csv-producer
./scripts/run-demo.sh
```

This builds and starts Postgres (two databases: `kulyrax` for Kulyrax's
projections, `producer_ledger` for the producer's own state — one instance,
never confused with each other), a single-node MongoDB replica set (required
for the transactions the Mongo adapter uses), a single-node Elasticsearch,
`kulyrax ingest` (the real `kulyrax` CLI binary, built from this repo), and
the producer itself — then drops both sample snapshots in, ten seconds
apart.

## Processing the two sample snapshots manually

If you'd rather drive it by hand instead of `run-demo.sh`:

```bash
docker compose up -d --build
mkdir -p data/incoming
cp samples/2026-01-01.csv data/incoming/      # see below for why this must be an atomic rename in a real feed
docker compose logs -f producer                # watch it process
cp samples/2026-01-02.csv data/incoming/
```

A real provider must write to a temp name and rename the finished file in
— the producer only ever picks up `*.csv`, never a file still being
written. `cp` followed by the shell's own rename semantics on most
filesystems is good enough for this demo; `scripts/run-demo.sh` does the
same `cp` + `mv` to a `.tmp` name explicitly, to show the intended pattern.

## Expected events

| File | Vehicle | Event |
|---|---|---|
| 2026-01-01.csv | v1001, v1002, v1003, v1004 | `VehicleCreated` (first snapshot — everything is new) |
| 2026-01-02.csv | v1001 | `VehiclePriceChanged` (1,200,000.00 → 1,150,000.00) |
| 2026-01-02.csv | v1002 | `VehicleUpdated` (mileage 8000 → 8500) |
| 2026-01-02.csv | v1003 | *(none — unchanged)* |
| 2026-01-02.csv | v1004 | `VehicleSold` (absent from the second snapshot) |
| 2026-01-02.csv | v1005 | `VehicleCreated` (new in the second snapshot) |

## Verifying the projections

```bash
./scripts/verify.sh
```

prints the `vehicles` rows/documents from all three stores. Or by hand:

```bash
# PostgreSQL
docker compose exec postgres psql -U postgres -d kulyrax -c \
  "SELECT provider_vehicle_id, price, mileage, sold FROM vehicles ORDER BY provider_vehicle_id;"

# MongoDB
docker compose exec mongo mongosh --quiet kulyrax --eval \
  'db.vehicles.find({}, {_id:1, price:1, mileage:1, sold:1}).forEach(printjson)'

# Elasticsearch
curl -s 'http://localhost:9200/vehicles/_search?size=50&pretty'
```

`v1004` should show `sold: true` in all three (not a deleted row —
`VehicleSold` is an upsert that flips a flag, not a Kulyrax
`operation: delete`, specifically so it stays queryable here).

## Inspecting the producer's own ledger

```bash
docker compose exec postgres psql -U postgres -d producer_ledger -c \
  "SELECT file_name, status, failure_reason FROM processed_files;"
docker compose exec postgres psql -U postgres -d producer_ledger -c \
  "SELECT event_id, event_type, provider_vehicle_id, sequence, status FROM producer_events ORDER BY position;"
```

## Recovering from a failed file

The producer refuses to start a new file while an unresolved failure
exists (`processed_files.status = 'failed'`) — this is deliberate: silently
skipping to the next day's file would advance past unfinished or rejected
work. The file itself is moved to `data/failed/` for inspection (CSV
validation / drop-safeguard rejections) or left in `data/incoming/` (a
dead-lettered event mid-run, since some of that file's events already
landed for real and moving it would hide that). To retry after fixing the
underlying issue:

```bash
docker compose exec postgres psql -U postgres -d producer_ledger -c \
  "DELETE FROM producer_events WHERE file_name = '<file>'; DELETE FROM processed_files WHERE file_name = '<file>';"
  # producer_events first: it has a foreign key onto processed_files(file_name)
  # with no ON DELETE CASCADE, so deleting the parent row first fails with a
  # foreign-key violation (verified by actually running it the other way round).
```

then put the (corrected, if applicable) file back in `data/incoming/`.
There is deliberately no management API for this — the ledger is the
source of truth and a human resolves it directly, per the task's explicit
scope limits.

## Stopping the stack

```bash
docker compose down        # keep data/ and the Docker volumes
docker compose down -v     # also remove the Postgres/Mongo/Elasticsearch volumes
```

## Running the tests

```bash
cd tests/AutomotiveCsvProducer.Tests
dotnet test                                                   # CSV + snapshot-diff unit tests — no services needed
AUTOMOTIVE_LEDGER_URL="Host=localhost;Port=5432;Database=producer_ledger;Username=postgres;Password=postgres" \
  dotnet test                                                 # also exercises the ledger (needs the compose Postgres up)
AUTOMOTIVE_E2E=1 dotnet test --filter EndToEndDockerTest       # the full Docker-based round trip (needs the whole stack up)
```

## Limitations

- **No cross-database atomicity** — see "What this producer can and cannot
  claim" above.
- **Single producer instance.** The ledger's "resume the one in-progress
  file" logic assumes one producer process; running two against the same
  ledger concurrently is not supported (and the proto's own session model
  only promises disjoint entity sets across *concurrent* sessions, not
  protection against two producers racing the same ledger).
- **Drop safeguard is a flat fraction**, not calendar- or
  seasonality-aware — a real feed with genuine high turnover might need a
  smarter heuristic than "more than 50% missing is suspicious."
- **Validation's allowed `condition`/`listing_status` value sets are
  illustrative**, not the provider's real enumeration — change
  `CsvValidator`'s two `HashSet`s to match a real feed.
