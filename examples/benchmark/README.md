# Benchmark

Measures how fast Kulyrax projects events into each backend, where the time goes,
and how long an event waits before its projection is committed.

Every run projects `UserRegistered` events, one new entity each (so every event is a
real insert), from a directory source into an empty store, then checks that every
entity landed. The networked backends run in Docker (`docker-compose.yml`, on host
ports that don't collide with a local install); each run recreates the container.

| Script | Measures |
|---|---|
| `run.sh <backend> [events] [per_file]` | Sustained throughput of `kulyrax run --once` over N events (default 100,000, in files of 1,000), plus time per stage. |
| `lag.sh <backend> <rate> [seconds] [poll_ms]` | Projection lag: `kulyrax run` runs continuously while `benchtool` publishes `rate` events/s; reports p50/p90/p99/max from file publish to batch commit. |
| `rebuild.sh [events] [rate] [seconds]` | Postgres: rebuilds N events into a second database while a live run ingests at `rate`; reports rebuild throughput and live lag during it. |
| `suite.sh [events]` | All of the above for every backend: throughput, then lag at half that throughput, then the rebuild. Writes `results/`. |

Backends: `sqlite`, `postgres`, `mysql`, `redis`, `mongodb`, `elasticsearch`, `neo4j`,
and the file-backed `file-document`, `file-keyvalue` and `file-graph`.

```sh
bash run.sh postgres            # 100,000 events
bash lag.sh postgres 15         # 15 events/s for 180 s
bash suite.sh                   # everything; several hours at current speeds
```

Set `KULYRAX=/path/to/kulyrax` to time a prebuilt binary; otherwise the scripts build a
release binary of this checkout once, before any timing starts.

## Reading the stage table

`kulyrax run` prints time per stage when it finishes (`timings` in `--output json`).
The stages nest:

- `source read`: polling the source (reading and parsing event files).
- `dispatch`: projecting a batch, which contains every `adapter` call.
- `adapter`: one adapter handling one event, which contains:
  - `schema`: SQL only, the `CREATE TABLE IF NOT EXISTS` for the guard table.
  - `read`: guard reads that feed `decide()`.
  - `write`: the target write and the guard write.
  - `commit`: the transaction commit (SQL, MongoDB, Neo4j; the file stores, Redis and
    Elasticsearch commit inside `write`).
  - what's left: building the mapping, checking out a connection, beginning a
    transaction.
- `checkpoint`: advancing the source position after a batch.

Each event is projected in its own durable transaction per adapter, so throughput is
usually bounded by how fast the storage can flush a commit. That varies a lot between
machines, operating systems and Docker setups: always report the CPU, disk, OS and
database version with a number (`suite.sh` writes `results/machine.txt`).
