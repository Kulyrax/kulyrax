# Benchmark

Times `kulyrax run --once` projecting N `UserRegistered` events (one new entity each)
into SQLite or Postgres, from an empty table and a fresh source checkpoint, and checks
that every row landed.

```sh
bash run.sh sqlite 2000
DATABASE_URL=postgres://postgres:postgres@localhost:5432/postgres bash run.sh postgres 2000
```

Set `KULYRAX=/path/to/kulyrax` to time a prebuilt binary; otherwise it uses a release
build of this checkout. The time includes startup and validation, which is negligible
next to the writes.

Each event is projected in its own durable transaction per adapter, so throughput is
bounded by how fast the storage can flush a commit, and it varies a lot between
machines and operating systems. Committing a whole source batch in one transaction is
planned. When reporting a number, include the CPU, disk, OS and database version.
