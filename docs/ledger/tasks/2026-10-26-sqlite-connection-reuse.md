---
status: completed
---

# SQLite adapter: reuse one WAL connection

## Objective

The first benchmark run measured the SQLite adapter at 13 events/s on a laptop: 2,000 events in 151 s, all rows verified. It opened a new connection for every event and committed in SQLite's default rollback-journal mode, which flushes the disk several times per commit. Make it faster without weakening durability.

Out of scope: committing a whole batch in one transaction. That would be far faster, but it changes the per-event adapter contract.

## Acceptance criteria

- [x] One connection per adapter, opened on the first event (building the adapter still opens nothing).
- [x] Every committed projection is still flushed to disk before the commit returns.
- [x] No panics: a poisoned lock or a failed open fails that event with an error.
- [x] fmt, clippy `-D warnings` and `cargo test --workspace` pass.

## Constraints and assumptions

- Constraint: Zero Panic Rule.
- Constraint: effectively-once projection. The source checkpoint advances right after a batch commits, so a projection lost after its commit would never be reprojected.

## Decisions

- WAL with `synchronous = FULL`: one log flush per commit instead of several, and the same power-loss durability as before. `synchronous = NORMAL` was rejected: it can lose the last commits on power loss, after the checkpoint has moved past them. The owner approved this choice (2026-10-26).
- `busy_timeout` of 5 s, so another reader of the file (such as a dump tool) doesn't fail writes instantly.

## Verification

| Criterion | Check | Observed result |
|---|---|---|
| Before | `bash examples/benchmark/run.sh sqlite 2000`, release build | 2000 events in 151.21 s = 13 events/s, 2000 rows verified |
| After | same command and machine | 2000 events in 42.80 s = 47 events/s, 2000 rows verified (3.6×) |
| Where the remaining time goes (Postgres, for comparison) | `run.sh postgres 2000` with `synchronous_commit` on, then off (diagnostic only, then reset) | 32 events/s, then 176 events/s: about 25 of 31 ms per event is the commit flush |
| Gates | clippy `-D warnings`; `cargo test --workspace --no-fail-fast` | clean; 106 test binaries, 312 passed, 0 failed |

Hardware: Intel Core i5-10210U (4 cores / 8 threads), 32 GB RAM, LITEON NVMe SSD, Windows 11 Pro 10.0.26200. Postgres 16.15 in Docker Desktop.

Inferred, not measured: SQLite's remaining ~21 ms per event is mostly the one WAL flush per commit, by analogy with the Postgres diagnostic.

## Risks and open questions

- WAL keeps `-wal` and `-shm` files next to the database while it's open, and doesn't work on network filesystems. This needs a line in the release notes.
- Throughput is bounded by one durable flush per event. The real fix is committing a source batch in one transaction per adapter (group commit), which is a design change to the per-event adapter contract.

## Outcome

The SQLite adapter reuses one WAL connection and is 3.6× faster on the benchmark, with the same durability.
