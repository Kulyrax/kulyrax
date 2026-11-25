---
status: in-progress
---

# Review F-02: gRPC acks reach every producer (runbook R2)

## Objective

Show whether an ack or committed position for producer A can reach producer B,
or let B believe its events are committed when only A's are. Owner's runbook R2:
test with two concurrent producers, interleaved, one failing; route every ack
only to the stream whose events it covers; a producer's committed position is
never advanced by another's events; settle before D3 freezes the client API;
a decision record; STOP before code if the proto changes.

Decision: [2026-11-25-grpc-acks-per-stream](../decisions/2026-11-25-grpc-acks-per-stream.md)
(accepted by the owner 2026-11-25; wire format unchanged).

## Acceptance criteria

- [x] Failing tests on main: an idle producer; interleaved producers with one
  failing event; numeric offsets.
- [x] Decision record. **STOP** for the owner.
- [x] After approval: the fix, the proto comments, release notes, the tests
  passing.
- [x] Every Docker backend suite green locally.
- [ ] CI green on the PR, merged.

## Verification

| Check | Command | Result |
|---|---|---|
| Ack fan-out | `crates/ingest/src/lib.rs` | `ack_tx: broadcast::Sender<AckMsg>`; every stream subscribes; `commit_with_dlq_count` sends one `AckMsg` (position, batch_events, dlq_events) to all |
| Ack position | `source/runner.rs` | the committed position is the maximum (`Ord` on the position string) over every event in the batch, across producers |
| Tests | `cargo test -p kulyrax-ingest --test grpc_producers -- --test-threads=1` | 3 of 3 fail, as expected. Idle B acked `a-0002`; A received `b-0002`; B told `0009` is committed |
| Fix | `cargo test -p kulyrax-ingest --test grpc_producers -- --test-threads=1` (after the fix) | 3 of 3 pass on the first run |
| Workspace, first run | `cargo test --workspace` | 1 failure: `grpc_integration::grpc_vs_directory_converge`, ack timeout. Cause: it sends `p1, p3, p2` and waited for an ack at `p3`, the old text-wise maximum. The stream's last position is now `p2`, its last send (the behaviour this fix makes). Test updated to wait for `p2`; the convergence assertion is unchanged |
| Gates | `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo build`/`test --workspace` | clean; 472 passed, 0 failed |
| Docker suites | `cargo test -p <crate> -- --test-threads=1` against local Postgres, MySQL, Redis, MongoDB, Elasticsearch, Neo4j | 0 failures: postgres 27, mysql 21, redis 14, mongodb 17, elasticsearch 11, neo4j 19, source-postgres 14, ingest 12, cli 16. Rerun with `--nocapture`: no test printed a skip notice (the "skip" lines are `Skipped` outcomes and outbox gap logs) |

## Risks and open questions

- Idle streams stop receiving acks (they never should have relied on them).
- A producer that stops reading its acks loses acks past 256 queued
  (`ACK_BOUND`), as it did with the broadcast. The acks still queued carry
  positions it has really committed, so it resumes early, never late.
- R3 (the source-position contract) overlaps: with this fix the gRPC ack no
  longer uses the run loop's text-wise maximum, but the directory and stdin
  sources still do.
