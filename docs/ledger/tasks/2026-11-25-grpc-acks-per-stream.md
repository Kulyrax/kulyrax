---
status: proposed
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
(proposed; wire format unchanged).

## Acceptance criteria

- [x] Failing tests on main: an idle producer; interleaved producers with one
  failing event; numeric offsets.
- [x] Decision record. **STOP** for the owner.
- [ ] After approval: the fix, the proto comments, release notes, the tests
  passing.

## Verification

| Check | Command | Result |
|---|---|---|
| Ack fan-out | `crates/ingest/src/lib.rs` | `ack_tx: broadcast::Sender<AckMsg>`; every stream subscribes; `commit_with_dlq_count` sends one `AckMsg` (position, batch_events, dlq_events) to all |
| Ack position | `source/runner.rs` | the committed position is the maximum (`Ord` on the position string) over every event in the batch, across producers |
| Tests | `cargo test -p kulyrax-ingest --test grpc_producers -- --test-threads=1` | 3 of 3 fail, as expected. Idle B acked `a-0002`; A received `b-0002`; B told `0009` is committed |

## Risks and open questions

- Idle streams stop receiving acks (they never should have relied on them).
- R3 (the source-position contract) overlaps: with this fix the gRPC ack no
  longer uses the run loop's text-wise maximum, but the directory and stdin
  sources still do.
