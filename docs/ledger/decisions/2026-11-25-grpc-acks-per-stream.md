---
status: accepted
---

# Acknowledge each gRPC stream for its own events only

Task: [2026-11-25-grpc-acks-per-stream](../tasks/2026-11-25-grpc-acks-per-stream.md)

## Context

Review finding F-02. Every producer's `Ingest.Stream` feeds one shared channel into
one `GrpcSource` (`crates/ingest/src/lib.rs`). On each commit, the source
broadcasts one `Ack` to **every** open stream (`ack_tx: broadcast::Sender`). That
ack carries:
- the batch's position, which the run loop computes as the text-wise maximum over
  all producers' events (`source/runner.rs`);
- `batch_events` and `dlq_events` for the whole mixed batch.

The proto defines `Ack.position` as "the highest position whose batch's guards
are now committed. The producer resumes from here after a disconnect". A
producer that acts on an ack meant for another producer resumes from a position
that is not its own. With offsets it compares, it skips events Kulyrax has never
received.

The client API is frozen at runbook step D3, so this has to be settled first. Proto
v1 must stay wire-compatible, or change now.

## Options considered

- **A. Per-stream acks over the existing wire format (recommended).**
  - The source keeps, for each event it polls, which stream sent it.
  - On commit, it sends each stream that has events in the committed batch one
    `Ack`, holding: the position of that stream's **last** event in the batch,
    in the producer's own send order (no comparison across producers, and none
    by text); that stream's event count; and its dead-lettered count, counted
    from the per-event outcomes the run loop reports before every commit
    (`EventSource::on_resolved`).
  - Streams with no events in the batch get nothing.
  - No field changes, so old clients keep working. Only the meaning of an ack
    narrows, from "the batch" to "your events in the batch", and the proto
    comments say so.
- **B. A producer id in the protocol.** A new `EventEnvelope.producer_id`, with
  acks keyed by it. This would let a producer recognise its acks across
  reconnects, but it is a wire change and needs every producer to send an id.
  Within one stream it adds nothing to A: a stream already is one producer.
- **C. Keep the broadcast, document that acks are global.** It leaves the
  failure in place for every multi-producer deployment and contradicts the
  proto's own "the producer resumes from here".

## Evidence

From `crates/ingest/tests/grpc_producers.rs`, against main, 2026-11-25, in
process (a `tonic` server and the reference `kulyrax-client`):

| Test | Result on main |
|---|---|
| An idle producer B (sent nothing) while A sends `a-0001`, `a-0002` | B received an ack for `a-0002` |
| A and B interleaved (`a-0001`, poison `b-0001`, `a-0002`, `b-0002`) | A received `b-0002` (the mixed batch's text-wise maximum, B's position) |
| Numeric offsets: A sends `0009`; B, which will send `0005`, is connected | B was told `0009` is committed |

All three assert the behaviour A gives; all three fail today.

## Decision

A.
- **Where the change lives:** `crates/ingest/src/lib.rs` only.
  - The receive channel carries `(stream id, event)`.
  - The source records `(stream id, position)` for each polled event, in poll
    order.
  - Its `on_resolved` records each event's outcome, and its commit groups the
    events since the last commit by stream and sends each stream its ack.
  - The broadcast becomes one bounded channel per stream, removed when the stream
    ends.
- **Unchanged:** `decide()`, `dispatch.rs` and the dependency graph. The run loop
  is unchanged too: it already calls `on_resolved` for every event before
  `commit`.
- **Proto:** wire unchanged (`ingest.proto` v1). The comments on `Ack.position`,
  `batch_events` and `dlq_events` now say "this stream's events".
- **Reconnects:** a stream that ends before its batch commits receives nothing;
  the producer resumes from the last ack it did receive and resends, and the
  guards absorb the duplicates. This is what the protocol already promises.
- **Release notes for 0.12.2:** a behaviour fix. Old clients keep working, and
  multi-producer deployments get correct acks.

## Trade-offs accepted

- A stream receives an ack only for batches that hold its events. An idle
  stream, which used to see every commit, now sees none. Nothing should have used
  those as a liveness signal; the release notes say so.
- The source keeps a small per-event record between poll and commit (stream id,
  position, outcome). It is bounded by the batch size.

## Revisit if

- Producers need to recognise their acks across reconnects, or to share one
  logical producer over several streams: then option B, as a new field, before
  or after 1.0 as an additive change.
