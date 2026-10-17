# grpc-producer

The gRPC ingestion deployment mode: run `kulyrax ingest` as a long-lived
service and stream events into it from any producer over
`proto/kulyrax/v1/ingest.proto`, instead of feeding it files through a
directory or stdin source. This example's producer is
[`kulyrax-client`](../../crates/client/), the reference Rust SDK —
the same ~20 lines of glue any `protoc`-generated client reproduces.

```sh
bash examples/grpc-producer/run.sh
```

This starts `kulyrax ingest --listen tcp://127.0.0.1:50061` in the
background, runs [`producer/src/main.rs`](producer/src/main.rs) against it
(two `UserRegistered` events), waits for both acks, stops the server, and
diffs the projected table against [`expected/users.txt`](expected/users.txt):

```
OK — app.db.users matches expected/users.txt:
u1|Ada
u2|Grace
```

Not wired into CI — a backgrounded server on a fixed port is more flake risk
than the other examples' one-shot `--once` runs are worth there — but it
needs no external infrastructure and runs the same way locally as
[`examples/sqlite-quickstart/`](../sqlite-quickstart/).

## Notes

- `config.yaml` has no `sources:` block: gRPC ingestion bypasses the
  file-source path entirely.
- `position` in each `EventEnvelope` is the producer's own opaque offset —
  Kulyrax never interprets it, only echoes the highest committed one back in
  an `Ack`. This producer waits for the ack covering `"offset-2"` before
  exiting, which works regardless of whether the two events land in one
  batch or two, since acked positions only move forward.

## Next

- [`docs/concepts.md`](../../docs/concepts.md) for the event envelope and
  position model.
- [`docs/producer-contract.md`](../../docs/producer-contract.md) for the
  full producer contract this example implements a slice of.
