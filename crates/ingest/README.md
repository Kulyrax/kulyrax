# kulyrax-ingest

Kulyrax's gRPC ingestion server: a `tonic` server implementing the
[`kulyrax/v1`](https://github.com/Panchauly/kulyrax/blob/master/proto/kulyrax/v1/ingest.proto)
`Ingest` contract, plus `GrpcSource` — an `EventSource` (from `kulyrax-core`) that a normal
`kulyrax_core::run_sources` loop drives like any file source. `tonic` lives only in this
crate; `kulyrax-core` stays gRPC-free.

Producers stream `EventEnvelope`s and get back position `Ack`s once each batch's guards
commit — Kulyrax holds no ingestion checkpoint; the producer owns resume-from-ack.

Most users reach this through the `kulyrax ingest` subcommand in `kulyrax-cli`. To embed
the server directly:

```rust,no_run
use std::sync::atomic::AtomicBool;

let (server, source) = kulyrax_ingest::start("grpc", "tcp://127.0.0.1:50051").unwrap();
println!("listening on {:?}", server.local_addr);
// hand `source` to kulyrax_core::run_sources(...) alongside your other sources.
```

See [`proto/README.md`](https://github.com/Panchauly/kulyrax/blob/master/proto/README.md)
for the wire contract and how to generate a client in another language.

Licensed under either of [MIT](https://github.com/Panchauly/kulyrax/blob/master/LICENSE-MIT)
or [Apache-2.0](https://github.com/Panchauly/kulyrax/blob/master/LICENSE-APACHE) at your option.
