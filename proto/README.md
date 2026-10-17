# The Kulyrax ingestion contract

[`kulyrax/v1/ingest.proto`](kulyrax/v1/ingest.proto) is Kulyrax's technology-agnostic
producer contract: one bidirectional streaming RPC. A producer in any
`protoc`/`buf`-supported language streams `EventEnvelope`s in and reads `Ack`s back —
nothing about the wire format is Rust-specific. [`kulyrax-client`](../crates/client/)
is the reference implementation; this file is the language-agnostic entry point for
everyone else.

Versioned: a breaking change gets a new `kulyrax/v2` package, so a `v1` producer never
silently breaks against a newer server.

## Generating a client

**Go** (`protoc-gen-go` + `protoc-gen-go-grpc`):
```sh
protoc --go_out=. --go-grpc_out=. proto/kulyrax/v1/ingest.proto
```

**Python** (`grpcio-tools`):
```sh
python -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. \
  proto/kulyrax/v1/ingest.proto
```

**Java** (`protoc-gen-grpc-java` plugin):
```sh
protoc --plugin=protoc-gen-grpc-java=<path-to-plugin> \
  --java_out=. --grpc-java_out=. proto/kulyrax/v1/ingest.proto
```

**C#** (`Grpc.Tools`, typically driven from `.csproj` `<Protobuf>` items, or directly):
```sh
protoc --csharp_out=. --grpc_out=. --plugin=protoc-gen-grpc=<path-to-grpc_csharp_plugin> \
  proto/kulyrax/v1/ingest.proto
```

**Node** (`@grpc/proto-loader`, no codegen step — load the `.proto` at runtime):
```js
const packageDef = protoLoader.loadSync("proto/kulyrax/v1/ingest.proto");
```

Or with [`buf`](https://buf.build) instead of bare `protoc`, point any of these
plugins at a `buf.gen.yaml` and run `buf generate` — the `.proto` itself needs no
`buf`-specific annotations.

## How to be a producer

Every generated stub differs in shape, but the sequence is always the same five steps.
In pseudocode:

```text
client = IngestClient.connect("host:port")
stream = client.Stream()                     # opens the bidirectional RPC

last_acked_position = load_from_local_state() # "" / null on first run

for event in events_after(last_acked_position):
    stream.send(EventEnvelope{
        event_type: event.type,
        payload:    json_encode(event.payload),
        version:    event.schema_version,     # defaults to 1 if omitted
        sequence:   event.per_entity_sequence, # see docs/concepts.md — load-bearing
        position:   event.resume_token,        # opaque to Kulyrax, yours to interpret
    })

async for ack in stream.responses():
    persist_locally(ack.position)              # durable *before* advancing past it
    last_acked_position = ack.position
```

The two fields worth getting right:

- **`sequence`** must strictly increase for successive events about the *same* entity —
  it's what gates idempotent writes on the server. See
  [`docs/concepts.md`](../docs/concepts.md#sequence-vs-position--two-different-numbers).
- **`position`** is yours: a Kafka offset, an outbox row id, anything that lets you
  resume `events_after(...)` after a restart. Kulyrax only ever echoes back the highest
  one whose batch is durably committed — persist it before treating that batch as done.

See [`docs/producer-contract.md`](../docs/producer-contract.md) for the complete set
of ordering and delivery assumptions, and
[`crates/client/src/lib.rs`](../crates/client/src/lib.rs) for a full,
real implementation of the same five steps in Rust.
