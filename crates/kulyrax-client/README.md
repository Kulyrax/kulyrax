# kulyrax-client

The reference Rust producer client for Kulyrax's gRPC ingestion contract
([`kulyrax/v1`](https://github.com/Panchauly/kulyrax/blob/master/proto/kulyrax/v1/ingest.proto)).
A thin wrapper over the generated `tonic` client — the shape a producer in any
`protoc`-supported language reproduces with generated stubs and ~20 lines of glue (see
[`proto/README.md`](https://github.com/Panchauly/kulyrax/blob/master/proto/README.md)).

```rust,no_run
# async fn example() -> Result<(), Box<dyn std::error::Error>> {
use kulyrax_client::{EventEnvelope, Producer};

let mut producer = Producer::connect("http://127.0.0.1:50051").await?;
producer.send(EventEnvelope {
    event_type: "UserCreated".into(),
    payload: r#"{"id":"u1","name":"Ada"}"#.into(),
    metadata: Default::default(),
    version: 1,
    sequence: 1,
    position: "offset-1".into(),
    event_id: String::new(),
}).await?;

while let Some(ack) = producer.next_ack().await {
    println!("committed through {}", ack?.position);
}
# Ok(())
# }
```

Licensed under either of [MIT](https://github.com/Panchauly/kulyrax/blob/master/LICENSE-MIT)
or [Apache-2.0](https://github.com/Panchauly/kulyrax/blob/master/LICENSE-APACHE) at your option.
