//! Streams two `UserRegistered` events into a running `kulyrax ingest` and
//! prints every `Ack` as it arrives. Point it at a running server:
//! `cargo run --manifest-path producer/Cargo.toml -- http://127.0.0.1:50061`
//! (`run.sh` starts the server for you).

use kulyrax_client::{EventEnvelope, Producer};

#[tokio::main]
async fn main() {
    let endpoint = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "http://127.0.0.1:50061".to_string());

    let mut producer = Producer::connect(endpoint)
        .await
        .expect("connect to kulyrax ingest");

    // (id, name, sequence, position) — `position` is this producer's own
    // opaque offset; Kulyrax only ever echoes it back, never interprets it.
    let events = [
        ("u1", "Ada", 1u64, "offset-1"),
        ("u2", "Grace", 1u64, "offset-2"),
    ];
    for (id, name, sequence, position) in events {
        producer
            .send(EventEnvelope::new(
                "UserRegistered",
                format!(r#"{{"id":"{id}","name":"{name}"}}"#),
                sequence,
                position,
            ))
            .await
            .expect("send event");
    }

    // An ack carries the last position this stream sent whose batch has
    // committed, so "wait for offset-2" is "wait for the last event's batch".
    while let Some(ack) = producer.next_ack().await {
        let ack = ack.expect("ack");
        println!(
            "ack: committed through {} ({} events)",
            ack.position, ack.batch_events
        );
        if ack.position == "offset-2" {
            break;
        }
    }
    producer.finish();
}
