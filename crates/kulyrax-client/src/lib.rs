//! Phase 20.3: the reference Rust client for the Kulyrax gRPC ingestion
//! contract. A thin wrapper over the generated `tonic` client — `connect`,
//! `send`, `acks` — and the shape a producer in any `protoc`-supported
//! language reproduces from `proto/kulyrax/v1/ingest.proto` with generated
//! stubs and ~20 lines of glue.

#[allow(clippy::result_large_err)] // tonic-generated client stubs return `Err(tonic::Status)` directly
pub mod pb {
    tonic::include_proto!("kulyrax.v1");
}

use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;

pub use pb::{Ack, EventEnvelope};

/// What an `Ack` actually proves about the batch it covers (Phase 29.1).
/// Read this before treating any `Ack` as a success signal — see
/// [`Ack::outcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    /// `ack_protocol_version >= 1` and `dlq_events == 0`: every event this
    /// batch covers reached every adapter it was routed to.
    Confirmed,
    /// `ack_protocol_version >= 1` and `dlq_events > 0`: the position still
    /// advanced, but this many events were dead-lettered rather than
    /// genuinely projected.
    DeadLettered { count: u32 },
    /// `ack_protocol_version == 0`: this server predates `dlq_events`
    /// accounting (or the field was never set), so `dlq_events` cannot be
    /// trusted either way — including when it reads `0`. Do not treat this
    /// as success.
    Unverified,
}

impl Ack {
    /// Classify this `Ack` per the compatibility signal in
    /// `ack_protocol_version` — never read `dlq_events` directly as a
    /// success/failure signal without going through this.
    pub fn outcome(&self) -> AckOutcome {
        if self.ack_protocol_version == 0 {
            AckOutcome::Unverified
        } else if self.dlq_events > 0 {
            AckOutcome::DeadLettered {
                count: self.dlq_events,
            }
        } else {
            AckOutcome::Confirmed
        }
    }
}

#[derive(Debug)]
pub enum ClientError {
    Connect(String),
    /// Boxed — `tonic::Status` is large and would bloat every `Result` here.
    Rpc(Box<tonic::Status>),
    Closed,
}

impl ClientError {
    fn rpc(s: tonic::Status) -> Self {
        ClientError::Rpc(Box::new(s))
    }
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Connect(s) => write!(f, "connect: {s}"),
            ClientError::Rpc(s) => write!(f, "rpc: {s}"),
            ClientError::Closed => write!(f, "stream closed"),
        }
    }
}
impl std::error::Error for ClientError {}

/// One producer session over `Ingest.Stream`. Send events; read position acks.
pub struct Producer {
    tx: mpsc::Sender<EventEnvelope>,
    acks: Streaming<Ack>,
}

impl Producer {
    /// Open a stream to `endpoint` (`http://host:port`).
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, ClientError> {
        let mut client = pb::ingest_client::IngestClient::connect(endpoint.into())
            .await
            .map_err(|e| ClientError::Connect(e.to_string()))?;
        let (tx, rx) = mpsc::channel::<EventEnvelope>(256);
        let resp = client
            .stream(ReceiverStream::new(rx))
            .await
            .map_err(ClientError::rpc)?;
        Ok(Self {
            tx,
            acks: resp.into_inner(),
        })
    }

    /// Enqueue an event. Awaits when the server's receive buffer is full
    /// (backpressure) — that is the intended flow-control point.
    pub async fn send(&self, env: EventEnvelope) -> Result<(), ClientError> {
        self.tx.send(env).await.map_err(|_| ClientError::Closed)
    }

    /// The next position ack, or `None` when the stream ends.
    pub async fn next_ack(&mut self) -> Option<Result<Ack, ClientError>> {
        self.acks.next().await.map(|r| r.map_err(ClientError::rpc))
    }

    /// Close the send side; the server sees the stream end and stops reading.
    pub fn finish(self) {
        drop(self.tx);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ack(ack_protocol_version: u32, dlq_events: u32) -> Ack {
        Ack {
            position: "p".into(),
            batch_events: 1,
            dlq_events,
            ack_protocol_version,
        }
    }

    #[test]
    fn an_old_server_that_never_sets_ack_protocol_version_is_unverified_not_confirmed() {
        // The exact failure this type exists to prevent: an old server
        // defaults `dlq_events` to 0 on the wire, which must NOT be read as
        // proof of success just because it happens to equal 0.
        assert_eq!(ack(0, 0).outcome(), AckOutcome::Unverified);
        assert_eq!(ack(0, 3).outcome(), AckOutcome::Unverified);
    }

    #[test]
    fn a_compatible_server_s_ack_is_classified_by_dlq_events() {
        assert_eq!(ack(1, 0).outcome(), AckOutcome::Confirmed);
        assert_eq!(ack(1, 2).outcome(), AckOutcome::DeadLettered { count: 2 });
    }
}
