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
