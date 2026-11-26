//! Phase 20.3: the reference Rust client for the Kulyrax gRPC ingestion
//! contract: `connect`, `send`, `next_ack`.
//!
//! The frozen contract is `proto/kulyrax/v1/ingest.proto`, not generated Rust
//! (stability §9.3). This crate's types ([`EventEnvelope`], [`Ack`],
//! [`AckOutcome`], [`ClientError`], [`RpcError`]) are Kulyrax's own, so a tonic
//! or prost upgrade never changes them. The generated code is private; the
//! `unstable-pb` feature exposes it as `pb`, outside semver.

#![deny(missing_docs)]

#[cfg(feature = "unstable-pb")]
#[doc(hidden)]
#[allow(clippy::result_large_err)] // tonic-generated client stubs return `Err(tonic::Status)` directly
pub mod pb {
    tonic::include_proto!("kulyrax.v1");
}
#[cfg(not(feature = "unstable-pb"))]
#[allow(clippy::result_large_err, dead_code)] // as above; some generated items go unused
mod pb {
    tonic::include_proto!("kulyrax.v1");
}

use std::collections::HashMap;

use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Streaming;

/// One event, as a producer sends it (`ingest.proto`'s `EventEnvelope`).
/// `#[non_exhaustive]`: build it with [`EventEnvelope::new`] and the `with_*`
/// methods.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct EventEnvelope {
    /// The event type, which routing matches on.
    pub event_type: String,
    /// JSON, exactly as the file sources carry it.
    pub payload: String,
    /// String metadata, which becomes the event's metadata.
    pub metadata: HashMap<String, String>,
    /// Payload schema version. 0 is read as 1.
    pub version: u32,
    /// Monotonic per entity (producer contract §1).
    pub sequence: u64,
    /// This producer's resume token. Required: an empty position ends the
    /// stream with `InvalidArgument` (producer contract §9).
    pub position: String,
    /// Optional; when empty, Kulyrax derives one from `position`.
    pub event_id: String,
}

impl EventEnvelope {
    /// An event with no metadata or event id, at payload schema version 1.
    pub fn new(
        event_type: impl Into<String>,
        payload: impl Into<String>,
        sequence: u64,
        position: impl Into<String>,
    ) -> Self {
        Self {
            event_type: event_type.into(),
            payload: payload.into(),
            metadata: HashMap::new(),
            version: 1,
            sequence,
            position: position.into(),
            event_id: String::new(),
        }
    }

    /// The envelope with `metadata` in place of its current metadata.
    pub fn with_metadata(mut self, metadata: HashMap<String, String>) -> Self {
        self.metadata = metadata;
        self
    }

    /// The envelope at payload schema version `version`.
    pub fn with_version(mut self, version: u32) -> Self {
        self.version = version;
        self
    }

    /// The envelope with an explicit event id.
    pub fn with_event_id(mut self, event_id: impl Into<String>) -> Self {
        self.event_id = event_id.into();
        self
    }
}

impl From<EventEnvelope> for pb::EventEnvelope {
    fn from(e: EventEnvelope) -> Self {
        pb::EventEnvelope {
            event_type: e.event_type,
            payload: e.payload,
            metadata: e.metadata,
            version: e.version,
            sequence: e.sequence,
            position: e.position,
            event_id: e.event_id,
        }
    }
}

/// A position acknowledgement (`ingest.proto`'s `Ack`): this stream's events up
/// to `position` are committed. Read it through [`Ack::outcome`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Ack {
    /// The last position this stream sent whose batch has committed.
    pub position: String,
    /// This stream's events in the batch the ack covers.
    pub batch_events: u32,
    /// How many of them were dead-lettered.
    pub dlq_events: u32,
    /// Whether `dlq_events` can be trusted: `0` means no (an old server).
    pub ack_protocol_version: u32,
}

impl From<pb::Ack> for Ack {
    fn from(a: pb::Ack) -> Self {
        Self {
            position: a.position,
            batch_events: a.batch_events,
            dlq_events: a.dlq_events,
            ack_protocol_version: a.ack_protocol_version,
        }
    }
}

/// What an `Ack` actually proves about the batch it covers (Phase 29.1).
/// Read this before treating any `Ack` as a success signal — see
/// [`Ack::outcome`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AckOutcome {
    /// `ack_protocol_version >= 1` and `dlq_events == 0`: every event this
    /// batch covers reached every adapter it was routed to.
    Confirmed,
    /// `ack_protocol_version >= 1` and `dlq_events > 0`: the position still
    /// advanced, but this many events were dead-lettered rather than
    /// genuinely projected.
    DeadLettered {
        /// How many events of the batch were dead-lettered.
        count: u32,
    },
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

/// A gRPC status code (the standard numbering, which every gRPC library
/// shares), without tying this crate's API to a gRPC library's own type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum StatusCode {
    /// Not an error.
    Ok,
    /// The operation was cancelled, usually by the caller.
    Cancelled,
    /// An error with no better code.
    Unknown,
    /// The request is invalid. Kulyrax sends it for an envelope with an empty
    /// position, ending that stream.
    InvalidArgument,
    /// The deadline passed before the operation finished.
    DeadlineExceeded,
    /// Something the request named was not found.
    NotFound,
    /// What the request would create already exists.
    AlreadyExists,
    /// The caller may not do this.
    PermissionDenied,
    /// A resource ran out (a quota, or space).
    ResourceExhausted,
    /// The system is not in a state that allows the operation.
    FailedPrecondition,
    /// The operation was aborted, typically by a concurrency conflict.
    Aborted,
    /// A value was out of its valid range.
    OutOfRange,
    /// The server does not implement the operation.
    Unimplemented,
    /// An internal server error.
    Internal,
    /// The service is unavailable for now; retrying may help.
    Unavailable,
    /// Data was lost or corrupted.
    DataLoss,
    /// The request has no valid credentials.
    Unauthenticated,
}

impl StatusCode {
    fn from_tonic(code: tonic::Code) -> Self {
        use tonic::Code as C;
        match code {
            C::Ok => Self::Ok,
            C::Cancelled => Self::Cancelled,
            C::InvalidArgument => Self::InvalidArgument,
            C::DeadlineExceeded => Self::DeadlineExceeded,
            C::NotFound => Self::NotFound,
            C::AlreadyExists => Self::AlreadyExists,
            C::PermissionDenied => Self::PermissionDenied,
            C::ResourceExhausted => Self::ResourceExhausted,
            C::FailedPrecondition => Self::FailedPrecondition,
            C::Aborted => Self::Aborted,
            C::OutOfRange => Self::OutOfRange,
            C::Unimplemented => Self::Unimplemented,
            C::Internal => Self::Internal,
            C::Unavailable => Self::Unavailable,
            C::DataLoss => Self::DataLoss,
            C::Unauthenticated => Self::Unauthenticated,
            _ => Self::Unknown,
        }
    }
}

/// The status a server ended the stream (or refused a call) with.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct RpcError {
    /// The status code.
    pub code: StatusCode,
    /// The server's message.
    pub message: String,
}

impl std::fmt::Display for RpcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for RpcError {}

impl From<tonic::Status> for RpcError {
    fn from(s: tonic::Status) -> Self {
        Self {
            code: StatusCode::from_tonic(s.code()),
            message: s.message().to_string(),
        }
    }
}

/// Why a [`Producer`] call failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum ClientError {
    /// The connection could not be made (a bad endpoint, or no server).
    Connect(String),
    /// The server's status: a refused call, or a stream it ended.
    Rpc(RpcError),
    /// The stream is closed: the server ended it, or the connection dropped.
    Closed,
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

impl std::error::Error for ClientError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ClientError::Rpc(e) => Some(e),
            _ => None,
        }
    }
}

fn rpc(s: tonic::Status) -> ClientError {
    ClientError::Rpc(s.into())
}

/// One producer session over `Ingest.Stream`. Send events; read position acks.
pub struct Producer {
    tx: mpsc::Sender<pb::EventEnvelope>,
    acks: Streaming<pb::Ack>,
}

impl Producer {
    /// Open a stream to `endpoint` (`http://host:port`).
    pub async fn connect(endpoint: impl Into<String>) -> Result<Self, ClientError> {
        let mut client = pb::ingest_client::IngestClient::connect(endpoint.into())
            .await
            .map_err(|e| ClientError::Connect(e.to_string()))?;
        let (tx, rx) = mpsc::channel::<pb::EventEnvelope>(256);
        let resp = client.stream(ReceiverStream::new(rx)).await.map_err(rpc)?;
        Ok(Self {
            tx,
            acks: resp.into_inner(),
        })
    }

    /// Enqueue an event. Awaits when the server's receive buffer is full
    /// (backpressure) — that is the intended flow-control point.
    pub async fn send(&self, env: EventEnvelope) -> Result<(), ClientError> {
        self.tx
            .send(env.into())
            .await
            .map_err(|_| ClientError::Closed)
    }

    /// The next position ack, or `None` when the stream ends.
    pub async fn next_ack(&mut self) -> Option<Result<Ack, ClientError>> {
        self.acks
            .next()
            .await
            .map(|r| r.map(Ack::from).map_err(rpc))
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

    #[test]
    fn an_envelope_round_trips_through_the_wire_type() {
        let env = EventEnvelope::new("UserCreated", "{}", 7, "offset-7")
            .with_version(2)
            .with_event_id("e7");
        let wire = pb::EventEnvelope::from(env.clone());
        assert_eq!(wire.position, "offset-7");
        assert_eq!(wire.version, 2);
        assert_eq!(wire.event_id, "e7");
        assert_eq!(wire.sequence, 7);
    }

    #[test]
    fn a_grpc_status_becomes_an_rpc_error_without_tonic_types() {
        let e = RpcError::from(tonic::Status::invalid_argument("empty position"));
        assert_eq!(e.code, StatusCode::InvalidArgument);
        assert_eq!(e.message, "empty position");
    }
}
