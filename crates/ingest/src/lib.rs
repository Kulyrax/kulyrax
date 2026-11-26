//! Phase 20: Kulyrax's gRPC ingestion service.
//!
//! Part 3 of the three-part model (`architecture.md` §1.2): a
//! technology-agnostic producer contract. A producer in any language streams
//! `EventEnvelope`s and gets back position `Ack`s once the batch's guards
//! commit (`proto/kulyrax/v1/ingest.proto`; `kulyrax-client` is the Rust
//! producer). Kulyrax persists nothing on the ingestion side — the producer
//! owns replay from its last ack.
//!
//! `tonic` lives here, never in `kulyrax-core`: [`GrpcSource`] implements the
//! `kulyrax_core::source::EventSource` trait, so `kulyrax_core::run_sources`
//! drives it exactly like a `directory` source.
//!
//! The generated gRPC code is private: the frozen contract is the `.proto`
//! (stability §9.3). The `unstable-pb` feature exposes it as `pb`, outside
//! semver.

#![deny(missing_docs)]

#[cfg(feature = "unstable-pb")]
#[doc(hidden)]
#[allow(clippy::result_large_err)] // tonic-generated client stubs return `Err(tonic::Status)` directly
pub mod pb {
    tonic::include_proto!("kulyrax.v1");
}
#[cfg(not(feature = "unstable-pb"))]
#[allow(clippy::result_large_err, dead_code)] // as above; the client stubs go unused here
mod pb {
    tonic::include_proto!("kulyrax.v1");
}

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use kulyrax_core::event::Event;
use kulyrax_core::execution::{ExecutionReport, ExecutionStatus};
use kulyrax_core::source::{EventSource, SourceError, SourcePosition, SourcedEvent};
use tokio::sync::{mpsc, watch};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming, transport::Server};

/// Bound of the receive channel. Not event storage — a socket buffer. When
/// full, the gRPC handler's `send` awaits, HTTP/2 flow control pauses the
/// producer's stream: backpressure, not buffering (Phase 20.2).
const CHANNEL_BOUND: usize = 1024;

/// Bound of each stream's ack channel. A producer that stops reading its acks
/// loses the newest ones past this many, as with the broadcast before it; the
/// acks still queued carry positions it really has committed.
const ACK_BOUND: usize = 256;

/// The `ack_protocol_version` this server sends on every `Ack` (Phase 29.1).
/// `1` means `dlq_events` is populated per the guarantee in
/// producer-contract.md. Bump only when that guarantee's meaning changes.
const CURRENT_ACK_PROTOCOL_VERSION: u32 = 1;

/// Why the gRPC server could not start.
#[derive(Debug)]
#[non_exhaustive]
pub enum IngestError {
    /// The listen address could not be bound.
    Bind(String),
    /// The server's async runtime could not be created.
    Runtime(std::io::Error),
    /// The ingest options are invalid (a bad listen address).
    Config(String),
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IngestError::Bind(s) => write!(f, "gRPC bind failed: {s}"),
            IngestError::Runtime(e) => write!(f, "tokio runtime: {e}"),
            IngestError::Config(s) => write!(f, "ingest config: {s}"),
        }
    }
}
impl std::error::Error for IngestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            IngestError::Runtime(e) => Some(e),
            IngestError::Bind(_) | IngestError::Config(_) => None,
        }
    }
}

// ---------------------------------------------------------------------------
// EventEnvelope <-> Event
// ---------------------------------------------------------------------------

impl pb::EventEnvelope {
    /// Decode into a Kulyrax [`Event`] + its opaque [`SourcePosition`].
    /// `version 0` → 1 (Phase 10 default); an empty `event_id` is derived (it
    /// is not load-bearing for gating — `sequence` is).
    pub fn into_sourced(self) -> SourcedEvent {
        let event_id = if !self.event_id.is_empty() {
            self.event_id
        } else if !self.position.is_empty() {
            self.position.clone()
        } else {
            format!("{}-{}", self.event_type, self.sequence)
        };
        SourcedEvent::new(
            Event::new(event_id, self.event_type, self.sequence, self.payload)
                .with_metadata(self.metadata.into_iter().collect())
                .with_version(if self.version == 0 { 1 } else { self.version }),
            SourcePosition(self.position),
        )
    }
}

impl pb::EventEnvelope {
    /// The envelope for `event`, at the producer's resume `position`. Every
    /// envelope needs one: Kulyrax refuses an empty position. (0.12 had a
    /// `From<&Event>` that left it empty.)
    /// On the generated type, so outside semver (`unstable-pb`); a Rust
    /// producer uses `kulyrax_client::EventEnvelope::new`.
    #[cfg(any(test, feature = "unstable-pb"))]
    pub fn from_event(e: &Event, position: impl Into<String>) -> Self {
        pb::EventEnvelope {
            event_type: e.event_type.clone(),
            payload: e.payload.clone(),
            metadata: e.metadata.clone().into_iter().collect(),
            version: e.version,
            sequence: e.sequence,
            position: position.into(),
            event_id: e.event_id.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// The gRPC service
// ---------------------------------------------------------------------------

/// One `Ingest.Stream` call. Acks go only to the stream whose events they
/// cover (F-02).
type StreamId = u64;

struct AckMsg {
    position: String,
    batch_events: u32,
    dlq_events: u32,
}

/// What a stream's response carries: its acks, or the status that ends it
/// (an event the stream may not send, such as one with no position).
type AckItem = Result<AckMsg, Status>;

/// The ack sender of every open stream. A stream registers itself when it
/// opens and is removed when its response stream is dropped.
type AckSenders = Arc<Mutex<HashMap<StreamId, mpsc::Sender<AckItem>>>>;

fn lock_senders(
    senders: &AckSenders,
) -> std::sync::MutexGuard<'_, HashMap<StreamId, mpsc::Sender<AckItem>>> {
    // The map holds no invariant that a panicking holder could break.
    senders.lock().unwrap_or_else(|e| e.into_inner())
}

/// Removes a stream's ack sender when its response stream is dropped (the
/// producer disconnected, or the server is shutting down).
struct Deregister {
    senders: AckSenders,
    id: StreamId,
}

impl Drop for Deregister {
    fn drop(&mut self) {
        lock_senders(&self.senders).remove(&self.id);
    }
}

struct IngestService {
    /// Decoded events, tagged with their stream → the bounded receive channel
    /// drained by `GrpcSource::poll`.
    ev_tx: mpsc::Sender<(StreamId, SourcedEvent)>,
    ack_senders: AckSenders,
    next_stream: AtomicU64,
}

/// Why an envelope is refused, ending its stream with `InvalidArgument`: an
/// empty `position` (producer contract §9; decision
/// 2026-11-25-source-position-contract). Its acks would carry nothing to
/// resume from. 0.12.x accepted it with a warning.
fn refusal(env: &pb::EventEnvelope) -> Option<Status> {
    env.position.is_empty().then(|| {
        Status::invalid_argument(format!(
            "event {:?} (type {:?}, sequence {}) has an empty position: give every event \
             the position your producer resumes from (docs/producer-contract.md §9)",
            env.event_id, env.event_type, env.sequence
        ))
    })
}

type AckStream =
    std::pin::Pin<Box<dyn futures_core::Stream<Item = Result<pb::Ack, Status>> + Send>>;

#[tonic::async_trait]
impl pb::ingest_server::Ingest for IngestService {
    type StreamStream = AckStream;

    // `Result<_, tonic::Status>` is the trait's return shape — `Status` is large
    // and there is nothing to box.
    #[allow(clippy::result_large_err)]
    async fn stream(
        &self,
        request: Request<Streaming<pb::EventEnvelope>>,
    ) -> Result<Response<Self::StreamStream>, Status> {
        let mut inbound = request.into_inner();
        let ev_tx = self.ev_tx.clone();
        let id = self.next_stream.fetch_add(1, Ordering::Relaxed);
        let (ack_tx, ack_rx) = mpsc::channel::<AckItem>(ACK_BOUND);
        let refuse_tx = ack_tx.clone();
        lock_senders(&self.ack_senders).insert(id, ack_tx);
        let deregister = Deregister {
            senders: Arc::clone(&self.ack_senders),
            id,
        };

        // Reader task: inbound envelopes → bounded channel. `send().await` is
        // where backpressure lands when the engine is behind. A refused
        // envelope ends the stream with its status; the events before it keep
        // their acks.
        tokio::spawn(async move {
            while let Ok(Some(env)) = inbound.message().await {
                if let Some(status) = refusal(&env) {
                    let _ = refuse_tx.send(Err(status)).await;
                    break;
                }
                if ev_tx.send((id, env.into_sourced())).await.is_err() {
                    break; // engine / source dropped
                }
            }
        });

        // Response stream: this stream's acks only, then its refusal if any.
        // The closure owns `deregister`, so the sender is removed when tonic
        // drops the stream.
        let out = ReceiverStream::new(ack_rx).map(move |item: AckItem| {
            let _ = &deregister;
            item.map(|a| pb::Ack {
                position: a.position,
                batch_events: a.batch_events,
                dlq_events: a.dlq_events,
                ack_protocol_version: CURRENT_ACK_PROTOCOL_VERSION,
            })
        });
        Ok(Response::new(Box::pin(out)))
    }
}

// ---------------------------------------------------------------------------
// GrpcSource + server lifecycle
// ---------------------------------------------------------------------------

/// An event polled but not yet committed: which stream sent it, its position,
/// and whether it was dead-lettered.
struct Pending {
    stream: StreamId,
    position: SourcePosition,
    dead_lettered: bool,
}

/// The `EventSource` handed to `kulyrax_core::run_sources`. Holds no persistent
/// state — `committed_position` is in memory only (Phase 20.2: the `Ack` is the
/// durable signal, the producer owns the offset).
pub struct GrpcSource {
    id: String,
    ev_rx: mpsc::Receiver<(StreamId, SourcedEvent)>,
    ack_senders: AckSenders,
    committed: Option<SourcePosition>,
    /// Events returned by `poll` since the last `commit`, in poll order.
    pending: Vec<Pending>,
    /// How many of `pending` `on_resolved` has reported on.
    resolved: usize,
}

impl EventSource for GrpcSource {
    fn id(&self) -> &str {
        &self.id
    }

    fn poll(&mut self, max_batch: usize) -> Result<Vec<SourcedEvent>, SourceError> {
        let mut out = Vec::new();
        while out.len() < max_batch {
            match self.ev_rx.try_recv() {
                Ok((stream, se)) => {
                    self.pending.push(Pending {
                        stream,
                        position: se.position.clone(),
                        dead_lettered: false,
                    });
                    out.push(se);
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        Ok(out)
    }

    fn on_resolved(&mut self, event: &SourcedEvent, report: &ExecutionReport) {
        // The run loop reports a committed batch's events in batch order, which
        // is poll order. A failed event in a committed batch was dead-lettered.
        let Some(p) = self.pending.get_mut(self.resolved) else {
            return;
        };
        debug_assert_eq!(p.position, event.position);
        p.dead_lettered = report.status == ExecutionStatus::Failed;
        self.resolved += 1;
    }

    fn commit(&mut self, position: SourcePosition) -> Result<(), SourceError> {
        self.commit_with_dlq_count(position, 0)
    }

    /// Sends each stream with events in this batch one `Ack` for its own
    /// events: the position of its last event (in its own send order), its
    /// event count, and its dead-lettered count. `dlq_events == 0` is that
    /// producer's only signal that every one of its events was projected
    /// (Phase 29). Streams with no events in the batch get nothing, and no
    /// position is compared across streams (F-02), so the batch-wide
    /// `position` and dead-letter count are not sent to anyone.
    fn commit_with_dlq_count(
        &mut self,
        position: SourcePosition,
        _dlq_events: u32,
    ) -> Result<(), SourceError> {
        // One ack per stream, in the order the streams first appear.
        let mut acks: Vec<(StreamId, AckMsg)> = Vec::new();
        for p in self.pending.drain(..) {
            let i = match acks.iter().position(|(s, _)| *s == p.stream) {
                Some(i) => i,
                None => {
                    acks.push((
                        p.stream,
                        AckMsg {
                            position: String::new(),
                            batch_events: 0,
                            dlq_events: 0,
                        },
                    ));
                    acks.len() - 1
                }
            };
            let ack = &mut acks[i].1;
            ack.position = p.position.0;
            ack.batch_events += 1;
            ack.dlq_events += u32::from(p.dead_lettered);
        }
        self.resolved = 0;

        let senders = lock_senders(&self.ack_senders);
        for (stream, ack) in acks {
            // A stream that has gone is not acked: its producer resumes from
            // the last ack it did receive. A full channel drops the ack (see
            // `ACK_BOUND`).
            if let Some(tx) = senders.get(&stream) {
                let _ = tx.try_send(Ok(ack));
            }
        }
        drop(senders);
        self.committed = Some(position);
        Ok(())
    }

    fn committed_position(&self) -> Option<&SourcePosition> {
        self.committed.as_ref()
    }
}

/// A running gRPC server. Keep it alive for as long as the source is polled;
/// [`GrpcServer::shutdown`] stops it gracefully.
#[non_exhaustive]
pub struct GrpcServer {
    runtime: tokio::runtime::Runtime,
    shutdown: watch::Sender<bool>,
    /// Set for a `tcp://` listener (useful when a `:0` ephemeral port was asked for).
    pub local_addr: Option<SocketAddr>,
    /// Set for a `unix:` listener (Phase 21.6, unix targets only).
    pub local_path: Option<std::path::PathBuf>,
}

impl GrpcServer {
    /// Stop accepting new stream items, let in-flight RPCs finish, then exit.
    pub fn shutdown(self) {
        let _ = self.shutdown.send(true);
        self.runtime
            .shutdown_timeout(std::time::Duration::from_secs(5));
    }
}

/// Where a bound listener ended up — a `SocketAddr` for `tcp://`, a path for
/// `unix:` (Phase 21.6).
enum Bound {
    Tcp(SocketAddr),
    #[cfg(unix)]
    Unix(std::path::PathBuf),
}

/// Bind a gRPC ingestion server and return it together with the [`GrpcSource`]
/// to feed `kulyrax_core::run_sources`.
///
/// `listen` accepts `"tcp://host:port"`, a bare `"host:port"`, or (Linux/macOS
/// only) `"unix:/path/to.sock"` — on Windows a `unix:` target returns a clear
/// config error.
pub fn start(
    source_id: impl Into<String>,
    listen: &str,
) -> Result<(GrpcServer, GrpcSource), IngestError> {
    let target = parse_listen(listen)?;
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(IngestError::Runtime)?;

    let (ev_tx, ev_rx) = mpsc::channel::<(StreamId, SourcedEvent)>(CHANNEL_BOUND);
    let ack_senders: AckSenders = Arc::default();
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // Bind inside the runtime task and report the actual address back — keeps
    // `start` free of `block_on`, so it is callable from any sync context.
    let (addr_tx, addr_rx) = tokio::sync::oneshot::channel::<Result<Bound, String>>();

    let svc = IngestService {
        ev_tx,
        ack_senders: Arc::clone(&ack_senders),
        next_stream: AtomicU64::new(0),
    };

    match target {
        Listen::Tcp(addr) => {
            let mut shutdown_rx = shutdown_rx;
            runtime.spawn(async move {
                let listener = match tokio::net::TcpListener::bind(addr).await {
                    Ok(l) => l,
                    Err(e) => {
                        let _ = addr_tx.send(Err(e.to_string()));
                        return;
                    }
                };
                let local = listener.local_addr();
                let _ = addr_tx.send(local.map(Bound::Tcp).map_err(|e| e.to_string()));
                let _ = Server::builder()
                    .add_service(pb::ingest_server::IngestServer::new(svc))
                    .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                        let _ = shutdown_rx.wait_for(|v| *v).await;
                    })
                    .await;
            });
        }
        #[cfg(unix)]
        Listen::Unix(path) => {
            prepare_unix_socket_path(&path)?;
            let mut shutdown_rx = shutdown_rx;
            let bound_path = path.clone();
            runtime.spawn(async move {
                let listener = match tokio::net::UnixListener::bind(&path) {
                    Ok(l) => l,
                    Err(e) => {
                        let _ = addr_tx.send(Err(e.to_string()));
                        return;
                    }
                };
                let _ = addr_tx.send(Ok(Bound::Unix(bound_path.clone())));
                let _ = Server::builder()
                    .add_service(pb::ingest_server::IngestServer::new(svc))
                    .serve_with_incoming_shutdown(
                        tokio_stream::wrappers::UnixListenerStream::new(listener),
                        async move {
                            let _ = shutdown_rx.wait_for(|v| *v).await;
                        },
                    )
                    .await;
                // Best-effort cleanup: only after the server loop has actually
                // stopped accepting, so a well-behaved shutdown leaves no stale file.
                let _ = std::fs::remove_file(&bound_path);
            });
        }
    }

    let bound = addr_rx
        .blocking_recv()
        .map_err(|_| IngestError::Bind("server task exited before binding".into()))?
        .map_err(IngestError::Bind)?;
    let (local_addr, local_path) = match bound {
        Bound::Tcp(a) => (Some(a), None),
        #[cfg(unix)]
        Bound::Unix(p) => (None, Some(p)),
    };

    let source = GrpcSource {
        id: source_id.into(),
        ev_rx,
        ack_senders,
        committed: None,
        pending: Vec::new(),
        resolved: 0,
    };
    Ok((
        GrpcServer {
            runtime,
            shutdown: shutdown_tx,
            local_addr,
            local_path,
        },
        source,
    ))
}

/// A stale socket file (nothing listening) is removed; a live one (something
/// accepts connections on it) refuses to start (Phase 21.6).
#[cfg(unix)]
fn prepare_unix_socket_path(path: &std::path::Path) -> Result<(), IngestError> {
    if path.exists() {
        match std::os::unix::net::UnixStream::connect(path) {
            Ok(_) => {
                return Err(IngestError::Bind(format!(
                    "{} is already in use by a running server",
                    path.display()
                )));
            }
            Err(_) => {
                std::fs::remove_file(path).map_err(|e| {
                    IngestError::Bind(format!("removing stale socket {}: {e}", path.display()))
                })?;
            }
        }
    }
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent).map_err(|e| {
            IngestError::Bind(format!(
                "creating socket directory {}: {e}",
                parent.display()
            ))
        })?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// `kulyrax ingest` orchestration (kept out of the thin CLI)
// ---------------------------------------------------------------------------

/// Options for [`serve`].
#[non_exhaustive]
pub struct ServeOptions {
    /// Where to listen: `tcp://host:port`, `host:port`, or (Linux and macOS)
    /// `unix:///path`.
    pub listen: String,
    /// The most events in one batch.
    pub max_batch: usize,
    /// How many times a failed batch is retried before the run halts.
    pub retry_budget: u32,
    /// How long a batch may wait to fill; see
    /// [`kulyrax_core::SourceRunOptions::max_batch_delay`].
    pub max_batch_delay: std::time::Duration,
    /// Where failed events are dead-lettered; `None` turns the DLQ off.
    pub dlq_dir: Option<std::path::PathBuf>,
    /// Poll cadence when caught up — the max ingest→dispatch latency.
    pub poll_interval: std::time::Duration,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            listen: "tcp://127.0.0.1:50051".into(),
            max_batch: kulyrax_core::source::runner::DEFAULT_LIVE_BATCH,
            retry_budget: 3,
            max_batch_delay: kulyrax_core::source::runner::DEFAULT_BATCH_DELAY,
            dlq_dir: None,
            poll_interval: std::time::Duration::from_millis(200),
        }
    }
}

/// Why [`serve`] stopped with an error.
#[derive(Debug)]
#[non_exhaustive]
pub enum ServeError {
    /// Loading the project, or building its adapters, failed.
    Pipeline(kulyrax_core::pipeline::PipelineError),
    /// The gRPC server could not start.
    Ingest(IngestError),
    /// The gRPC source failed while the run loop read it.
    Source(SourceError),
}

impl std::fmt::Display for ServeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServeError::Pipeline(e) => write!(f, "{e}"),
            ServeError::Ingest(e) => write!(f, "{e}"),
            ServeError::Source(e) => write!(f, "{e}"),
        }
    }
}
impl std::error::Error for ServeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ServeError::Pipeline(e) => Some(e),
            ServeError::Ingest(e) => Some(e),
            ServeError::Source(e) => Some(e),
        }
    }
}

/// Load the project, start the gRPC server, and run the
/// `poll → dispatch → commit → ack` loop until `stop` is set (SIGINT/SIGTERM).
/// On shutdown the in-flight batch is committed and acked before the server
/// stops (Phase 20.4).
///
/// `register_backends` runs on the runtime before adapters build — pass
/// `kulyrax_backends::register_default_backends` to get every split-out
/// networked backend (Postgres, ...), or `|_| {}` for none (Phase 28).
pub fn serve(
    config_path: &std::path::Path,
    mappings_dir: &std::path::Path,
    opts: &ServeOptions,
    stop: &std::sync::atomic::AtomicBool,
    register_backends: impl Fn(&mut kulyrax_core::KulyraxRuntime) + Clone + 'static,
) -> Result<kulyrax_core::SourceRunReport, ServeError> {
    let project = kulyrax_core::pipeline::load_project(config_path, mappings_dir)
        .map_err(ServeError::Pipeline)?;
    let mut runtime = kulyrax_core::KulyraxRuntime::from_parts(
        project.config,
        project.sql_mappings,
        project.doc_mappings,
        project.kv_mappings,
        project.graph_mappings,
        project.routing_rules,
    );
    register_backends.clone()(&mut runtime);
    runtime.build_adapters().map_err(ServeError::Pipeline)?;
    runtime.prepare_adapters().map_err(ServeError::Pipeline)?;

    let (server, source) = start("grpc", &opts.listen).map_err(ServeError::Ingest)?;
    if let Some(a) = server.local_addr {
        eprintln!("kulyrax ingest: listening on tcp://{a}");
    } else if let Some(p) = &server.local_path {
        eprintln!("kulyrax ingest: listening on unix:{}", p.display());
    }

    let run_opts = {
        let mut o = kulyrax_core::SourceRunOptions::default();
        o.dlq_dir = opts.dlq_dir.clone();
        o.max_batch = opts.max_batch;
        o.max_batch_delay = opts.max_batch_delay;
        o.mode = kulyrax_core::RunMode::Continuous {
            poll_interval: opts.poll_interval,
        };
        o.retry_budget = opts.retry_budget;
        o
    };

    // `kulyrax reload` swaps config, routing and mappings between batches.
    let mut reloader = kulyrax_core::runtime::FileReloader::new(
        config_path,
        mappings_dir,
        runtime.config().sources.clone(),
        register_backends,
        runtime.upcasters(),
        |outcome| match outcome {
            kulyrax_core::runtime::ReloadOutcome::Applied { message } => {
                eprintln!("kulyrax ingest: reload applied ({message})")
            }
            kulyrax_core::runtime::ReloadOutcome::Rejected { reason } => eprintln!(
                "kulyrax ingest: reload rejected, keeping the previous configuration: {reason}"
            ),
            // `ReloadOutcome` is `#[non_exhaustive]`; nothing else is reported.
            _ => {}
        },
    );
    let result =
        runtime.run_sources_with_reload(vec![Box::new(source)], &run_opts, stop, &mut reloader);

    server.shutdown();
    result.map_err(ServeError::Source)
}

/// A parsed `--listen` target (Phase 21.6 adds `Unix`).
enum Listen {
    Tcp(SocketAddr),
    #[cfg(unix)]
    Unix(std::path::PathBuf),
}

/// `tcp://h:p`, a bare `h:p`, or `unix:/path`. `unix:` is only ever a `Listen`
/// target on Unix; elsewhere it is a clear config error, not a silent TCP fallback.
fn parse_listen(listen: &str) -> Result<Listen, IngestError> {
    if let Some(path) = listen.strip_prefix("unix:") {
        #[cfg(unix)]
        {
            return Ok(Listen::Unix(std::path::PathBuf::from(path)));
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            return Err(IngestError::Config(
                "unix-domain-socket listener is not built on this platform; use tcp://host:port"
                    .into(),
            ));
        }
    }
    let s = listen.strip_prefix("tcp://").unwrap_or(listen);
    s.parse()
        .map(Listen::Tcp)
        .map_err(|e| IngestError::Config(format!("invalid listen address {listen:?}: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn envelope_round_trips_through_event() {
        let env = pb::EventEnvelope {
            event_type: "UserCreated".into(),
            payload: r#"{"id":"u1"}"#.into(),
            metadata: HashMap::from([("trace".to_string(), "abc".to_string())]),
            version: 0,
            sequence: 7,
            position: "offset-42".into(),
            event_id: String::new(),
        };
        let se = env.into_sourced();
        assert_eq!(se.event.event_type, "UserCreated");
        assert_eq!(se.event.version, 1, "version 0 defaults to 1");
        assert_eq!(se.event.sequence, 7);
        assert_eq!(se.event.event_id, "offset-42", "derived from position");
        assert_eq!(se.position, SourcePosition("offset-42".into()));

        let back = pb::EventEnvelope::from_event(&se.event, se.position.as_str());
        assert_eq!(back.event_type, "UserCreated");
        assert_eq!(back.sequence, 7);
        assert_eq!(back.version, 1);
        assert_eq!(
            back.position, "offset-42",
            "the position goes with the event"
        );
    }

    #[test]
    fn an_empty_position_is_refused_with_invalid_argument() {
        let at = |position: &str| pb::EventEnvelope {
            event_id: "e1".into(),
            position: position.into(),
            ..Default::default()
        };
        assert!(refusal(&at("p1")).is_none(), "a position is accepted");
        let status = refusal(&at("")).expect("an empty position is refused");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
        assert!(status.message().contains("empty position"), "{status:?}");
        assert!(status.message().contains("\"e1\""), "{status:?}");
    }

    #[test]
    fn parse_listen_forms() {
        assert!(matches!(
            parse_listen("tcp://127.0.0.1:50051"),
            Ok(Listen::Tcp(_))
        ));
        assert!(matches!(parse_listen("127.0.0.1:0"), Ok(Listen::Tcp(_))));
        assert!(parse_listen("not-an-addr").is_err());

        #[cfg(unix)]
        assert!(matches!(
            parse_listen("unix:/tmp/x.sock"),
            Ok(Listen::Unix(_))
        ));
        #[cfg(not(unix))]
        assert!(parse_listen("unix:/tmp/x.sock").is_err());
    }
}
