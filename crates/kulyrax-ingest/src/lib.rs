//! Phase 20: Kulyrax's gRPC ingestion service.
//!
//! Part 3 of the three-part model (`architecture.md` §1.2): a
//! technology-agnostic producer contract. A producer in any language streams
//! [`EventEnvelope`](pb::EventEnvelope)s and gets back position
//! [`Ack`](pb::Ack)s once the batch's guards commit. Kulyrax persists nothing
//! on the ingestion side — the producer owns replay from its last ack.
//!
//! `tonic` lives here, never in `kulyrax-core`: [`GrpcSource`] implements the
//! `kulyrax_core::source::EventSource` trait, so `kulyrax_core::run_sources`
//! drives it exactly like a `directory` source.

#[allow(clippy::result_large_err)] // tonic-generated client stubs return `Err(tonic::Status)` directly
pub mod pb {
    tonic::include_proto!("kulyrax.v1");
}

use std::net::SocketAddr;

use kulyrax_core::event::Event;
use kulyrax_core::source::{EventSource, SourceError, SourcePosition, SourcedEvent};
use tokio::sync::{broadcast, mpsc, watch};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{BroadcastStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming, transport::Server};

/// Bound of the receive channel. Not event storage — a socket buffer. When
/// full, the gRPC handler's `send` awaits, HTTP/2 flow control pauses the
/// producer's stream: backpressure, not buffering (Phase 20.2).
const CHANNEL_BOUND: usize = 1024;

/// The `ack_protocol_version` this server sends on every `Ack` (Phase 29.1).
/// `1` means `dlq_events` is populated per the guarantee in
/// producer-contract.md. Bump only when that guarantee's meaning changes.
const CURRENT_ACK_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug)]
pub enum IngestError {
    Bind(String),
    Runtime(std::io::Error),
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
impl std::error::Error for IngestError {}

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
        SourcedEvent {
            event: Event {
                event_id,
                event_type: self.event_type,
                payload: self.payload,
                metadata: self.metadata.into_iter().collect(),
                version: if self.version == 0 { 1 } else { self.version },
                sequence: self.sequence,
            },
            position: SourcePosition(self.position),
        }
    }
}

impl From<&Event> for pb::EventEnvelope {
    fn from(e: &Event) -> Self {
        pb::EventEnvelope {
            event_type: e.event_type.clone(),
            payload: e.payload.clone(),
            metadata: e.metadata.clone().into_iter().collect(),
            version: e.version,
            sequence: e.sequence,
            position: String::new(),
            event_id: e.event_id.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// The gRPC service
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct AckMsg {
    position: String,
    batch_events: u32,
    dlq_events: u32,
}

struct IngestService {
    /// Decoded events → the bounded receive channel drained by `GrpcSource::poll`.
    ev_tx: mpsc::Sender<SourcedEvent>,
    /// Committed positions fanned out to every active session's response stream.
    ack_tx: broadcast::Sender<AckMsg>,
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
        let ack_rx = self.ack_tx.subscribe();

        // Reader task: inbound envelopes → bounded channel. `send().await` is
        // where backpressure lands when the engine is behind.
        tokio::spawn(async move {
            while let Ok(Some(env)) = inbound.message().await {
                if ev_tx.send(env.into_sourced()).await.is_err() {
                    break; // engine / source dropped
                }
            }
        });

        // Response stream: every committed position becomes an `Ack`.
        let out = BroadcastStream::new(ack_rx).filter_map(|r| {
            r.ok().map(|a: AckMsg| {
                Ok(pb::Ack {
                    position: a.position,
                    batch_events: a.batch_events,
                    dlq_events: a.dlq_events,
                    ack_protocol_version: CURRENT_ACK_PROTOCOL_VERSION,
                })
            })
        });
        Ok(Response::new(Box::pin(out)))
    }
}

// ---------------------------------------------------------------------------
// GrpcSource + server lifecycle
// ---------------------------------------------------------------------------

/// The `EventSource` handed to `kulyrax_core::run_sources`. Holds no persistent
/// state — `committed_position` is in memory only (Phase 20.2: the `Ack` is the
/// durable signal, the producer owns the offset).
pub struct GrpcSource {
    id: String,
    ev_rx: mpsc::Receiver<SourcedEvent>,
    ack_tx: broadcast::Sender<AckMsg>,
    committed: Option<SourcePosition>,
    /// Events returned by `poll` since the last `commit` — the `Ack.batch_events`.
    since_commit: u32,
}

impl EventSource for GrpcSource {
    fn id(&self) -> &str {
        &self.id
    }

    fn poll(&mut self, max_batch: usize) -> Result<Vec<SourcedEvent>, SourceError> {
        let mut out = Vec::new();
        while out.len() < max_batch {
            match self.ev_rx.try_recv() {
                Ok(se) => out.push(se),
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => break,
            }
        }
        self.since_commit += out.len() as u32;
        Ok(out)
    }

    fn commit(&mut self, position: SourcePosition) -> Result<(), SourceError> {
        self.commit_with_dlq_count(position, 0)
    }

    fn commit_with_dlq_count(
        &mut self,
        position: SourcePosition,
        dlq_events: u32,
    ) -> Result<(), SourceError> {
        // Fan the position out to every session; a session forwards it as an
        // `Ack` and the producer advances its offset. `dlq_events == 0` is
        // the producer's only signal that every event this ack covers was
        // actually projected, not silently dead-lettered (Phase 29).
        let _ = self.ack_tx.send(AckMsg {
            position: position.0.clone(),
            batch_events: std::mem::take(&mut self.since_commit),
            dlq_events,
        });
        self.committed = Some(position);
        Ok(())
    }

    fn committed_position(&self) -> Option<&SourcePosition> {
        self.committed.as_ref()
    }
}

/// A running gRPC server. Keep it alive for as long as the source is polled;
/// [`GrpcServer::shutdown`] stops it gracefully.
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

    let (ev_tx, ev_rx) = mpsc::channel::<SourcedEvent>(CHANNEL_BOUND);
    let (ack_tx, _ack_rx0) = broadcast::channel::<AckMsg>(256);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    // Bind inside the runtime task and report the actual address back — keeps
    // `start` free of `block_on`, so it is callable from any sync context.
    let (addr_tx, addr_rx) = tokio::sync::oneshot::channel::<Result<Bound, String>>();

    let svc = IngestService {
        ev_tx,
        ack_tx: ack_tx.clone(),
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
        ack_tx,
        committed: None,
        since_commit: 0,
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
pub struct ServeOptions {
    pub listen: String,
    pub max_batch: usize,
    pub retry_budget: u32,
    pub dlq_dir: Option<std::path::PathBuf>,
    /// Poll cadence when caught up — the max ingest→dispatch latency.
    pub poll_interval: std::time::Duration,
}

impl Default for ServeOptions {
    fn default() -> Self {
        Self {
            listen: "tcp://127.0.0.1:50051".into(),
            max_batch: 256,
            retry_budget: 3,
            dlq_dir: None,
            poll_interval: std::time::Duration::from_millis(200),
        }
    }
}

#[derive(Debug)]
pub enum ServeError {
    Pipeline(kulyrax_core::pipeline::PipelineError),
    Ingest(IngestError),
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
impl std::error::Error for ServeError {}

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
    register_backends: impl FnOnce(&mut kulyrax_core::KulyraxRuntime),
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
    register_backends(&mut runtime);

    let (server, source) = start("grpc", &opts.listen).map_err(ServeError::Ingest)?;
    if let Some(a) = server.local_addr {
        eprintln!("kulyrax ingest: listening on tcp://{a}");
    } else if let Some(p) = &server.local_path {
        eprintln!("kulyrax ingest: listening on unix:{}", p.display());
    }

    let run_opts = kulyrax_core::SourceRunOptions {
        mode: kulyrax_core::RunMode::Continuous {
            poll_interval: opts.poll_interval,
        },
        max_batch: opts.max_batch,
        retry_budget: opts.retry_budget,
        dlq_dir: opts.dlq_dir.clone(),
    };

    let result = runtime.run_sources(vec![Box::new(source)], &run_opts, stop);

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

        let back = pb::EventEnvelope::from(&se.event);
        assert_eq!(back.event_type, "UserCreated");
        assert_eq!(back.sequence, 7);
        assert_eq!(back.version, 1);
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
