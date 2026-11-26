//! Phase 17: the **source** side — the symmetric other half of
//! [`StorageAdapter`](crate::adapter::StorageAdapter). A source is a
//! checkpointed, resumable reader that hands the engine ordered batches of
//! events with a durable position. It does *not* own transport, durability,
//! partitioning, or fan-out (architecture.md §1) — it consumes from something
//! that already has those properties.

pub mod checkpoint;
pub mod directory;
pub mod runner;
pub mod stdin;

use serde::{Deserialize, Serialize};

use crate::event::Event;

/// An opaque resume token, made by one source for that source: a file name, a
/// line number, an outbox cursor, a gRPC producer's own `position`.
///
/// Only the source that made a position may order it, by its own rule. The
/// run loop never compares positions. It commits each batch at the position
/// of the batch's **last event in delivery order** (`docs/producer-contract.md`,
/// §9 "Positions").
///
/// Positions are compared for equality (`Eq`, `Hash`) but have no order: text
/// order is not delivery order in general (`"9"` sorts after `"10"`). A source
/// that needs to order its own positions parses them by its own rule. 0.12
/// derived `PartialOrd` and `Ord`; 1.0 removes them (decision
/// 2026-11-25-source-position-contract).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SourcePosition(pub String);

impl SourcePosition {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SourcePosition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// One event plus the position needed to commit past it.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SourcedEvent {
    pub event: Event,
    pub position: SourcePosition,
}

/// Failure reading from or checkpointing a source.
#[derive(Debug)]
#[non_exhaustive]
pub enum SourceError {
    Io(std::io::Error),
    Parse(String),
    Checkpoint(String),
    /// One or more adapters failed `StorageAdapter::prepare`; nothing was
    /// ingested.
    Prepare(Vec<crate::adapter::PrepareFailure>),
    /// The source cannot be read: it could not start (a refused start-up
    /// check, a held lock, an unknown `type:`) or lost its connection.
    Unavailable(String),
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceError::Io(e) => write!(f, "{}", e),
            SourceError::Parse(s) => write!(f, "source parse error: {}", s),
            SourceError::Checkpoint(s) => write!(f, "checkpoint error: {}", s),
            SourceError::Unavailable(s) => write!(f, "source unavailable: {}", s),
            SourceError::Prepare(failures) => {
                write!(f, "adapters failed to prepare:")?;
                for failure in failures {
                    write!(f, " {failure};")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for SourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SourceError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for SourceError {
    fn from(e: std::io::Error) -> Self {
        SourceError::Io(e)
    }
}

impl From<crate::replay::ReplayLoadError> for SourceError {
    fn from(e: crate::replay::ReplayLoadError) -> Self {
        match e {
            crate::replay::ReplayLoadError::Io(e) => SourceError::Io(e),
            e @ crate::replay::ReplayLoadError::Json { .. } => SourceError::Parse(e.to_string()),
        }
    }
}

/// A pluggable, checkpointed event source — the input-side mirror of
/// [`StorageAdapter`](crate::adapter::StorageAdapter). Three methods, exactly
/// like the sink trait: a future Kafka / outbox / HTTP source implements the
/// same shape.
pub trait EventSource {
    /// Stable source identifier (from config).
    fn id(&self) -> &str;

    /// The next batch of events in delivery order, at most `max_batch` of them.
    /// An empty `Vec` means "caught up for now". Each event carries the
    /// [`SourcePosition`] needed to commit past it.
    fn poll(&mut self, max_batch: usize) -> Result<Vec<SourcedEvent>, SourceError>;

    /// Durably record that everything this source delivered up to and
    /// including the event at `position` has resolved. `position` is that of
    /// the last event, in delivery order, of the batch the last `poll`
    /// returned. Called by the run loop **only after** the batch fully
    /// resolves (every adapter outcome recorded: success, skip, or DLQ). A
    /// committed position never moves backward, by the source's own order.
    /// After a restart the source resumes after it.
    fn commit(&mut self, position: SourcePosition) -> Result<(), SourceError>;

    /// The currently committed position, if any (for `kulyrax sources` and
    /// resume). Read from the checkpoint at construction.
    fn committed_position(&self) -> Option<&SourcePosition>;

    /// Like [`commit`](Self::commit), but also reports how many of this
    /// batch's events were DLQ'd rather than genuinely projected (0 when no
    /// DLQ is configured, or none of this batch needed it). The position
    /// still advances either way — a DLQ'd event is done with, not retried —
    /// this is purely an extra signal for a source that can forward it
    /// somewhere a consumer might see it (`GrpcSource`, into the `Ack`).
    /// Default: ignore the count and defer to `commit`, so every existing
    /// source (`directory`, `stdin`) needs no change.
    fn commit_with_dlq_count(
        &mut self,
        position: SourcePosition,
        _dlq_events: u32,
    ) -> Result<(), SourceError> {
        self.commit(position)
    }

    /// Told each event's outcome once its batch has resolved, before the
    /// batch is committed. Default: nothing.
    fn on_resolved(&mut self, _event: &SourcedEvent, _report: &crate::execution::ExecutionReport) {}

    /// Extra `name: value` lines for `kulyrax sources`. Default: none.
    fn details(&self) -> Vec<(String, String)> {
        Vec::new()
    }
}

/// Take the exclusive OS lock that makes a source the only reader of its
/// checkpoint: `<state_dir>/<source_id>.lock`, held while the returned file
/// is open, released when the process exits however it exits. A second
/// process on the same source id and state directory gets
/// [`SourceError::Unavailable`] naming the lock.
pub fn lock_source(
    state_dir: &std::path::Path,
    source_id: &str,
) -> Result<std::fs::File, SourceError> {
    std::fs::create_dir_all(state_dir)?;
    let path = state_dir.join(format!("{source_id}.lock"));
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)?;
    match file.try_lock() {
        Ok(()) => Ok(file),
        Err(std::fs::TryLockError::WouldBlock) => Err(SourceError::Unavailable(format!(
            "source {source_id:?} is in use by another process (lock {} is held); only one \
             Kulyrax process may read a source",
            path.display()
        ))),
        Err(std::fs::TryLockError::Error(e)) => Err(SourceError::Io(e)),
    }
}

impl SourcedEvent {
    /// An event and the position to commit past it. `SourcedEvent` is
    /// `#[non_exhaustive]`; a source builds it with this.
    pub fn new(event: Event, position: SourcePosition) -> Self {
        Self { event, position }
    }
}
