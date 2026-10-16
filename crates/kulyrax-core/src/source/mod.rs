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

/// An opaque, source-defined position. Lexicographically comparable **within a
/// single source** — a source encodes its progress however it likes (a file
/// name, a zero-padded line number, later a Kafka offset), as long as
/// `a < b` in string order iff `a` precedes `b` in delivery order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
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
pub struct SourcedEvent {
    pub event: Event,
    pub position: SourcePosition,
}

/// Failure reading from or checkpointing a source.
#[derive(Debug)]
pub enum SourceError {
    Io(std::io::Error),
    Parse(String),
    Checkpoint(String),
}

impl std::fmt::Display for SourceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SourceError::Io(e) => write!(f, "{}", e),
            SourceError::Parse(s) => write!(f, "source parse error: {}", s),
            SourceError::Checkpoint(s) => write!(f, "checkpoint error: {}", s),
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
            crate::replay::ReplayLoadError::Json(s) => SourceError::Parse(s),
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

    /// Durably record that everything up to and including `position` has been
    /// projected. Called by the run loop **only after** a batch fully resolves
    /// (every adapter outcome recorded — success, skip, or DLQ). A committed
    /// position never moves backward.
    fn commit(&mut self, position: SourcePosition) -> Result<(), SourceError>;

    /// The currently committed position, if any (for `kulyrax sources` and
    /// resume). Read from the checkpoint at construction.
    fn committed_position(&self) -> Option<&SourcePosition>;
}
