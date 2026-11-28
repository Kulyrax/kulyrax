use std::fmt;

/// Errors related to graph projection and writing. Structurally identical to
/// `KvError` / `DocumentError` / `SqlError` — same four cases, same meaning,
/// named for the fourth storage kind (Phase 16).
#[derive(Debug)]
#[non_exhaustive]
pub enum GraphError {
    /// No mapping exists for the given event type.
    MappingNotFound(String),

    /// Payload JSON is invalid or incompatible with the mapping.
    BuildFailed(String),

    /// Failed to write the record / guard / incident index to storage.
    WriteFailed(String),

    /// Phase 24.3: a backend's optimistic CAS (Neo4j's guard-node
    /// `WHERE`-conditioned `SET`, inside a transaction) was aborted by a
    /// concurrent writer. Retryable — the caller re-runs the whole
    /// read-decide-write cycle from scratch against a freshly re-read guard,
    /// not returned as a hard failure.
    WriteConflict,

    /// No upcaster chain from the event's version to the mapping's target version.
    UnsupportedVersion {
        /// The event's type.
        event_type: String,
        /// The payload version the event arrived with.
        from_version: u32,
        /// The mapping's version.
        to_version: u32,
        /// Why no upcast reaches it.
        reason: String,
    },
    /// The store's format marker refuses the start: newer than this version
    /// reads, not recognised, or absent and not creatable. Raised by
    /// `prepare`, before any guard is read.
    Format(crate::format::FormatError),
}

/// A format refusal stays typed ([`crate::adapter::AdapterError::Format`],
/// exit 4); any other graph error is a write failure with its message.
impl From<GraphError> for crate::adapter::AdapterError {
    fn from(e: GraphError) -> Self {
        match e {
            GraphError::Format(f) => crate::adapter::AdapterError::Format(f),
            other => crate::adapter::AdapterError::WriteFailed(other.to_string()),
        }
    }
}

impl From<crate::format::FormatError> for GraphError {
    fn from(e: crate::format::FormatError) -> Self {
        GraphError::Format(e)
    }
}

impl fmt::Display for GraphError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GraphError::MappingNotFound(event) => {
                write!(f, "no graph mapping found for event '{}'", event)
            }
            GraphError::BuildFailed(msg) => {
                write!(f, "failed to build graph projection: {}", msg)
            }
            GraphError::WriteFailed(msg) => {
                write!(f, "graph write failed: {}", msg)
            }
            GraphError::WriteConflict => {
                write!(f, "graph write conflict: guard changed concurrently")
            }
            GraphError::UnsupportedVersion {
                event_type,
                from_version,
                to_version,
                reason,
            } => write!(
                f,
                "cannot project event {:?} from v{} to mapping v{}: {}",
                event_type, from_version, to_version, reason
            ),
            GraphError::Format(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for GraphError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            GraphError::Format(e) => Some(e),
            _ => None,
        }
    }
}
