use std::fmt;

/// Errors related to document projection and writing
#[derive(Debug)]
#[non_exhaustive]
pub enum DocumentError {
    /// No mapping exists for the given event type
    MappingNotFound(String),

    /// Payload JSON is invalid or incompatible with mapping
    BuildFailed(String),

    /// Failed to write the document to storage
    WriteFailed(String),

    /// Phase 23.3: a backend's transaction (MongoDB's client-session
    /// transaction) was aborted by a concurrent writer. Retryable — the
    /// caller re-runs [`super::exec::project`] from scratch in a fresh
    /// transaction against a freshly re-read guard, not returned as a hard
    /// failure.
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
}

impl fmt::Display for DocumentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DocumentError::MappingNotFound(event) => {
                write!(f, "no document mapping found for event '{}'", event)
            }
            DocumentError::BuildFailed(msg) => {
                write!(f, "failed to build document projection: {}", msg)
            }
            DocumentError::WriteFailed(msg) => {
                write!(f, "document write failed: {}", msg)
            }
            DocumentError::WriteConflict => {
                write!(f, "document write conflict: guard changed concurrently")
            }
            DocumentError::UnsupportedVersion {
                event_type,
                from_version,
                to_version,
                reason,
            } => write!(
                f,
                "cannot project event {:?} from v{} to mapping v{}: {}",
                event_type, from_version, to_version, reason
            ),
        }
    }
}

impl std::error::Error for DocumentError {}
