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
    /// The store's format marker refuses the start: newer than this version
    /// reads, not recognised, or absent and not creatable. Raised by
    /// `prepare`, before any guard is read.
    Format(crate::format::FormatError),
}

/// A format refusal stays typed ([`crate::adapter::AdapterError::Format`],
/// exit 4); any other document error is a write failure with its message.
impl From<DocumentError> for crate::adapter::AdapterError {
    fn from(e: DocumentError) -> Self {
        match e {
            DocumentError::Format(f) => crate::adapter::AdapterError::Format(f),
            other => crate::adapter::AdapterError::WriteFailed(other.to_string()),
        }
    }
}

impl From<crate::format::FormatError> for DocumentError {
    fn from(e: crate::format::FormatError) -> Self {
        DocumentError::Format(e)
    }
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
            DocumentError::Format(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for DocumentError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DocumentError::Format(e) => Some(e),
            _ => None,
        }
    }
}
