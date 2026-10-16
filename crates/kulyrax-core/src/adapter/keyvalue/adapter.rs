use std::fmt;

/// Errors related to key-value projection and writing. Structurally identical
/// to `DocumentError` / `SqlError` — same four cases, same meaning, just
/// named for the third storage kind (Phase 14).
#[derive(Debug)]
pub enum KvError {
    /// No mapping exists for the given event type
    MappingNotFound(String),

    /// Payload JSON is invalid or incompatible with mapping
    BuildFailed(String),

    /// Failed to write the value/guard to storage
    WriteFailed(String),

    /// Phase 22.3: a backend's optimistic transaction (Redis WATCH/MULTI/EXEC)
    /// was aborted by a concurrent writer. Retryable — the caller re-runs
    /// [`super::exec::project`] from scratch against a freshly re-read guard,
    /// not returned as a hard failure.
    WriteConflict,

    /// No upcaster chain from the event's version to the mapping's target version.
    UnsupportedVersion {
        event_type: String,
        from_version: u32,
        to_version: u32,
        reason: String,
    },
}

impl fmt::Display for KvError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            KvError::MappingNotFound(event) => {
                write!(f, "no key-value mapping found for event '{}'", event)
            }
            KvError::BuildFailed(msg) => {
                write!(f, "failed to build key-value projection: {}", msg)
            }
            KvError::WriteFailed(msg) => {
                write!(f, "key-value write failed: {}", msg)
            }
            KvError::WriteConflict => {
                write!(f, "key-value write conflict: guard changed concurrently")
            }
            KvError::UnsupportedVersion {
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

impl std::error::Error for KvError {}
