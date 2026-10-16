use std::fmt;

/// SQL-side adapter / builder errors
#[derive(Debug)]
pub enum SqlError {
    MappingNotFound(String),
    BuildFailed(String),
    ExecutionFailed(String),
    /// No upcaster chain from the event's version to the mapping's target version.
    UnsupportedVersion {
        event_type: String,
        from_version: u32,
        to_version: u32,
        reason: String,
    },
}

impl fmt::Display for SqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SqlError::MappingNotFound(event) => {
                write!(f, "no SQL mapping found for event '{}'", event)
            }
            SqlError::BuildFailed(msg) => {
                write!(f, "failed to build SQL projection: {}", msg)
            }
            SqlError::ExecutionFailed(msg) => {
                write!(f, "SQL execution failed: {}", msg)
            }
            SqlError::UnsupportedVersion {
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

impl std::error::Error for SqlError {}
