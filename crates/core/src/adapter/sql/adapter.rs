use std::fmt;

/// SQL-side adapter / builder errors
#[derive(Debug)]
#[non_exhaustive]
pub enum SqlError {
    /// No SQL mapping exists for this event type.
    MappingNotFound(String),
    /// The row could not be built from the event (a missing field, a bad
    /// value).
    BuildFailed(String),
    /// The database refused the statement, or could not be reached.
    ExecutionFailed(String),
    /// A shadow rebuild refused to start or to swap: a check that protects
    /// the live tables failed. Nothing was changed.
    RebuildRefused(String),
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
    /// The database's format marker (`kulyrax_format`) refuses the start:
    /// newer than this version reads, not recognised, or absent and not
    /// creatable. Raised by `prepare`, before the guard table is touched.
    Format(crate::format::FormatError),
}

/// A format refusal stays typed ([`AdapterError::Format`], exit 4); any other
/// SQL error is a write failure with its message.
impl From<SqlError> for crate::adapter::AdapterError {
    fn from(e: SqlError) -> Self {
        match e {
            SqlError::Format(f) => crate::adapter::AdapterError::Format(f),
            other => crate::adapter::AdapterError::WriteFailed(other.to_string()),
        }
    }
}

impl From<crate::format::FormatError> for SqlError {
    fn from(e: crate::format::FormatError) -> Self {
        SqlError::Format(e)
    }
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
            SqlError::RebuildRefused(msg) => write!(f, "{msg}"),
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
            SqlError::Format(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for SqlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SqlError::Format(e) => Some(e),
            _ => None,
        }
    }
}
