use core::fmt;

/// A runtime-level error. Nothing in Kulyrax returns it today; it is kept for
/// compatibility.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum RuntimeError {
    /// No adapter is available for routing.
    NoAdaptersForRouting,
    /// An adapter's config is invalid.
    AdapterConfigInvalid(String),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            RuntimeError::NoAdaptersForRouting => write!(f, "no adapters available for routing"),
            RuntimeError::AdapterConfigInvalid(msg) => write!(f, "invalid adapter config: {}", msg),
        }
    }
}

impl std::error::Error for RuntimeError {}
