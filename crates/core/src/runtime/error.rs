use core::fmt;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    NoAdaptersForRouting,
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
