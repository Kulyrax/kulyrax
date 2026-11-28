//! On-disk format versions (step 5; decision `2026-11-28-format-versions`).
//!
//! Every format Kulyrax persists and reads back records its version: one
//! marker per store (a file-store root, a database, an index, …), or a field
//! inside a file that stands alone (a source checkpoint, a batch log). Data
//! with no version was written by 0.12.x and is read as format 1.
//!
//! A version newer than this build reads, content that is not a Kulyrax
//! format, or a marker that could not be created all refuse the start with a
//! [`FormatError`], before anything is read, replayed or written. The CLI exits
//! 4 for it.

use std::fmt;
use std::io;

/// A store or file whose format this version refuses to read.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct FormatError {
    /// What was refused: a file path, or a backend and the store's name
    /// (never a URL, so never a credential).
    pub store: String,
    /// Why.
    pub problem: FormatProblem,
}

/// Why a [`FormatError`] refused a store.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum FormatProblem {
    /// Written by a newer Kulyrax: format `found`, and this version reads up
    /// to `supported`.
    Newer {
        /// The version recorded.
        found: u32,
        /// The newest version this build reads.
        supported: u32,
    },
    /// Not a Kulyrax format this version knows; the text says what was found.
    Unrecognized(String),
    /// The format marker is absent and could not be created.
    MarkerNotCreated {
        /// The right the store's user needs, as the backend names it.
        grant: String,
        /// The backend's error (redacted of credentials).
        cause: String,
    },
}

impl FormatError {
    /// A refusal of `store` for `problem`.
    pub fn new(store: impl Into<String>, problem: FormatProblem) -> Self {
        Self {
            store: store.into(),
            problem,
        }
    }

    /// `Ok` when `found` is a version this build reads (`1..=supported`).
    pub fn check(store: &str, found: u32, supported: u32) -> Result<(), Self> {
        match found {
            0 => Err(Self::new(
                store,
                FormatProblem::Unrecognized("format version 0".to_string()),
            )),
            v if v > supported => Err(Self::new(store, FormatProblem::Newer { found, supported })),
            _ => Ok(()),
        }
    }

    /// Carry this error through an [`io::Error`], for code that returns one.
    pub(crate) fn into_io(self) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, self)
    }

    /// The `FormatError` an [`io::Error`] carries, if it carries one.
    pub(crate) fn from_io(e: &io::Error) -> Option<Self> {
        e.get_ref()
            .and_then(|inner| inner.downcast_ref::<Self>())
            .cloned()
    }
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.problem {
            FormatProblem::Newer { found, supported } => write!(
                f,
                "{} was written by a newer Kulyrax (format {found}; this version reads \
                 formats up to {supported}). Upgrade Kulyrax to read it",
                self.store
            ),
            FormatProblem::Unrecognized(what) => write!(
                f,
                "{} is not in a format this version of Kulyrax knows ({what}); \
                 it was left as it is",
                self.store
            ),
            FormatProblem::MarkerNotCreated { grant, cause } => write!(
                f,
                "{} has no format marker and Kulyrax could not create one ({cause}). \
                 Grant the user Kulyrax connects as {grant}",
                self.store
            ),
        }
    }
}

impl std::error::Error for FormatError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_accepts_known_versions_only() {
        assert!(FormatError::check("s", 1, 2).is_ok());
        assert!(FormatError::check("s", 2, 2).is_ok());
        assert_eq!(
            FormatError::check("s", 3, 2).map_err(|e| e.problem),
            Err(FormatProblem::Newer {
                found: 3,
                supported: 2
            })
        );
        assert!(matches!(
            FormatError::check("s", 0, 2).map_err(|e| e.problem),
            Err(FormatProblem::Unrecognized(_))
        ));
    }

    #[test]
    fn survives_an_io_error() {
        let e = FormatError::new("store", FormatProblem::Unrecognized("x".into()));
        let io = e.clone().into_io();
        assert_eq!(FormatError::from_io(&io), Some(e));
        assert_eq!(FormatError::from_io(&io::Error::other("plain")), None);
    }
}
