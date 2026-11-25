pub mod adapter;
pub mod exec;
pub mod file;
pub(crate) mod loader;
pub mod mapping;
pub mod runtime;

/// The public extension surface for a new document backend (Phase 25.1) —
/// see [`exec::DocumentBackend`]'s doc comment for the atomicity contract.
/// Canonical path: `exec`; this re-export is kept for 0.12's paths
/// (decision 2026-11-26-canonical-paths).
#[doc(hidden)]
pub use exec::{DocumentBackend, DocumentOutcome, DocumentPlan, FacetGuard, ProjectionGuard};
