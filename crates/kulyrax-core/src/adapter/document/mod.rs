pub mod adapter;
pub mod exec;
pub mod file;
pub mod loader;
pub mod mapping;
pub mod mongo;
pub mod preview;
pub mod runtime;

/// The public extension surface for a new document backend (Phase 25.1) —
/// see [`exec::DocumentBackend`]'s doc comment for the atomicity contract.
pub use exec::{DocumentBackend, DocumentOutcome, DocumentPlan, FacetGuard, ProjectionGuard};
