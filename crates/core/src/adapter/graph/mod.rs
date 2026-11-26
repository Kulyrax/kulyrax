pub mod adapter;
pub mod exec;
pub(crate) mod loader;
pub mod mapping;
pub mod runtime;
pub mod store;

/// The public extension surface for a new graph backend (Phase 25.1) — see
/// [`exec::GraphBackend`]'s doc comment for the atomicity contract.
/// Canonical path: `exec`; this re-export is kept for 0.12's paths
/// (decision 2026-11-26-canonical-paths).
#[doc(hidden)]
pub use exec::{EdgePlan, GraphBackend, GraphFacetGuard, GraphGuard, GraphOutcome, NodePlan};
