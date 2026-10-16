pub mod adapter;
pub mod exec;
pub mod loader;
pub mod mapping;
pub mod neo4j;
pub mod preview;
pub mod runtime;
pub mod store;

/// The public extension surface for a new graph backend (Phase 25.1) — see
/// [`exec::GraphBackend`]'s doc comment for the atomicity contract.
pub use exec::{EdgePlan, GraphBackend, GraphFacetGuard, GraphGuard, GraphOutcome, NodePlan};
