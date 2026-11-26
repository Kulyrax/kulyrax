/// The graph error type.
pub mod adapter;
pub mod exec;
pub(crate) mod loader;
/// The graph mapping format.
pub mod mapping;
/// Resolving a graph mapping against an event.
pub mod runtime;
/// The file-backed graph store (`type: graph`).
pub mod store;

// The extension surface for a new graph backend (Phase 25.1) is `exec`:
// its traits, plans and guards, and the functions that drive them. 0.12 also
// re-exported its types here; since 1.0 each has one path (decision
// 2026-11-26-canonical-paths).
