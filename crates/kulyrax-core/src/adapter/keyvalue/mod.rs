pub mod adapter;
pub mod exec;
pub mod loader;
pub mod mapping;
pub mod preview;
pub mod redis;
pub mod runtime;
pub mod store;

/// The public extension surface for a new key-value backend (Phase 25.1) —
/// see [`exec::KvBackend`]'s doc comment for the atomicity contract.
pub use exec::{KvBackend, KvFacetGuard, KvGuard, KvOutcome, KvPlan};
