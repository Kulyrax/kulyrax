pub mod adapter;
pub mod exec;
pub mod loader;
pub mod mapping;
pub mod mysql;
pub mod preview;
pub mod runtime;
pub mod sqlite;

/// The public extension surface for a new SQL backend (Phase 25.1) — see
/// [`exec::SqlTxn`]'s doc comment for the atomicity contract.
pub use exec::{GuardRow, Placeholders, SqlOutcome, SqlPlan, SqlTxn, SqlWrite};
