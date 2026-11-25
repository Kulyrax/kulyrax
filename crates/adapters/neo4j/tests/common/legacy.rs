//! The 0.12 names the Neo4j wipe also removes: before 1.0, Kulyrax was called
//! `conduit`. Only `legacy.rs` files and the migration's tests may name it.

/// The 0.12 guard label.
pub const OLD_GUARD_LABEL: &str = "__ConduitGuard";

/// The prefix of the constraints and indexes 0.12 named.
pub const OLD_NAME_PREFIX: &str = "conduit_";
