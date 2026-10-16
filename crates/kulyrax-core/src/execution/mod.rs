//! Execution report types: the **stable public contract** for [crate::execute_event].
//!
//! [ExecutionReport] is the official return type — not internal, not transitional.
//! Use these types to interpret or serialize execution outcomes.

pub mod report;
pub mod time;
pub mod time_option;
pub use report::*;
