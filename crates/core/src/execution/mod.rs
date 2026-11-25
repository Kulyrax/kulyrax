//! Execution report types: the **stable public contract** for [crate::execute_event].
//!
//! [ExecutionReport] is the official return type — not internal, not transitional.
//! Use these types to interpret or serialize execution outcomes. Their
//! canonical path is `execution::report`; the crate root re-exports them too.

pub mod report;
#[doc(hidden)]
pub mod stages;
pub(crate) mod time;
pub(crate) mod time_option;
#[doc(hidden)]
pub use report::*;
