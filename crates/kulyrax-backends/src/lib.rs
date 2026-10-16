//! Phase 28: thin wiring crate. `kulyrax-core` stopped shipping every
//! networked backend's driver unconditionally — each one (Postgres, and
//! eventually MySQL/Redis/MongoDB/Neo4j) now lives in its own
//! `kulyrax-adapter-*` crate and resolves via [`kulyrax_core::KulyraxRuntime`]'s
//! instance-scoped adapter registry, exactly like a third-party community
//! backend would. This crate's only job is registering the default set so
//! `kulyrax-cli` (and any other embedder who wants "every backend works out
//! of the box") stays a one-line call instead of hand-rolling the wiring
//! itself.
//!
//! One Cargo feature per backend (`default = ["all"]` enables every backend
//! currently split out) — an embedder who only needs Postgres can depend on
//! `kulyrax-adapter-postgres` directly and skip this crate entirely.

use std::sync::Arc;

use kulyrax_core::KulyraxRuntime;

/// Register every backend compiled into this crate (by feature) onto
/// `runtime`. Call this **before** the runtime's adapters build — i.e.
/// before the first `run_once`/`dry_run_once`/`replay`/`run_sources` call —
/// the same contract [`KulyraxRuntime::register_adapter_factory`] documents.
///
/// `#[allow(unused_variables)]`: with every backend feature disabled (e.g.
/// `--no-default-features`), this becomes a no-op and `runtime` goes unused.
#[allow(unused_variables)]
pub fn register_default_backends(runtime: &mut KulyraxRuntime) {
    #[cfg(feature = "postgres")]
    {
        let mappings = runtime.sql_mappings().cloned().unwrap_or_default();
        let upcasters = runtime.upcasters();
        let migration_policy = runtime.config().migration_policy;
        runtime.register_adapter_factory("postgres", move |raw| {
            kulyrax_adapter_postgres::factory(
                raw,
                mappings.clone(),
                Arc::clone(&upcasters),
                migration_policy,
            )
        });
    }
}
