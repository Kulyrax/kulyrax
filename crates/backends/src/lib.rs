//! Registers the default set of `kulyrax-adapter-*` backends and
//! `kulyrax-source-*` sources on a runtime, one Cargo feature each. An
//! embedder that needs only one can depend on its crate directly and skip
//! this one.

use kulyrax_core::KulyraxRuntime;

/// Register every backend enabled by feature. Call before the runtime builds
/// its adapters (its first `run_once` / `replay` /
/// `run_sources` call).
#[allow(unused_variables)] // `runtime` is unused when every backend feature is off.
pub fn register_default_backends(runtime: &mut KulyraxRuntime) {
    #[cfg(feature = "postgres")]
    runtime.register_adapter_factory("postgres", kulyrax_adapter_postgres::factory);

    #[cfg(feature = "elasticsearch")]
    runtime.register_adapter_factory("elasticsearch", kulyrax_adapter_elasticsearch::factory);

    #[cfg(feature = "mysql")]
    runtime.register_adapter_factory("mysql", kulyrax_adapter_mysql::factory);

    #[cfg(feature = "redis")]
    runtime.register_adapter_factory("redis", kulyrax_adapter_redis::factory);

    #[cfg(feature = "mongodb")]
    runtime.register_adapter_factory("mongodb", kulyrax_adapter_mongodb::factory);

    #[cfg(feature = "neo4j")]
    runtime.register_adapter_factory("neo4j", kulyrax_adapter_neo4j::factory);

    #[cfg(feature = "postgres-outbox")]
    runtime.register_source_factory("postgres_outbox", kulyrax_source_postgres::factory);
}
