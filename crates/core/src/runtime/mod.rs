//! The runtime: config, registries, the [`engine::KulyraxRuntime`] facade,
//! validation, dry runs and reload. Each type's canonical path is its module
//! (`runtime::engine::KulyraxRuntime`, `runtime::dry_run::DryRunReport`, …).
//! The flat re-exports below are kept for 0.12's paths but are
//! `#[doc(hidden)]`, outside semver (decision 2026-11-26-canonical-paths).

/// `config.yaml`: [`config::KulyraxConfig`] and every entry type.
pub mod config;
#[doc(hidden)]
pub mod dependency_graph;
pub mod dry_run;
pub mod engine;
pub(crate) mod execute;
pub(crate) mod factory;
pub mod registry;
pub mod reload;
pub mod validation;

#[doc(hidden)]
pub use dependency_graph::{
    AdapterExecutionMeta, PROJECTION_DEPTH_EXCEEDS_RECOMMENDED_WARNING,
    RECOMMENDED_MAX_DEPENDENCY_LAYER, adapter_metadata_map, dependency_depth_exceeds_recommended,
    dependency_layers_grouped, dependency_layers_parallel, execution_order_for_routed,
    max_dependency_layer,
};
#[doc(hidden)]
pub use dry_run::{DryRunAdapter, DryRunOutcome, DryRunReport, PlannedWrite};
#[doc(hidden)]
pub use engine::KulyraxRuntime;
#[doc(hidden)]
pub use factory::{
    AdapterBuildFailure, build_adapters_from_config, build_adapters_reporting_failures,
};
#[doc(hidden)]
pub use registry::{
    AdapterBuildContext, AdapterFactory, AdapterRegistry, MappingSet, SourceBuildContext,
    SourceFactory, SourceRegistry,
};
#[doc(hidden)]
pub use reload::{FileReloader, ReloadOutcome, request_reload};
#[doc(hidden)]
pub use validation::{
    ValidationIssue, ValidationReport, validate_projection_config, validate_routing_adapter_ids,
    validate_routing_and_dependencies_for_event_type, validate_routing_for_event_type,
};
