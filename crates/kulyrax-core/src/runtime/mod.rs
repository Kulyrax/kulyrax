pub mod config;
pub mod dependency_graph;
pub mod engine;
pub mod error;
pub mod execute;
pub mod factory;
pub mod registry;
pub mod result;
pub mod validation;

pub use dependency_graph::{
    AdapterExecutionMeta, PROJECTION_DEPTH_EXCEEDS_RECOMMENDED_WARNING,
    RECOMMENDED_MAX_DEPENDENCY_LAYER, adapter_metadata_map, dependency_depth_exceeds_recommended,
    dependency_layers_grouped, dependency_layers_parallel, execution_order_for_routed,
    max_dependency_layer,
};
pub use engine::KulyraxRuntime;
pub use error::RuntimeError;
pub use factory::{
    AdapterBuildFailure, build_adapters_from_config, build_adapters_reporting_failures,
};
pub use registry::{AdapterBuildContext, AdapterFactory, AdapterRegistry, MappingSet};
pub use validation::{
    ValidationIssue, ValidationReport, validate_projection_config, validate_routing_adapter_ids,
    validate_routing_and_dependencies_for_event_type, validate_routing_for_event_type,
};
