pub mod adapter;
pub mod dispatch;
pub mod event;
pub mod execution;
pub mod pipeline;
pub mod replay;
pub mod routing;
pub mod runtime;
pub mod source;
pub mod upcast;

use std::collections::HashMap;
use std::sync::Arc;

use crate::{
    adapter::{
        document::mapping::DocumentMapping, graph::mapping::GraphMapping,
        keyvalue::mapping::KvMapping, sql::mapping::SqlMapping,
    },
    dispatch::dispatch,
    event::Event,
    runtime::{build_adapters_from_config, config::KulyraxConfig},
};

// ---------------------------------------------------------------------------
// Public API: execution contract (stable)
// ---------------------------------------------------------------------------
//
// [ExecutionReport] is the official return type of [execute_event]. It is not
// an internal artifact or transitional type. Consumers depend on this contract.

/// Re-export at crate root so users can `use kulyrax_core::ExecutionReport`.
pub use execution::{
    AdapterExecutionReport, AdapterOutcome, AdapterReportError, ExecutionMode, ExecutionReport,
    ExecutionStatus,
};

/// Phase 10.2 upcaster registry for schema evolution.
pub use upcast::{UpcastError, Upcaster, UpcasterRegistry};

/// Phase 7 replay API.
pub use replay::{
    EventsFromPath, PerEventReplaySummary, ReplayContext, ReplayLoadError, ReplayReport,
    ReplayRunOptions, events_from_path, replay_stream, replay_stream_with_options,
};

/// Phase 17 event-source API.
pub use source::runner::{
    RunMode, SourceRunOptions, SourceRunReport, StoppedReason, run_sources,
    run_sources_with_adapters,
};
pub use source::{EventSource, SourceError, SourcePosition, SourcedEvent};

/// Phase 8 projection validation.
pub use runtime::{
    AdapterExecutionMeta, PROJECTION_DEPTH_EXCEEDS_RECOMMENDED_WARNING,
    RECOMMENDED_MAX_DEPENDENCY_LAYER, ValidationIssue, ValidationReport, adapter_metadata_map,
    dependency_depth_exceeds_recommended, dependency_layers_grouped, dependency_layers_parallel,
    execution_order_for_routed, max_dependency_layer, validate_projection_config,
    validate_routing_adapter_ids, validate_routing_and_dependencies_for_event_type,
    validate_routing_for_event_type,
};

/// Phase 27: the facade type — owns config, mappings, routing, an
/// instance-scoped [`AdapterRegistry`], and (once built) the resolved
/// adapters, exposing `run_once`/`dry_run_once`/`replay`/`run_sources` as
/// methods instead of free functions that each re-thread the same bundle.
pub use runtime::{AdapterBuildContext, AdapterRegistry, KulyraxRuntime, MappingSet};

/// Execution report types (also at [crate root](crate) for convenience).
pub mod report {
    pub use crate::execution::{
        AdapterExecutionReport, AdapterOutcome, AdapterReportError, ExecutionMode, ExecutionReport,
        ExecutionStatus,
    };
}

// ---------------------------------------------------------------------------
// Execution entry point
// ---------------------------------------------------------------------------

/// Execute a single event using a validated configuration.
///
/// **Stable contract:** returns an [ExecutionReport]. This is the official
/// public API for execution — not internal, not incidental. The report is
/// the single contract for observability and outcome.
///
/// Runs with an empty [UpcasterRegistry] — events whose version doesn't match
/// a mapping's target version are handled per `config.migration_policy` with no
/// upcaster chain available. Use [execute_event_with_upcasters] to register
/// upcasters for schema-evolving payloads.
///
/// Requirements:
/// - `config.validate()` MUST be called before this function
/// - call [`validate_projection_config`] at startup when config + routing + mappings are loaded (CLI does this for run/replay/dry-run)
/// - mappings are consumed exactly once
/// - `routing_rules` is caller-loaded (e.g. via [`pipeline::load_project`]) — there is
///   no implicit global routing table
#[allow(clippy::too_many_arguments)]
pub fn execute_event(
    config: &KulyraxConfig,
    sql_mappings: HashMap<String, SqlMapping>,
    document_mappings: HashMap<String, DocumentMapping>,
    kv_mappings: HashMap<String, KvMapping>,
    graph_mappings: HashMap<String, GraphMapping>,
    routing_rules: &HashMap<String, Vec<crate::routing::AdapterId>>,
    event: Event,
) -> ExecutionReport {
    execute_event_with_upcasters(
        config,
        sql_mappings,
        document_mappings,
        kv_mappings,
        graph_mappings,
        routing_rules,
        event,
        Arc::new(UpcasterRegistry::new()),
    )
}

/// Same as [execute_event], with an explicit [UpcasterRegistry] (Phase 10.3) for
/// projecting version-mismatched event payloads before mapping.
///
/// Builds adapters with an empty [`runtime::AdapterRegistry`] — an
/// `AdapterConfig::Custom` type is not resolvable through this low-level
/// entry point. Use [`KulyraxRuntime`] to register custom adapter factories.
#[allow(clippy::too_many_arguments)]
pub fn execute_event_with_upcasters(
    config: &KulyraxConfig,
    sql_mappings: HashMap<String, SqlMapping>,
    document_mappings: HashMap<String, DocumentMapping>,
    kv_mappings: HashMap<String, KvMapping>,
    graph_mappings: HashMap<String, GraphMapping>,
    routing_rules: &HashMap<String, Vec<crate::routing::AdapterId>>,
    event: Event,
    upcasters: Arc<UpcasterRegistry>,
) -> ExecutionReport {
    let mut adapters = build_adapters_from_config(
        config,
        crate::runtime::MappingSet {
            sql: sql_mappings,
            document: document_mappings,
            kv: kv_mappings,
            graph: graph_mappings,
        },
        upcasters,
        &runtime::AdapterRegistry::new(),
    );
    let adapter_meta = crate::runtime::adapter_metadata_map(config);
    dispatch(
        &event,
        &mut adapters,
        config.failure_policy,
        routing_rules,
        &adapter_meta,
    )
}

/// Execute a single event with an explicit [ExecutionMode] (e.g. [execution::ExecutionMode::DryRun] for no-write simulation).
/// Same as [execute_event] when mode is [execution::ExecutionMode::Run].
/// When mode is DryRun, the pipeline runs but adapters are expected to skip persistence (adapter support is required for true dry-run).
#[allow(clippy::too_many_arguments)]
pub fn execute_event_with_mode(
    config: &KulyraxConfig,
    sql_mappings: HashMap<String, SqlMapping>,
    document_mappings: HashMap<String, DocumentMapping>,
    kv_mappings: HashMap<String, KvMapping>,
    graph_mappings: HashMap<String, GraphMapping>,
    routing_rules: &HashMap<String, Vec<crate::routing::AdapterId>>,
    event: Event,
    mode: crate::execution::ExecutionMode,
) -> ExecutionReport {
    match mode {
        crate::execution::ExecutionMode::Run => execute_event(
            config,
            sql_mappings,
            document_mappings,
            kv_mappings,
            graph_mappings,
            routing_rules,
            event,
        ),
        crate::execution::ExecutionMode::DryRun => {
            let mut adapters = build_adapters_from_config(
                config,
                crate::runtime::MappingSet {
                    sql: sql_mappings,
                    document: document_mappings,
                    kv: kv_mappings,
                    graph: graph_mappings,
                },
                Arc::new(UpcasterRegistry::new()),
                &runtime::AdapterRegistry::new(),
            );
            let adapter_meta = crate::runtime::adapter_metadata_map(config);
            dispatch(
                &event,
                &mut adapters,
                config.failure_policy,
                routing_rules,
                &adapter_meta,
            )
        }
    }
}
