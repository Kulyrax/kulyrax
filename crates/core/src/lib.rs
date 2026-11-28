//! The Kulyrax engine: a deterministic, projection-only engine for event
//! sourcing. It reads an event log it does not own and projects each event
//! into SQL, document, key-value and graph stores. Every write is gated by
//! the entity's sequence ([`adapter::decide`]), so a redelivered event is
//! skipped, never applied twice. Start with [`KulyraxRuntime`] to embed
//! it, or [`pipeline`] for what the `kulyrax` CLI does. A new backend
//! implements one of the storage kinds' traits (`docs/writing-a-backend.md`).
//!
//! Public paths (stability §1, decision 2026-11-26-canonical-paths): a type's
//! canonical path is the module that defines it. The crate root also
//! re-exports the front door (the runtime, reports, sources, upcasting,
//! validation). Every other re-export, and every module stability §1 marks
//! UNSTABLE, is `#[doc(hidden)]`: reachable, but outside semver.

#![deny(missing_docs)]

/// Storage adapters: the [`adapter::StorageAdapter`] trait, the four storage
/// kinds, the write decision ([`adapter::decide`]), and the built-in adapters.
pub mod adapter;
pub(crate) mod batch;
#[doc(hidden)]
pub mod crash;
#[doc(hidden)]
pub mod dispatch;
/// The [`event::Event`] envelope.
pub mod event;
pub mod execution;
pub mod format;
#[doc(hidden)]
pub mod kulyrax_dir;
pub mod pipeline;
pub mod rebuild;
#[doc(hidden)]
pub mod redact;
pub mod replay;
/// Routing: event types to adapter ids.
pub mod routing;
pub mod runtime;
pub(crate) mod scaffold;
pub mod source;
pub mod upcast;
pub mod yaml;

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
pub use execution::report::{
    AdapterExecutionReport, AdapterFailure, AdapterOutcome, ExecutionReport, ExecutionStatus,
};

/// Phase 10.2 upcaster registry for schema evolution.
pub use upcast::{UpcastError, Upcaster, UpcasterRegistry};

#[doc(hidden)]
pub use replay::{
    EventsFromPath, PerEventReplaySummary, ReplayContext, ReplayLoadError, events_from_path,
    replay_stream, replay_stream_with_options,
};
/// Phase 7 replay API.
pub use replay::{ReplayReport, ReplayRunOptions};

#[doc(hidden)]
pub use source::runner::run_sources_with_adapters;
/// Phase 17 event-source API.
pub use source::runner::{RunMode, SourceRunOptions, SourceRunReport, StoppedReason, run_sources};
pub use source::{EventSource, SourceError, SourcePosition, SourcedEvent};

/// Phase 8 projection validation.
pub use runtime::validation::{ValidationIssue, ValidationReport, validate_projection_config};
#[doc(hidden)]
pub use runtime::{
    AdapterExecutionMeta, PROJECTION_DEPTH_EXCEEDS_RECOMMENDED_WARNING,
    RECOMMENDED_MAX_DEPENDENCY_LAYER, adapter_metadata_map, dependency_depth_exceeds_recommended,
    dependency_layers_grouped, dependency_layers_parallel, execution_order_for_routed,
    max_dependency_layer, validate_routing_adapter_ids,
    validate_routing_and_dependencies_for_event_type, validate_routing_for_event_type,
};

/// Phase 27: the facade type — owns config, mappings, routing, an
/// instance-scoped [`AdapterRegistry`], and (once built) the resolved
/// adapters, exposing `run_once`/`dry_run_once`/`replay`/`run_sources` as
/// methods instead of free functions that each re-thread the same bundle.
pub use runtime::engine::KulyraxRuntime;
pub use runtime::registry::{AdapterBuildContext, AdapterRegistry, MappingSet};

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
    // An adapter that fails to prepare fails this event, with the prepare
    // error in the report.
    let _ = crate::adapter::prepare_adapters(&adapters);
    let adapter_meta = crate::runtime::adapter_metadata_map(config);
    dispatch(
        &event,
        &mut adapters,
        config.failure_policy,
        routing_rules,
        &adapter_meta,
    )
}
