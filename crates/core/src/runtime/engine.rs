//! `KulyraxRuntime`: one project's config, mappings, routing, adapter
//! registry and built adapters behind a single facade.
//!
//! Adapters are built once per runtime, lazily on first use, and reused for
//! every later event. The free `execute_event*` helpers rebuild (and
//! reconnect) on every call; an embedder that processes many events should
//! hold one runtime instead.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crate::adapter::StorageAdapter;
use crate::adapter::document::mapping::DocumentMapping;
use crate::adapter::graph::mapping::GraphMapping;
use crate::adapter::keyvalue::mapping::KvMapping;
use crate::adapter::sql::mapping::SqlMapping;
use crate::dispatch::dispatch;
use crate::event::Event;
use crate::execution::ExecutionReport;
use crate::pipeline::{PipelineError, load_project};
use crate::replay::{ReplayContext, ReplayLoadError, ReplayReport, ReplayRunOptions};
use crate::routing::AdapterId;
use crate::runtime::config::{ConfigError, CustomAdapterConfig, KulyraxConfig};
use crate::runtime::dependency_graph::{AdapterExecutionMeta, adapter_metadata_map};
use crate::runtime::dry_run::{DryRunReport, dry_run_event};
use crate::runtime::factory::build_adapters_reporting_failures;
use crate::runtime::registry::{AdapterBuildContext, AdapterRegistry, MappingSet};
use crate::source::runner::{
    Reloaded, Reloader, SourceRunOptions, SourceRunReport, run_sources_with_adapters,
    run_sources_with_reload,
};
use crate::source::{EventSource, SourceError};
use crate::upcast::UpcasterRegistry;

pub struct KulyraxRuntime {
    config: KulyraxConfig,
    /// Kept for the runtime's lifetime: every adapter build gets a copy, so
    /// adapters rebuilt after `run_sources` (which consumes them) still have
    /// their mappings, and `dry_run_once` can resolve writes without adapters.
    mappings: MappingSet,
    routing_rules: HashMap<String, Vec<AdapterId>>,
    adapter_meta: HashMap<AdapterId, AdapterExecutionMeta>,
    registry: AdapterRegistry,
    upcasters: Arc<UpcasterRegistry>,
    adapters: Option<Vec<Box<dyn StorageAdapter>>>,
}

impl KulyraxRuntime {
    /// Load and cross-validate config, mappings and routing, like
    /// [`crate::pipeline::load_project`]. Adapters are not built yet, so
    /// custom factories can still be registered.
    pub fn load(config_path: &Path, mappings_dir: &Path) -> Result<Self, PipelineError> {
        let project = load_project(config_path, mappings_dir)?;
        Ok(Self::new(
            project.config,
            MappingSet {
                sql: project.sql_mappings,
                document: project.doc_mappings,
                kv: project.kv_mappings,
                graph: project.graph_mappings,
            },
            project.routing_rules,
        ))
    }

    /// Build a runtime from in-memory parts. Skips the cross-validation that
    /// [`Self::load`] runs; call [`crate::runtime::validate_projection_config`]
    /// first if you want it.
    pub fn from_parts(
        config: KulyraxConfig,
        sql_mappings: HashMap<String, SqlMapping>,
        doc_mappings: HashMap<String, DocumentMapping>,
        kv_mappings: HashMap<String, KvMapping>,
        graph_mappings: HashMap<String, GraphMapping>,
        routing_rules: HashMap<String, Vec<AdapterId>>,
    ) -> Self {
        Self::new(
            config,
            MappingSet {
                sql: sql_mappings,
                document: doc_mappings,
                kv: kv_mappings,
                graph: graph_mappings,
            },
            routing_rules,
        )
    }

    fn new(
        config: KulyraxConfig,
        mappings: MappingSet,
        routing_rules: HashMap<String, Vec<AdapterId>>,
    ) -> Self {
        Self {
            adapter_meta: adapter_metadata_map(&config),
            config,
            mappings,
            routing_rules,
            registry: AdapterRegistry::new(),
            upcasters: Arc::new(UpcasterRegistry::new()),
            adapters: None,
        }
    }

    /// [`Self::load`], then `register_backends` (typically
    /// `kulyrax_backends::register_default_backends`).
    pub fn load_with_backends(
        config_path: &Path,
        mappings_dir: &Path,
        register_backends: impl FnOnce(&mut KulyraxRuntime),
    ) -> Result<Self, PipelineError> {
        let mut runtime = Self::load(config_path, mappings_dir)?;
        register_backends(&mut runtime);
        Ok(runtime)
    }

    pub fn config(&self) -> &KulyraxConfig {
        &self.config
    }

    pub fn upcasters(&self) -> Arc<UpcasterRegistry> {
        Arc::clone(&self.upcasters)
    }

    /// Register a factory for adapter `type: <type_name>`. Must happen before
    /// adapters are built (the first `run_once` / `replay` /
    /// `run_sources` call); an unregistered type builds as a failing
    /// placeholder.
    pub fn register_adapter_factory(
        &mut self,
        type_name: &str,
        factory: impl Fn(
            &CustomAdapterConfig,
            &AdapterBuildContext,
        ) -> Result<Box<dyn StorageAdapter>, ConfigError>
        + Send
        + Sync
        + 'static,
    ) {
        self.registry.register(type_name, factory);
    }

    /// Use these upcasters instead of the empty default. Must happen before
    /// adapters are built.
    pub fn with_upcasters(mut self, upcasters: Arc<UpcasterRegistry>) -> Self {
        self.upcasters = upcasters;
        self
    }

    /// Build the adapters now and fail if any custom adapter could not be
    /// built, so a bad URL or an unregistered `type:` stops startup instead of
    /// failing every event routed to it. Optional: the run methods build on
    /// first use either way.
    pub fn build_adapters(&mut self) -> Result<(), PipelineError> {
        if self.adapters.is_none() {
            let (adapters, failures) = build_adapters_reporting_failures(
                &self.config,
                self.mappings.clone(),
                Arc::clone(&self.upcasters),
                &self.registry,
            );
            self.adapters = Some(adapters);
            if !failures.is_empty() {
                return Err(PipelineError::AdapterBuild(failures));
            }
        }
        Ok(())
    }

    /// Build the adapters if needed, then prepare every one (see
    /// [`StorageAdapter::prepare`]): the first point any adapter touches its
    /// store. Every write path calls this before writing; `dry_run_once` never
    /// does. Safe to call again.
    pub fn prepare_adapters(&mut self) -> Result<(), PipelineError> {
        self.ensure_adapters_built();
        let failures =
            crate::adapter::prepare_adapters(self.adapters.as_deref().unwrap_or_default());
        if failures.is_empty() {
            Ok(())
        } else {
            Err(PipelineError::AdapterPrepare(failures))
        }
    }

    // Returns `()` rather than a borrow so callers can then use
    // `self.adapters` alongside other fields without a borrow conflict.
    // Build failures are already represented by failing placeholder adapters.
    fn ensure_adapters_built(&mut self) {
        let _ = self.build_adapters();
    }

    pub fn run_once(&mut self, event: Event) -> ExecutionReport {
        // An adapter that fails to prepare fails this event, with the prepare
        // error in the report. Call `prepare_adapters` first to stop instead.
        let _ = self.prepare_adapters();
        let adapters = self.adapters.get_or_insert_with(Vec::new);
        dispatch(
            &event,
            adapters,
            self.config.failure_policy,
            &self.routing_rules,
            &self.adapter_meta,
        )
    }

    /// Resolve what `event` would write to each adapter it routes to, without
    /// writing anything, building adapters, or contacting any store. See
    /// [`crate::runtime::dry_run`] for what the report does and doesn't cover.
    pub fn dry_run_once(&self, event: &Event) -> Result<DryRunReport, PipelineError> {
        dry_run_event(
            event,
            &self.config,
            &self.routing_rules,
            &self.mappings,
            &self.upcasters,
        )
        .map_err(PipelineError::Validation)
    }

    /// Replay a stream in sequence order through the built adapters.
    pub fn replay(
        &mut self,
        events: impl Iterator<Item = Result<Event, ReplayLoadError>>,
        opts: &ReplayRunOptions,
    ) -> Result<ReplayReport, ReplayLoadError> {
        self.ensure_adapters_built();
        let adapters = self.adapters.take().unwrap_or_default();
        let mut ctx = ReplayContext::from_parts(
            adapters,
            self.config.failure_policy,
            self.routing_rules.clone(),
            self.adapter_meta.clone(),
        );
        let result = ctx.run_stream_with_options(events, opts);
        self.adapters = Some(ctx.into_adapters());
        result
    }

    /// [`Self::run_sources`] that asks `reloader` between batches whether to
    /// switch to a new configuration (see [`run_sources_with_reload`]).
    pub fn run_sources_with_reload(
        &mut self,
        sources: Vec<Box<dyn EventSource>>,
        opts: &SourceRunOptions,
        stop: &AtomicBool,
        reloader: &mut dyn Reloader,
    ) -> Result<SourceRunReport, SourceError> {
        self.ensure_adapters_built();
        let adapters = self.adapters.take().unwrap_or_default();
        run_sources_with_reload(
            &self.config,
            self.routing_rules.clone(),
            adapters,
            sources,
            opts,
            stop,
            Some(reloader),
        )
    }

    /// Build the adapters and hand over everything a running loop needs to
    /// switch to this runtime's configuration.
    pub(crate) fn into_reloaded(mut self) -> Result<Reloaded, PipelineError> {
        self.build_adapters()?;
        self.prepare_adapters()?;
        Ok(Reloaded {
            adapters: self.adapters.take().unwrap_or_default(),
            routing_rules: self.routing_rules,
            config: self.config,
        })
    }

    /// Run the poll → sort → dispatch → commit loop until caught up, stopped,
    /// or halted. Consumes the built adapters, so a later call on this runtime
    /// builds a fresh set.
    pub fn run_sources(
        &mut self,
        sources: Vec<Box<dyn EventSource>>,
        opts: &SourceRunOptions,
        stop: &AtomicBool,
    ) -> Result<SourceRunReport, SourceError> {
        self.ensure_adapters_built();
        let adapters = self.adapters.take().unwrap_or_default();
        run_sources_with_adapters(
            &self.config,
            self.routing_rules.clone(),
            adapters,
            sources,
            opts,
            stop,
        )
    }
}
