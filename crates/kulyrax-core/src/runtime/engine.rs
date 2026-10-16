//! Phase 27: `KulyraxRuntime` — a facade owning config, mappings, routing, an
//! instance-scoped [`AdapterRegistry`], and (once resolved) the built
//! adapters for one project, exposed as methods instead of a pile of free
//! functions that each re-thread the same bundle
//! (`execute_event*`/`ReplayContext::new`/`run_sources` previously did this
//! independently, recomputing dependency metadata and rebuilding adapters on
//! every call).
//!
//! Adapters are resolved **once** per `KulyraxRuntime` (lazily, on first use),
//! not once per event — `execute_event*` rebuilds its whole adapter set
//! (including reopening connections) on every single call; `replay`/
//! `run_sources` already built once and reused. This is a deliberate
//! behavior clarification, not a regression: a one-shot CLI subcommand
//! creates exactly one runtime per process invocation, so nothing
//! user-visible changes there — it only matters for an embedder holding one
//! `KulyraxRuntime` across many calls, where "build once" is the correct
//! semantic.

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
use crate::runtime::config::{ConfigError, KulyraxConfig};
use crate::runtime::dependency_graph::{AdapterExecutionMeta, adapter_metadata_map};
use crate::runtime::factory::build_adapters_from_config;
use crate::runtime::registry::AdapterRegistry;
use crate::source::runner::{SourceRunOptions, SourceRunReport, run_sources_with_adapters};
use crate::source::{EventSource, SourceError};
use crate::upcast::UpcasterRegistry;

/// Owns everything needed to run one project: config, mappings, routing, an
/// instance-scoped adapter registry, and — once resolved — the built
/// adapters. See the module docs for why adapters build once, lazily, rather
/// than once per event.
pub struct KulyraxRuntime {
    config: KulyraxConfig,
    sql_mappings: Option<HashMap<String, SqlMapping>>,
    doc_mappings: Option<HashMap<String, DocumentMapping>>,
    kv_mappings: Option<HashMap<String, KvMapping>>,
    graph_mappings: Option<HashMap<String, GraphMapping>>,
    routing_rules: HashMap<String, Vec<AdapterId>>,
    adapter_meta: HashMap<AdapterId, AdapterExecutionMeta>,
    registry: AdapterRegistry,
    upcasters: Arc<UpcasterRegistry>,
    adapters: Option<Vec<Box<dyn StorageAdapter>>>,
}

impl KulyraxRuntime {
    /// Load config, mappings, and routing for `config_path`/`mappings_dir`,
    /// running the same Phase 8 cross-validation as [`crate::pipeline::load_project`].
    /// Adapters are not built yet — register any custom factories first, then
    /// call `run_once`/`dry_run_once`/`replay`/`run_sources`, which build
    /// adapters on first use.
    pub fn load(config_path: &Path, mappings_dir: &Path) -> Result<Self, PipelineError> {
        let project = load_project(config_path, mappings_dir)?;
        Ok(Self {
            adapter_meta: adapter_metadata_map(&project.config),
            config: project.config,
            sql_mappings: Some(project.sql_mappings),
            doc_mappings: Some(project.doc_mappings),
            kv_mappings: Some(project.kv_mappings),
            graph_mappings: Some(project.graph_mappings),
            routing_rules: project.routing_rules,
            registry: AdapterRegistry::new(),
            upcasters: Arc::new(UpcasterRegistry::new()),
            adapters: None,
        })
    }

    /// Construct a runtime directly from already-loaded parts, skipping file
    /// I/O and [`crate::pipeline::load_project`]'s Phase 8 cross-validation —
    /// the caller is responsible for that (e.g. via
    /// [`crate::runtime::validate_projection_config`]) beforehand if wanted.
    /// Useful for an embedder building config/mappings/routing in memory
    /// rather than loading them from files.
    pub fn from_parts(
        config: KulyraxConfig,
        sql_mappings: HashMap<String, SqlMapping>,
        doc_mappings: HashMap<String, DocumentMapping>,
        kv_mappings: HashMap<String, KvMapping>,
        graph_mappings: HashMap<String, GraphMapping>,
        routing_rules: HashMap<String, Vec<AdapterId>>,
    ) -> Self {
        Self {
            adapter_meta: adapter_metadata_map(&config),
            config,
            sql_mappings: Some(sql_mappings),
            doc_mappings: Some(doc_mappings),
            kv_mappings: Some(kv_mappings),
            graph_mappings: Some(graph_mappings),
            routing_rules,
            registry: AdapterRegistry::new(),
            upcasters: Arc::new(UpcasterRegistry::new()),
            adapters: None,
        }
    }

    /// Same as [`Self::load`], but also runs `register_backends` on the
    /// runtime before returning it — the "load, then register" combo every
    /// caller wanting non-default adapter types needs (Phase 28's
    /// `kulyrax-backends` wiring crate is the typical `register_backends`).
    pub fn load_with_backends(
        config_path: &Path,
        mappings_dir: &Path,
        register_backends: impl FnOnce(&mut KulyraxRuntime),
    ) -> Result<Self, PipelineError> {
        let mut runtime = Self::load(config_path, mappings_dir)?;
        register_backends(&mut runtime);
        Ok(runtime)
    }

    /// The loaded, validated config.
    pub fn config(&self) -> &KulyraxConfig {
        &self.config
    }

    /// The loaded SQL mapping table, if adapters haven't been built yet
    /// (`ensure_adapters_built` consumes it) — read this (and the sibling
    /// `*_mappings`/`upcasters` accessors below) to hand a backend's factory
    /// what it needs *before* registering it, since the registry's own
    /// factory signature only carries the raw YAML value. `None` after the
    /// first `run_once`/`dry_run_once`/`replay`/`run_sources` call.
    pub fn sql_mappings(&self) -> Option<&HashMap<String, SqlMapping>> {
        self.sql_mappings.as_ref()
    }

    /// Same as [`Self::sql_mappings`], for the document-mapping table.
    pub fn document_mappings(&self) -> Option<&HashMap<String, DocumentMapping>> {
        self.doc_mappings.as_ref()
    }

    /// Same as [`Self::sql_mappings`], for the key-value mapping table.
    pub fn kv_mappings(&self) -> Option<&HashMap<String, KvMapping>> {
        self.kv_mappings.as_ref()
    }

    /// Same as [`Self::sql_mappings`], for the graph mapping table.
    pub fn graph_mappings(&self) -> Option<&HashMap<String, GraphMapping>> {
        self.graph_mappings.as_ref()
    }

    /// The upcaster registry this runtime builds adapters with — a cheap
    /// `Arc` clone, safe to call any time (unlike the mapping accessors, it
    /// doesn't get consumed when adapters build).
    pub fn upcasters(&self) -> Arc<UpcasterRegistry> {
        Arc::clone(&self.upcasters)
    }

    /// Register a factory for adapter `type: <type_name>`, so a `Custom`
    /// entry in the config resolves through it. Must be called before
    /// adapters are built (i.e. before the first `run_once`/`dry_run_once`/
    /// `replay`/`run_sources` call) — a `Custom` type is otherwise a
    /// [`crate::runtime::config::ConfigError::UnregisteredAdapterType`] placeholder,
    /// same as an unregistered type anywhere else.
    pub fn register_adapter_factory(
        &mut self,
        type_name: &str,
        factory: impl Fn(serde_yaml::Value) -> Result<Box<dyn StorageAdapter>, ConfigError>
        + Send
        + Sync
        + 'static,
    ) {
        self.registry.register(type_name, factory);
    }

    /// Use an explicit [`UpcasterRegistry`] for schema-evolving payloads
    /// (Phase 10.3), instead of the empty default. Must be called before
    /// adapters are built.
    pub fn with_upcasters(mut self, upcasters: Arc<UpcasterRegistry>) -> Self {
        self.upcasters = upcasters;
        self
    }

    /// Resolve every configured adapter (including any `Custom` types
    /// registered via [`Self::register_adapter_factory`]) if not already
    /// built. Returns `()`, not a borrow, so callers can follow it with
    /// direct field access to `self.adapters` alongside other fields
    /// (`self.routing_rules`, `self.adapter_meta`) without the borrow
    /// checker treating this call as holding `self` for longer than the call
    /// itself.
    fn ensure_adapters_built(&mut self) {
        if self.adapters.is_none() {
            let sql = self.sql_mappings.take().unwrap_or_default();
            let doc = self.doc_mappings.take().unwrap_or_default();
            let kv = self.kv_mappings.take().unwrap_or_default();
            let graph = self.graph_mappings.take().unwrap_or_default();
            let adapters = build_adapters_from_config(
                &self.config,
                sql,
                doc,
                kv,
                graph,
                Arc::clone(&self.upcasters),
                &self.registry,
            );
            self.adapters = Some(adapters);
        }
    }

    /// Run a single event through dispatch.
    pub fn run_once(&mut self, event: Event) -> ExecutionReport {
        self.ensure_adapters_built();
        let adapters = self.adapters.get_or_insert_with(Vec::new);
        dispatch(
            &event,
            adapters,
            self.config.failure_policy,
            &self.routing_rules,
            &self.adapter_meta,
        )
    }

    /// Same as [`Self::run_once`] — matches `execute_event_with_mode`'s
    /// existing `DryRun` behavior, which dispatches identically to `Run`
    /// today (true dry-run relies on adapter-level support).
    pub fn dry_run_once(&mut self, event: Event) -> ExecutionReport {
        self.run_once(event)
    }

    /// Replay a stream of events, sequence-ordered, through the built
    /// adapters (Phase 7/15.5).
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

    /// Run the `poll → sort → dispatch → commit` loop over `sources` until
    /// caught up, shut down, or halted (Phase 17). Consumes the built
    /// adapters — `run_sources_with_adapters` does not hand them back, so a
    /// subsequent call on this runtime rebuilds a fresh set.
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
