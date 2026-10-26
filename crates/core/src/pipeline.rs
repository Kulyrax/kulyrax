//! High-level command orchestration for CLI-style front ends.
//!
//! Bundles config/mapping/routing loading, validation, and execution behind a
//! small set of functions so front ends (e.g. `kulyrax-cli`) stay limited to
//! argument parsing and result rendering — no raw file I/O or adapter setup.

use core::fmt;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use crate::adapter::document::loader::load_document_mappings;
use crate::adapter::document::mapping::DocumentMapping;
use crate::adapter::graph::loader::load_graph_mappings;
use crate::adapter::graph::mapping::GraphMapping;
use crate::adapter::keyvalue::loader::load_keyvalue_mappings;
use crate::adapter::keyvalue::mapping::KvMapping;
use crate::adapter::sql::loader::load_sql_mappings;
use crate::adapter::sql::mapping::SqlMapping;
use crate::event::Event;
use crate::execution::ExecutionReport;
use crate::replay::{ReplayLoadError, ReplayReport, ReplayRunOptions, events_from_path};
use crate::routing::{AdapterId, load_routing, route_with_rules};
use crate::runtime::config::{ConfigError, KulyraxConfig, SourceConfig};
use crate::runtime::dry_run::DryRunReport;
use crate::runtime::engine::KulyraxRuntime;
use crate::runtime::factory::AdapterBuildFailure;
use crate::runtime::reload::{FileReloader, ReloadOutcome};
use crate::runtime::{
    ValidationReport, adapter_metadata_map, dependency_depth_exceeds_recommended,
    dependency_layers_grouped, dependency_layers_parallel, validate_projection_config,
    validate_routing_and_dependencies_for_event_type,
};
pub use crate::scaffold::{InitError, init_project};
use crate::source::directory::DirectorySource;
use crate::source::runner::{RunMode, SourceRunOptions, SourceRunReport};
use crate::source::stdin::StdinSource;
use crate::source::{EventSource, SourceError};
use std::sync::atomic::AtomicBool;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum PipelineError {
    Io(std::io::Error),
    ConfigParse(String),
    Config(ConfigError),
    Mapping(String),
    Routing(String),
    Validation(ValidationReport),
    EventParse(String),
    Replay(ReplayLoadError),
    Source(SourceError),
    AdapterBuild(Vec<AdapterBuildFailure>),
    /// One or more built adapters failed `StorageAdapter::prepare`.
    AdapterPrepare(Vec<crate::adapter::PrepareFailure>),
}

impl fmt::Display for PipelineError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PipelineError::Io(e) => write!(f, "{}", e),
            PipelineError::ConfigParse(msg) => write!(f, "failed to parse config: {}", msg),
            PipelineError::Config(e) => write!(f, "{}", e),
            PipelineError::Mapping(msg) => write!(f, "{}", msg),
            PipelineError::Routing(msg) => write!(f, "{}", msg),
            PipelineError::Validation(report) => write!(f, "{}", report),
            PipelineError::EventParse(msg) => write!(f, "failed to parse event: {}", msg),
            PipelineError::Replay(e) => write!(f, "{}", e),
            PipelineError::Source(e) => write!(f, "{}", e),
            PipelineError::AdapterBuild(failures) => {
                write!(f, "{} adapter(s) could not be built:", failures.len())?;
                for failure in failures {
                    write!(f, "\n  - {:?}: {}", failure.adapter_id, failure.reason)?;
                }
                Ok(())
            }
            PipelineError::AdapterPrepare(failures) => {
                write!(f, "{} adapter(s) could not be prepared:", failures.len())?;
                for failure in failures {
                    write!(f, "\n  - {failure}")?;
                }
                Ok(())
            }
        }
    }
}

impl std::error::Error for PipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            PipelineError::Io(e) => Some(e),
            PipelineError::Config(e) => Some(e),
            PipelineError::Validation(e) => Some(e),
            PipelineError::Replay(e) => Some(e),
            PipelineError::Source(e) => Some(e),
            _ => None,
        }
    }
}

impl From<SourceError> for PipelineError {
    fn from(e: SourceError) -> Self {
        PipelineError::Source(e)
    }
}

impl From<std::io::Error> for PipelineError {
    fn from(e: std::io::Error) -> Self {
        PipelineError::Io(e)
    }
}

impl From<ConfigError> for PipelineError {
    fn from(e: ConfigError) -> Self {
        PipelineError::Config(e)
    }
}

impl From<ValidationReport> for PipelineError {
    fn from(e: ValidationReport) -> Self {
        PipelineError::Validation(e)
    }
}

impl From<ReplayLoadError> for PipelineError {
    fn from(e: ReplayLoadError) -> Self {
        PipelineError::Replay(e)
    }
}

// ---------------------------------------------------------------------------
// Loading
// ---------------------------------------------------------------------------

/// Resolve the routing file path relative to the directory containing `config_path`.
pub fn resolve_routing_path(config_path: &Path, routing_file: &str) -> PathBuf {
    config_path
        .parent()
        .unwrap_or(Path::new("."))
        .join(routing_file)
}

/// Load and validate a [KulyraxConfig] from a YAML file.
pub fn load_config(path: &Path) -> Result<KulyraxConfig, PipelineError> {
    let file = fs::File::open(path)?;
    let config: KulyraxConfig =
        serde_yaml::from_reader(file).map_err(|e| PipelineError::ConfigParse(e.to_string()))?;
    config.validate()?;
    Ok(config)
}

/// SQL, document, key-value, and graph mapping tables keyed by event type.
pub type LoadedMappings = (
    HashMap<String, SqlMapping>,
    HashMap<String, DocumentMapping>,
    HashMap<String, KvMapping>,
    HashMap<String, GraphMapping>,
);

/// Load SQL, document, key-value, and graph mappings from `<mappings_dir>/sql`,
/// `<mappings_dir>/document`, `<mappings_dir>/keyvalue`, and
/// `<mappings_dir>/graph`.
///
/// `sql/` and `document/` are mandatory (Phase 3/11 convention — always
/// present, empty is fine). `keyvalue/` (Phase 14) and `graph/` (Phase 16) are
/// optional: a project with none of that kind need not create the directory,
/// so adopting a later phase doesn't force a change on every existing project.
pub fn load_mappings(mappings_dir: &Path) -> Result<LoadedMappings, PipelineError> {
    let sql = load_sql_mappings(mappings_dir.join("sql")).map_err(PipelineError::Mapping)?;
    let doc =
        load_document_mappings(mappings_dir.join("document")).map_err(PipelineError::Mapping)?;
    let kv_dir = mappings_dir.join("keyvalue");
    let kv = if kv_dir.is_dir() {
        load_keyvalue_mappings(&kv_dir).map_err(PipelineError::Mapping)?
    } else {
        HashMap::new()
    };
    let graph_dir = mappings_dir.join("graph");
    let graph = if graph_dir.is_dir() {
        load_graph_mappings(&graph_dir).map_err(PipelineError::Mapping)?
    } else {
        HashMap::new()
    };
    Ok((sql, doc, kv, graph))
}

/// Load a single [Event] from a JSON file.
pub fn load_event(path: &Path) -> Result<Event, PipelineError> {
    let raw = fs::read_to_string(path)?;
    serde_json::from_str(&raw).map_err(|e| PipelineError::EventParse(e.to_string()))
}

/// A fully loaded and cross-validated project: config, mappings, and routing rules.
pub struct LoadedProject {
    pub config: KulyraxConfig,
    pub sql_mappings: HashMap<String, SqlMapping>,
    pub doc_mappings: HashMap<String, DocumentMapping>,
    pub kv_mappings: HashMap<String, KvMapping>,
    pub graph_mappings: HashMap<String, GraphMapping>,
    pub routing_rules: HashMap<String, Vec<AdapterId>>,
}

/// Load config, mappings, and routing, then run Phase 8 cross-validation.
pub fn load_project(
    config_path: &Path,
    mappings_dir: &Path,
) -> Result<LoadedProject, PipelineError> {
    let config = load_config(config_path)?;
    let (sql_mappings, doc_mappings, kv_mappings, graph_mappings) = load_mappings(mappings_dir)?;
    let routing_path = resolve_routing_path(config_path, &config.routing.file);
    let routing_rules = load_routing(&routing_path).map_err(PipelineError::Routing)?;
    validate_projection_config(
        &config,
        &routing_rules,
        &sql_mappings,
        &doc_mappings,
        &kv_mappings,
        &graph_mappings,
    )?;
    Ok(LoadedProject {
        config,
        sql_mappings,
        doc_mappings,
        kv_mappings,
        graph_mappings,
        routing_rules,
    })
}

// ---------------------------------------------------------------------------
// Commands
// ---------------------------------------------------------------------------

/// Run a single event through the full pipeline (config, mappings, routing, execution).
///
/// `register_backends` runs on the runtime before adapters build — pass
/// `kulyrax_backends::register_default_backends` to get every split-out
/// networked backend (Postgres, ...), or `|_| {}` for none.
pub fn run(
    config_path: &Path,
    mappings_dir: &Path,
    event_path: &Path,
    register_backends: impl FnOnce(&mut KulyraxRuntime),
) -> Result<ExecutionReport, PipelineError> {
    let mut runtime =
        KulyraxRuntime::load_with_backends(config_path, mappings_dir, register_backends)?;
    runtime.build_adapters()?;
    runtime.prepare_adapters()?;
    let event = load_event(event_path)?;
    Ok(runtime.run_once(event))
}

/// Show what one event would write, without writing. Loads and validates the
/// project like [run], but never builds adapters, so no store is contacted.
pub fn dry_run(
    config_path: &Path,
    mappings_dir: &Path,
    event_path: &Path,
) -> Result<DryRunReport, PipelineError> {
    let runtime = KulyraxRuntime::load(config_path, mappings_dir)?;
    let event = load_event(event_path)?;
    runtime.dry_run_once(&event)
}

/// Result of [explain]: routing/dependency resolution for one event, without execution.
pub struct ExplainResult {
    pub event: Event,
    pub routed_adapters: Vec<AdapterId>,
    pub execution_order: Vec<AdapterId>,
    pub execution_layers: Vec<(u32, Vec<AdapterId>)>,
    pub depth_warning: bool,
}

/// Resolve routing and dependency order for one event without executing it.
/// When `mappings_dir` is `Some`, also runs full Phase 8 validation.
pub fn explain(
    config_path: &Path,
    event_path: &Path,
    mappings_dir: Option<&Path>,
) -> Result<ExplainResult, PipelineError> {
    let config = load_config(config_path)?;
    let event = load_event(event_path)?;

    let routing_path = resolve_routing_path(config_path, &config.routing.file);
    let rules = load_routing(&routing_path).map_err(PipelineError::Routing)?;
    if let Some(dir) = mappings_dir {
        let (sql, doc, kv, graph) = load_mappings(dir)?;
        validate_projection_config(&config, &rules, &sql, &doc, &kv, &graph)?;
    }

    let execution_order =
        validate_routing_and_dependencies_for_event_type(&config, &rules, &event.event_type)?;

    let adapter_ids = route_with_rules(&event, &rules);
    let routed_set: std::collections::HashSet<_> = adapter_ids.iter().cloned().collect();
    let adapter_meta = adapter_metadata_map(&config);
    let layer_vec = if execution_order.is_empty() {
        Vec::new()
    } else {
        dependency_layers_parallel(&execution_order, &adapter_meta, &routed_set)
    };
    let execution_layers = if execution_order.is_empty() {
        Vec::new()
    } else {
        dependency_layers_grouped(&execution_order, &layer_vec)
    };
    let depth_warning = dependency_depth_exceeds_recommended(&layer_vec);

    Ok(ExplainResult {
        event,
        routed_adapters: adapter_ids,
        execution_order,
        execution_layers,
        depth_warning,
    })
}

// ---------------------------------------------------------------------------
// Phase 17: source loop
// ---------------------------------------------------------------------------

/// Build the configured [`EventSource`]s, resolving `path` / `state_dir`
/// relative to the config file's directory (same rule as the routing file).
pub fn build_sources(
    config: &KulyraxConfig,
    config_path: &Path,
) -> Result<Vec<Box<dyn EventSource>>, PipelineError> {
    let base = config_path.parent().unwrap_or_else(|| Path::new("."));
    let mut out: Vec<Box<dyn EventSource>> = Vec::new();
    for sc in &config.sources {
        let source: Box<dyn EventSource> = match sc {
            SourceConfig::Directory(d) => Box::new(DirectorySource::new(
                d.id.clone(),
                base.join(&d.path),
                base.join(&d.state_dir),
            )?),
            SourceConfig::Stdin(s) => Box::new(StdinSource::new(
                s.id.clone(),
                base.join(&s.state_dir),
                std::io::BufReader::new(std::io::stdin()),
            )?),
        };
        out.push(source);
    }
    Ok(out)
}

/// Phase 17.4: configured sources and their committed positions, for
/// `kulyrax sources`.
pub fn list_sources(config_path: &Path) -> Result<Vec<(String, Option<String>)>, PipelineError> {
    let config = load_config(config_path)?;
    let sources = build_sources(&config, config_path)?;
    Ok(sources
        .iter()
        .map(|s| {
            (
                s.id().to_string(),
                s.committed_position().map(|p| p.as_str().to_string()),
            )
        })
        .collect())
}

/// Load the project, build its sources, and run the
/// `poll → sort → dispatch → commit` loop until caught up (`RunMode::Once`),
/// shut down (`stop`), or halted by a retry-exhausted batch.
///
/// In continuous mode the loop also answers `kulyrax reload` between batches
/// (see [`crate::runtime::reload`]); `on_reload` is told each outcome.
pub fn run_source_loop(
    config_path: &Path,
    mappings_dir: &Path,
    opts: &SourceRunOptions,
    stop: &AtomicBool,
    register_backends: impl Fn(&mut KulyraxRuntime) + Clone + 'static,
    on_reload: impl FnMut(&ReloadOutcome) + 'static,
) -> Result<SourceRunReport, PipelineError> {
    let mut runtime =
        KulyraxRuntime::load_with_backends(config_path, mappings_dir, register_backends.clone())?;
    runtime.build_adapters()?;
    runtime.prepare_adapters()?;
    let sources = build_sources(runtime.config(), config_path)?;
    if sources.is_empty() {
        return Err(PipelineError::Mapping(
            "no sources configured — add `sources:` to the config, or use `run --event <file>`"
                .to_string(),
        ));
    }
    if matches!(opts.mode, RunMode::Once) {
        return Ok(runtime.run_sources(sources, opts, stop)?);
    }
    let mut reloader = FileReloader::new(
        config_path,
        mappings_dir,
        runtime.config().sources.clone(),
        register_backends,
        runtime.upcasters(),
        on_reload,
    );
    Ok(runtime.run_sources_with_reload(sources, opts, stop, &mut reloader)?)
}

/// Ask the process running with `config_path` to reload; see
/// [`crate::runtime::reload`].
pub fn request_reload(
    config_path: &Path,
    timeout: std::time::Duration,
) -> Result<ReloadOutcome, PipelineError> {
    Ok(crate::runtime::reload::request_reload(
        config_path,
        timeout,
    )?)
}

/// Replay a directory/file of events through the full pipeline.
pub fn replay(
    config_path: &Path,
    mappings_dir: &Path,
    events_path: &Path,
    opts: &ReplayRunOptions,
    register_backends: impl FnOnce(&mut KulyraxRuntime),
) -> Result<ReplayReport, PipelineError> {
    let mut runtime =
        KulyraxRuntime::load_with_backends(config_path, mappings_dir, register_backends)?;
    runtime.build_adapters()?;
    runtime.prepare_adapters()?;
    let mut iter = events_from_path(events_path)?;
    Ok(runtime.replay(&mut iter, opts)?)
}
