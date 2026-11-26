//! Rebuild a SQL projection in place: project the full event history into
//! shadow tables, then swap them in with one transaction.
//!
//! Readers keep the current tables, and their content, until the swap; the
//! swap replaces each table and its guard lanes together, so a reader sees
//! either the old projection or the new one, never a mix. Use it after a
//! mapping change, or to repair a projection.
//!
//! **Other writers must be stopped** for the tables being rebuilt: a live
//! `run` or `ingest` would keep writing the old tables, and the swap would
//! drop those writes. The rebuild cannot hold them off, so it detects them
//! instead. It takes a [`GuardFingerprint`] of the tables' guard lanes when
//! it starts and checks it again, with the tables locked, before it swaps;
//! a writer changes it, and the swap is then refused and nothing changes.
//!
//! Supported where table DDL is transactional: SQLite and Postgres. MySQL
//! commits every `ALTER`/`RENAME` implicitly, so its swap could not be one
//! transaction with the guard lanes.
//!
//! Decision record: `docs/ledger/decisions/2026-11-24-shadow-rebuild.md`.

use std::collections::{BTreeSet, HashMap};
use std::path::Path;

use crate::adapter::AdapterError;
use crate::adapter::sql::adapter::SqlError;
use crate::adapter::sql::exec::{GuardFingerprint, ShadowTable};
use crate::pipeline::{PipelineError, load_project};
use crate::replay::{ReplayReport, ReplayRunOptions, events_from_path};
use crate::routing::AdapterId;
use crate::runtime::KulyraxRuntime;
use crate::runtime::config::AdapterConfig;

/// Appended to a table's name to name its shadow.
pub const SHADOW_SUFFIX: &str = "__kulyrax_rebuild";

/// A SQL adapter that can build tables in shadow and swap them in. Reached
/// through [`crate::adapter::StorageAdapter::shadow_rebuild`].
pub trait ShadowRebuild {
    /// Create every shadow table (empty, shaped like its table) and return
    /// the target tables' guard fingerprint. A table the swap would break is
    /// [`RebuildError::Refused`], before anything is created.
    fn start_rebuild(&self, tables: &[ShadowTable]) -> Result<GuardFingerprint, RebuildError>;

    /// Swap every shadow table in, in one transaction, unless the target
    /// tables' guard fingerprint is no longer `before`: that is
    /// [`RebuildError::Refused`], and nothing changes.
    fn finish_rebuild(
        &self,
        tables: &[ShadowTable],
        before: GuardFingerprint,
    ) -> Result<(), RebuildError>;
}

/// Why a [`rebuild`] did not swap new tables in. The kinds are what the CLI's
/// exit codes tell apart (`docs/stability.md` §3).
#[derive(Debug)]
#[non_exhaustive]
pub enum RebuildError {
    /// The request cannot be met: no such adapter, no SQL mapping routed to
    /// it, or an adapter that cannot rebuild. CLI exit code 2.
    Invalid(String),
    /// A check that protects the live tables refused: a table the swap would
    /// break, a mapped table named like a shadow, or another writer during the
    /// rebuild. Nothing was swapped. Exit code 4.
    Refused(String),
    /// Events failed while replaying into the shadow tables, which are left
    /// for inspection, not swapped in. Exit code 3.
    EventsFailed(String),
    /// Anything else (the database failed). Exit code 1.
    Failed(String),
}

impl std::fmt::Display for RebuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RebuildError::Invalid(m)
            | RebuildError::Refused(m)
            | RebuildError::EventsFailed(m)
            | RebuildError::Failed(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for RebuildError {}

/// A backend's refusal stays a refusal; any other SQL error is a failure.
impl From<SqlError> for RebuildError {
    fn from(e: SqlError) -> Self {
        match e {
            SqlError::RebuildRefused(m) => RebuildError::Refused(m),
            other => RebuildError::Failed(other.to_string()),
        }
    }
}

/// The adapter could not be reached (a poisoned lock, no connection).
impl From<AdapterError> for RebuildError {
    fn from(e: AdapterError) -> Self {
        RebuildError::Failed(e.to_string())
    }
}

/// What [`rebuild`] did.
#[derive(Debug)]
#[non_exhaustive]
pub struct RebuildReport {
    /// The tables rebuilt and swapped in.
    pub tables: Vec<String>,
    /// The replay of the history into the shadow tables.
    pub replay: ReplayReport,
}

/// Rebuild every SQL table `adapter_id` projects, from the events at
/// `events_path`, with the mappings in `mappings_dir`. See the module docs.
pub fn rebuild(
    config_path: &Path,
    mappings_dir: &Path,
    events_path: &Path,
    adapter_id: &str,
    opts: &ReplayRunOptions,
    register_backends: impl FnOnce(&mut KulyraxRuntime),
) -> Result<RebuildReport, PipelineError> {
    let invalid = |msg: String| PipelineError::Rebuild(RebuildError::Invalid(msg));
    let project = load_project(config_path, mappings_dir)?;

    // The rebuild runs this adapter alone.
    let mut config = project.config;
    let Some(position) = config.adapters.iter().position(|a| a.id() == adapter_id) else {
        return Err(invalid(format!("no adapter {adapter_id:?} in the config")));
    };
    let mut adapter = config.adapters.swap_remove(position);
    clear_depends_on(&mut adapter);
    config.adapters = vec![adapter];

    // Event types routed to the adapter, and their SQL mappings, pointed at
    // the shadow tables.
    let routing: HashMap<String, Vec<AdapterId>> = project
        .routing_rules
        .iter()
        .filter(|(_, ids)| ids.iter().any(|id| id == adapter_id))
        .map(|(event, _)| (event.clone(), vec![adapter_id.to_string()]))
        .collect();
    let mut tables = BTreeSet::new();
    let mut sql = HashMap::new();
    for (event, mapping) in &project.sql_mappings {
        if !routing.contains_key(event) {
            continue;
        }
        if mapping.table.ends_with(SHADOW_SUFFIX) {
            return Err(PipelineError::Rebuild(RebuildError::Refused(format!(
                "table {:?} ends with {SHADOW_SUFFIX:?}, which rebuilds reserve for shadow tables",
                mapping.table
            ))));
        }
        tables.insert(mapping.table.clone());
        let mut shadowed = mapping.clone();
        shadowed.table = format!("{}{SHADOW_SUFFIX}", mapping.table);
        sql.insert(event.clone(), shadowed);
    }
    if tables.is_empty() {
        return Err(invalid(format!(
            "adapter {adapter_id:?} has no SQL mappings routed to it; only SQL projections can \
             be rebuilt"
        )));
    }
    let tables: Vec<ShadowTable> = tables
        .into_iter()
        .map(|table| ShadowTable {
            shadow: format!("{table}{SHADOW_SUFFIX}"),
            table,
        })
        .collect();

    let mut runtime = KulyraxRuntime::from_parts(
        config,
        sql,
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        routing,
    );
    register_backends(&mut runtime);
    runtime.build_adapters()?;
    runtime.prepare_adapters()?;

    let before = {
        let rebuilder = shadow_rebuild(&runtime, adapter_id)?;
        rebuilder.start_rebuild(&tables).map_err(|e| {
            PipelineError::Rebuild(match e {
                RebuildError::Failed(m) => {
                    RebuildError::Failed(format!("could not create the shadow tables: {m}"))
                }
                other => other,
            })
        })?
    };

    let mut events = events_from_path(events_path)?;
    let replay = runtime.replay(&mut events, opts)?;
    if replay.events_failed > 0 || replay.stopped_early {
        return Err(PipelineError::Rebuild(RebuildError::EventsFailed(format!(
            "{} of {} events failed (first: {}); the rebuilt tables were not swapped in and are \
             left as {} for inspection",
            replay.events_failed,
            replay.events_processed,
            replay
                .first_failure_event_id
                .as_deref()
                .unwrap_or("unknown"),
            shadow_names(&tables),
        ))));
    }

    shadow_rebuild(&runtime, adapter_id)?
        .finish_rebuild(&tables, before)
        .map_err(PipelineError::Rebuild)?;
    Ok(RebuildReport {
        tables: tables.into_iter().map(|t| t.table).collect(),
        replay,
    })
}

fn shadow_rebuild<'a>(
    runtime: &'a KulyraxRuntime,
    adapter_id: &str,
) -> Result<&'a dyn ShadowRebuild, PipelineError> {
    runtime
        .built_adapters()
        .iter()
        .find(|a| a.id() == adapter_id)
        .and_then(|a| a.shadow_rebuild())
        .ok_or_else(|| {
            PipelineError::Rebuild(RebuildError::Invalid(format!(
                "adapter {adapter_id:?} cannot rebuild into shadow tables; SQLite and Postgres \
                 adapters can"
            )))
        })
}

fn shadow_names(tables: &[ShadowTable]) -> String {
    tables
        .iter()
        .map(|t| t.shadow.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn clear_depends_on(adapter: &mut AdapterConfig) {
    match adapter {
        AdapterConfig::Sqlite(cfg) => cfg.depends_on.clear(),
        AdapterConfig::File(cfg) => cfg.depends_on.clear(),
        AdapterConfig::KeyValue(cfg) => cfg.depends_on.clear(),
        AdapterConfig::Graph(cfg) => cfg.depends_on.clear(),
        AdapterConfig::Custom(cfg) => cfg.depends_on.clear(),
    }
}
