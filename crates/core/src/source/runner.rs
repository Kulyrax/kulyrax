//! Phase 17.3 / 17.5: the continuous run loop —
//! `poll → sort → dispatch batch → commit`, with across-batch retry and DLQ.
//!
//! The guarantee: a source position is committed **only** on a batch whose
//! every event was projected, skipped by a guard, or DLQ'd. A crash before
//! `commit` re-polls the same batch, which is safe because every sink is
//! idempotent (Phases 11–16) — at-least-once redelivery + sequence-gated
//! sinks ⇒ effectively-once projection.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use serde::Serialize;

use super::{EventSource, SourceError, SourcePosition, SourcedEvent};
use crate::adapter::StorageAdapter;
use crate::adapter::document::mapping::DocumentMapping;
use crate::adapter::graph::mapping::GraphMapping;
use crate::adapter::keyvalue::mapping::KvMapping;
use crate::adapter::sql::mapping::SqlMapping;
use crate::dispatch::dispatch;
use crate::execution::{AdapterOutcome, ExecutionReport, ExecutionStatus};
use crate::routing::AdapterId;
use crate::runtime::build_adapters_from_config;
use crate::runtime::config::KulyraxConfig;
use crate::runtime::dependency_graph::adapter_metadata_map;
use crate::upcast::UpcasterRegistry;
use std::collections::HashMap;
use std::sync::Arc;

/// Whether to drain every source once and exit, or keep polling.
#[derive(Debug, Clone, Copy)]
pub enum RunMode {
    /// Drain every source to its current end, commit, return. Subsumes `replay`.
    Once,
    /// Keep polling; sleep `poll_interval` when all sources are caught up.
    Continuous { poll_interval: Duration },
}

#[derive(Debug, Clone)]
pub struct SourceRunOptions {
    pub mode: RunMode,
    /// Max events per source per poll.
    pub max_batch: usize,
    /// Across-batch retries before a batch is DLQ'd (if `dlq_dir` set) or the loop halts.
    pub retry_budget: u32,
    /// Where poison events are parked. `None` → a persistently-failing batch halts the loop.
    pub dlq_dir: Option<PathBuf>,
}

impl Default for SourceRunOptions {
    fn default() -> Self {
        Self {
            mode: RunMode::Once,
            max_batch: 256,
            retry_budget: 3,
            dlq_dir: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StoppedReason {
    /// Every source returned an empty poll (`RunMode::Once`, or a graceful
    /// shutdown while caught up).
    CaughtUp,
    /// The stop flag was set (SIGINT/SIGTERM); the in-flight batch finished and committed.
    Shutdown,
    /// A batch kept failing past the retry budget and no DLQ is configured.
    RetryExhausted,
}

#[derive(Debug, Clone, Serialize)]
pub struct BatchSummary {
    pub source_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position_from: Option<String>,
    pub position_to: String,
    pub events: u64,
    pub created: u64,
    pub updated: u64,
    pub deleted: u64,
    pub skipped: u64,
    pub failed: u64,
    pub dlq: u64,
    pub retries: u32,
    pub committed: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceRunReport {
    pub batches: Vec<BatchSummary>,
    pub events_processed: u64,
    pub events_succeeded: u64,
    pub events_failed: u64,
    pub events_dlq: u64,
    pub batches_committed: u64,
    pub stopped_reason: StoppedReason,
    /// Set only when `stopped_reason` is `RetryExhausted`: the position of the
    /// batch that could not be committed, for operator intervention.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub halt_position: Option<String>,
}

/// One pass of dispatching a sorted batch.
struct BatchOutcome {
    created: u64,
    updated: u64,
    deleted: u64,
    skipped: u64,
    failed: u64,
    /// Indices into the sorted batch whose dispatch status was `Failed`.
    hard_failures: Vec<usize>,
}

#[allow(clippy::too_many_arguments)]
fn dispatch_batch(
    sorted: &[SourcedEvent],
    adapters: &mut [Box<dyn StorageAdapter>],
    failure_policy: crate::runtime::config::FailurePolicy,
    routing_rules: &HashMap<String, Vec<AdapterId>>,
    adapter_meta: &HashMap<AdapterId, crate::runtime::dependency_graph::AdapterExecutionMeta>,
    source_id: &str,
    reports: &mut Vec<ExecutionReport>,
) -> BatchOutcome {
    let mut o = BatchOutcome {
        created: 0,
        updated: 0,
        deleted: 0,
        skipped: 0,
        failed: 0,
        hard_failures: Vec::new(),
    };
    reports.clear();
    for (idx, se) in sorted.iter().enumerate() {
        let mut report = dispatch(
            &se.event,
            adapters,
            failure_policy,
            routing_rules,
            adapter_meta,
        );
        report.source_id = Some(source_id.to_string());
        report.source_position = Some(se.position.0.clone());
        for ar in &report.adapter_reports {
            match ar.outcome {
                AdapterOutcome::Created => o.created += 1,
                AdapterOutcome::Updated => o.updated += 1,
                AdapterOutcome::Deleted => o.deleted += 1,
                AdapterOutcome::Skipped { .. } => o.skipped += 1,
                AdapterOutcome::Failed => o.failed += 1,
            }
        }
        if report.status == ExecutionStatus::Failed {
            o.hard_failures.push(idx);
        }
        reports.push(report);
    }
    o
}

fn dlq_write(
    dlq_dir: &std::path::Path,
    se: &SourcedEvent,
    report: &ExecutionReport,
) -> Result<(), SourceError> {
    std::fs::create_dir_all(dlq_dir)?;
    let safe: String = se
        .event
        .event_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    let path = dlq_dir.join(format!("{safe}.{}.json", se.position.0));
    let body = serde_json::json!({ "event": se.event, "report": report });
    let json =
        serde_json::to_string_pretty(&body).map_err(|e| SourceError::Parse(e.to_string()))?;
    std::fs::write(path, json)?;
    Ok(())
}

/// Build adapters from config, then run the poll → sort → dispatch → commit
/// loop over every source until caught up (`RunMode::Once`), shut down
/// (`stop`), or halted by a retry-exhausted batch with no DLQ.
#[allow(clippy::too_many_arguments)]
pub fn run_sources(
    config: &KulyraxConfig,
    routing_rules: HashMap<String, Vec<AdapterId>>,
    sql_mappings: HashMap<String, SqlMapping>,
    document_mappings: HashMap<String, DocumentMapping>,
    kv_mappings: HashMap<String, KvMapping>,
    graph_mappings: HashMap<String, GraphMapping>,
    sources: Vec<Box<dyn EventSource>>,
    opts: &SourceRunOptions,
    stop: &AtomicBool,
) -> Result<SourceRunReport, SourceError> {
    let adapters = build_adapters_from_config(
        config,
        crate::runtime::MappingSet {
            sql: sql_mappings,
            document: document_mappings,
            kv: kv_mappings,
            graph: graph_mappings,
        },
        Arc::new(UpcasterRegistry::new()),
        &crate::runtime::AdapterRegistry::new(),
    );
    run_sources_with_adapters(config, routing_rules, adapters, sources, opts, stop)
}

/// Same as [`run_sources`] but with pre-built adapters — the injection point
/// for tests (a flaky adapter, a spy) and future callers that own their
/// adapter set.
pub fn run_sources_with_adapters(
    config: &KulyraxConfig,
    routing_rules: HashMap<String, Vec<AdapterId>>,
    adapters: Vec<Box<dyn StorageAdapter>>,
    sources: Vec<Box<dyn EventSource>>,
    opts: &SourceRunOptions,
    stop: &AtomicBool,
) -> Result<SourceRunReport, SourceError> {
    run_sources_with_reload(config, routing_rules, adapters, sources, opts, stop, None)
}

/// A new adapter set, routing table and config, ready to swap in.
pub struct Reloaded {
    pub config: KulyraxConfig,
    pub routing_rules: HashMap<String, Vec<AdapterId>>,
    pub adapters: Vec<Box<dyn StorageAdapter>>,
}

/// Asked between batches whether to switch to a new configuration.
///
/// Return `Some` only for a configuration that is already validated and
/// whose adapters are built: the loop swaps it in without further checks.
/// A rejected reload returns `None` and the loop carries on unchanged.
pub trait Reloader {
    fn poll(&mut self) -> Option<Reloaded>;
}

/// [`run_sources_with_adapters`] that also asks `reloader` for a new
/// configuration at the top of every loop iteration. That point is only
/// reached once every batch polled so far has been committed or the loop has
/// halted, so no batch ever runs on a mix of old and new adapters. Sources are
/// not replaced: their positions carry over.
pub fn run_sources_with_reload(
    config: &KulyraxConfig,
    mut routing_rules: HashMap<String, Vec<AdapterId>>,
    mut adapters: Vec<Box<dyn StorageAdapter>>,
    mut sources: Vec<Box<dyn EventSource>>,
    opts: &SourceRunOptions,
    stop: &AtomicBool,
    mut reloader: Option<&mut dyn Reloader>,
) -> Result<SourceRunReport, SourceError> {
    let mut adapter_meta = adapter_metadata_map(config);
    let mut failure_policy = config.failure_policy;

    let mut report = SourceRunReport {
        batches: Vec::new(),
        events_processed: 0,
        events_succeeded: 0,
        events_failed: 0,
        events_dlq: 0,
        batches_committed: 0,
        stopped_reason: StoppedReason::CaughtUp,
        halt_position: None,
    };
    let mut reports_buf: Vec<ExecutionReport> = Vec::new();

    loop {
        if stop.load(Ordering::Relaxed) {
            report.stopped_reason = StoppedReason::Shutdown;
            return Ok(report);
        }

        if let Some(reloaded) = reloader.as_deref_mut().and_then(|r| r.poll()) {
            adapter_meta = adapter_metadata_map(&reloaded.config);
            failure_policy = reloaded.config.failure_policy;
            routing_rules = reloaded.routing_rules;
            adapters = reloaded.adapters;
        }

        let mut progressed = false;

        for source in sources.iter_mut() {
            let batch = source.poll(opts.max_batch)?;
            if batch.is_empty() {
                continue;
            }
            progressed = true;

            // Phase 15.5 (as in replay): a stable sort by `sequence` delivers
            // every entity's events in the order `decide()` needs.
            let mut sorted = batch;
            sorted.sort_by_key(|se| se.event.sequence);
            let position_to = sorted
                .iter()
                .map(|se| se.position.clone())
                .max()
                .unwrap_or_else(|| SourcePosition(String::new()));
            let position_from = sorted.iter().map(|se| se.position.0.clone()).min();
            let source_id = source.id().to_string();

            let mut retries = 0u32;
            let (o, committed, dlq_count) = loop {
                let o = dispatch_batch(
                    &sorted,
                    &mut adapters,
                    failure_policy,
                    &routing_rules,
                    &adapter_meta,
                    &source_id,
                    &mut reports_buf,
                );
                if o.hard_failures.is_empty() {
                    break (o, true, 0);
                }
                if retries >= opts.retry_budget {
                    match &opts.dlq_dir {
                        Some(dir) => {
                            for &idx in &o.hard_failures {
                                dlq_write(dir, &sorted[idx], &reports_buf[idx])?;
                            }
                            let n = o.hard_failures.len() as u64;
                            break (o, true, n);
                        }
                        None => break (o, false, 0),
                    }
                }
                retries += 1;
            };

            report.events_processed += sorted.len() as u64;
            report.events_failed += o.hard_failures.len() as u64;
            report.events_succeeded += sorted.len() as u64 - o.hard_failures.len() as u64;
            report.events_dlq += dlq_count;

            report.batches.push(BatchSummary {
                source_id: source_id.clone(),
                position_from,
                position_to: position_to.0.clone(),
                events: sorted.len() as u64,
                created: o.created,
                updated: o.updated,
                deleted: o.deleted,
                skipped: o.skipped,
                failed: o.failed,
                dlq: dlq_count,
                retries,
                committed,
            });

            if committed {
                source.commit_with_dlq_count(position_to.clone(), dlq_count as u32)?;
                report.batches_committed += 1;
            } else {
                report.stopped_reason = StoppedReason::RetryExhausted;
                report.halt_position = Some(position_to.0);
                return Ok(report);
            }
        }

        match opts.mode {
            RunMode::Once => {
                if !progressed {
                    report.stopped_reason = StoppedReason::CaughtUp;
                    return Ok(report);
                }
            }
            RunMode::Continuous { poll_interval } => {
                if !progressed {
                    if stop.load(Ordering::Relaxed) {
                        report.stopped_reason = StoppedReason::Shutdown;
                        return Ok(report);
                    }
                    sleep_interruptible(poll_interval, stop);
                }
            }
        }
    }
}

/// Sleep up to `total`, waking every 100 ms to check the stop flag.
fn sleep_interruptible(total: Duration, stop: &AtomicBool) {
    let step = Duration::from_millis(100);
    let mut slept = Duration::ZERO;
    while slept < total {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let this = step.min(total - slept);
        std::thread::sleep(this);
        slept += this;
    }
}
