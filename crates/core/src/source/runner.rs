//! Phase 17.3 / 17.5: the continuous run loop —
//! `poll → dispatch batch → commit`, with across-batch retry and DLQ.
//!
//! The guarantee: a source position is committed **only** on a batch whose
//! every event was projected, skipped by a guard, or DLQ'd. A crash before
//! `commit` re-polls the same batch, which is safe because every sink is
//! idempotent (Phases 11–16) — at-least-once redelivery + sequence-gated
//! sinks ⇒ effectively-once projection.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use serde::Serialize;

use super::{EventSource, SourceError, SourcePosition, SourcedEvent};
use crate::adapter::document::mapping::DocumentMapping;
use crate::adapter::graph::mapping::GraphMapping;
use crate::adapter::keyvalue::mapping::KvMapping;
use crate::adapter::sql::mapping::SqlMapping;
use crate::adapter::{StorageAdapter, prepare_adapters};
use crate::batch::{BatchResult, dispatch_batch};
use crate::dispatch::dispatch;
use crate::event::Event;
use crate::execution::stages::{self, Stage, StageTimings, timed};
use crate::execution::{AdapterOutcome, ExecutionReport, ExecutionStatus};
use crate::routing::AdapterId;
use crate::runtime::build_adapters_from_config;
use crate::runtime::config::KulyraxConfig;
use crate::runtime::dependency_graph::adapter_metadata_map;
use crate::upcast::UpcasterRegistry;
use std::collections::HashMap;
use std::sync::Arc;

/// Default events per batch for continuous ingest (`run`, `ingest`). Catch-up
/// (`run --once`, `replay`) uses [`crate::replay::DEFAULT_REPLAY_BATCH`].
pub const DEFAULT_LIVE_BATCH: usize = 5000;

/// Default [`SourceRunOptions::max_batch_delay`].
pub const DEFAULT_BATCH_DELAY: Duration = Duration::from_millis(100);

/// Whether to drain every source once and exit, or keep polling.
#[derive(Debug, Clone, Copy)]
#[non_exhaustive]
pub enum RunMode {
    /// Drain every source to its current end, commit, return. Subsumes `replay`.
    Once,
    /// Keep polling; sleep `poll_interval` when all sources are caught up.
    Continuous { poll_interval: Duration },
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct SourceRunOptions {
    pub mode: RunMode,
    /// Max events per source per poll: one batch, committed in one
    /// transaction per adapter (see `crate::batch`).
    pub max_batch: usize,
    /// Continuous mode: how long a batch may wait to fill. A batch closes
    /// when `max_batch` events are waiting, or when this much time has passed
    /// since the previous poll came back short, whichever comes first: after
    /// a short poll the loop waits out the rest of the delay before polling
    /// again, so the next batch collects what arrived meanwhile. A full poll
    /// (a backlog) never waits, and neither does `RunMode::Once`. Zero
    /// disables it.
    pub max_batch_delay: Duration,
    /// Across-batch retries before a batch is DLQ'd (if `dlq_dir` set) or the loop halts.
    pub retry_budget: u32,
    /// Where poison events are parked. `None` → a persistently-failing batch halts the loop.
    pub dlq_dir: Option<PathBuf>,
}

impl Default for SourceRunOptions {
    fn default() -> Self {
        Self {
            mode: RunMode::Once,
            max_batch: DEFAULT_LIVE_BATCH,
            max_batch_delay: DEFAULT_BATCH_DELAY,
            retry_budget: 3,
            dlq_dir: None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
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
#[non_exhaustive]
pub struct BatchSummary {
    pub source_id: String,
    /// The position of the batch's first event, in delivery order.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub position_from: Option<String>,
    /// The position of the batch's last event, in delivery order: the one the
    /// batch is committed at.
    pub position_to: String,
    pub events: u64,
    pub created: u64,
    pub updated: u64,
    pub deleted: u64,
    pub skipped: u64,
    pub failed: u64,
    pub dlq: u64,
    pub retries: u32,
    /// The batch transaction rolled back (a failing event, or an adapter that
    /// could not begin or commit its batch) and the batch was dispatched one
    /// event at a time.
    pub fell_back: bool,
    pub committed: bool,
}

#[derive(Debug, Clone, Serialize)]
#[non_exhaustive]
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
    /// Wall-clock time per stage of the loop, summed over the whole run.
    pub timings: StageTimings,
}

/// One pass of dispatching a batch.
struct BatchOutcome {
    created: u64,
    updated: u64,
    deleted: u64,
    skipped: u64,
    failed: u64,
    /// Indices into the batch whose dispatch status was `Failed`.
    hard_failures: Vec<usize>,
    /// The batch transaction was rolled back and the batch dispatched one
    /// event at a time instead.
    fell_back: bool,
}

/// Dispatch a batch, in delivery order: in one transaction per adapter when `batched`
/// (see `crate::batch`), falling back to one event at a time if that rolls
/// back; one event at a time otherwise.
#[allow(clippy::too_many_arguments)]
fn dispatch_source_batch(
    batch: &[SourcedEvent],
    batched: bool,
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
        fell_back: false,
    };
    reports.clear();
    if batched {
        let events: Vec<&Event> = batch.iter().map(|se| &se.event).collect();
        match dispatch_batch(
            &events,
            adapters,
            failure_policy,
            routing_rules,
            adapter_meta,
        ) {
            BatchResult::Committed(committed) => *reports = committed,
            BatchResult::RolledBack(_) => o.fell_back = true,
        }
    }
    if !batched || o.fell_back {
        for se in batch {
            reports.push(dispatch(
                &se.event,
                adapters,
                failure_policy,
                routing_rules,
                adapter_meta,
            ));
        }
    }
    for (idx, (report, se)) in reports.iter_mut().zip(batch).enumerate() {
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
    }
    o
}

fn dlq_write(
    dlq_dir: &std::path::Path,
    se: &SourcedEvent,
    report: &ExecutionReport,
) -> Result<(), SourceError> {
    std::fs::create_dir_all(dlq_dir)?;
    let body = serde_json::json!({ "event": se.event, "report": report });
    let json =
        serde_json::to_string_pretty(&body).map_err(|e| SourceError::Parse(e.to_string()))?;
    let base = dlq_base_name(&se.event.event_id, &se.position.0);

    // Claim a name no other DLQ file has (`create_new` is atomic), so a DLQ
    // file is never overwritten: two events that would share a name, or one
    // event dead-lettered twice, get `~2`, `~3`, … The claimed file stays
    // empty until the content replaces it below; a crash in between leaves
    // an empty file, and the event is read again (its batch never committed).
    let mut n = 1u32;
    let path = loop {
        let name = if n == 1 {
            format!("{base}.json")
        } else {
            format!("{base}~{n}.json")
        };
        let candidate = dlq_dir.join(name);
        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(_) => break candidate,
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => n += 1,
            Err(e) => return Err(e.into()),
        }
    };

    // Durable before the batch commits, as checkpoints are: write a temp
    // file, fsync it, rename it over the claimed name, fsync the directory.
    // Otherwise the checkpoint could survive a power loss that the DLQ file
    // did not, and the event would be skipped without a trace.
    let tmp = dlq_dir.join(format!(".dlq.tmp-{}", std::process::id()));
    let mut f = std::fs::File::create(&tmp)?;
    std::io::Write::write_all(&mut f, json.as_bytes())?;
    f.sync_all()?;
    drop(f);
    crate::crash::crash_point("dlq-before-rename");
    std::fs::rename(&tmp, &path)?;
    crate::source::checkpoint::sync_dir(dlq_dir)?;
    crate::crash::crash_point("dlq-written");
    Ok(())
}

/// The DLQ file name for an event, before the `.json` and any `~n` suffix:
/// the injective file-name stems of the event id and of the position, joined
/// by `~`, which no stem contains, so the pair is injective too. The position
/// can come from a producer (gRPC `EventEnvelope.position`), so it is encoded
/// like any value bound for a file name and cannot leave `dlq_dir`. When the
/// pair would make too long a name, it is hashed (the file holds the event).
fn dlq_base_name(event_id: &str, position: &str) -> String {
    use crate::adapter::file_names;
    let id = file_names::stem(event_id);
    let pos = file_names::stem(position);
    if id.len() + 1 + pos.len() <= file_names::MAX_STEM {
        format!("{id}~{pos}")
    } else {
        file_names::hashed(&format!("{event_id}\0{position}"))
    }
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

#[doc(hidden)]
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

#[doc(hidden)]
/// A new adapter set, routing table and config, ready to swap in.
pub struct Reloaded {
    pub config: KulyraxConfig,
    pub routing_rules: HashMap<String, Vec<AdapterId>>,
    pub adapters: Vec<Box<dyn StorageAdapter>>,
}

#[doc(hidden)]
/// Asked between batches whether to switch to a new configuration.
///
/// Return `Some` only for a configuration that is already validated and
/// whose adapters are built and prepared (`StorageAdapter::prepare`): the
/// loop swaps it in without further checks.
/// A rejected reload returns `None` and the loop carries on unchanged.
pub trait Reloader {
    fn poll(&mut self) -> Option<Reloaded>;
}

#[doc(hidden)]
/// [`run_sources_with_adapters`] that also asks `reloader` for a new
/// configuration at the top of every loop iteration. That point is only
/// reached once every batch polled so far has been committed or the loop has
/// halted, so no batch ever runs on a mix of old and new adapters. Sources are
/// not replaced: their positions carry over.
pub fn run_sources_with_reload(
    config: &KulyraxConfig,
    routing_rules: HashMap<String, Vec<AdapterId>>,
    adapters: Vec<Box<dyn StorageAdapter>>,
    sources: Vec<Box<dyn EventSource>>,
    opts: &SourceRunOptions,
    stop: &AtomicBool,
    reloader: Option<&mut dyn Reloader>,
) -> Result<SourceRunReport, SourceError> {
    // Drop anything an earlier run on this thread left behind.
    stages::take();
    // Before the first poll, so a store that can't be set up fails the run
    // without ingesting anything. A reloaded adapter set arrives prepared.
    let failures = prepare_adapters(&adapters);
    if !failures.is_empty() {
        return Err(SourceError::Prepare(failures));
    }
    let mut report = run_loop(
        config,
        routing_rules,
        adapters,
        sources,
        opts,
        stop,
        reloader,
    )?;
    report.timings = stages::take();
    Ok(report)
}

fn run_loop(
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
        timings: StageTimings::default(),
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
        // For `max_batch_delay`: when this iteration's polls started, and
        // whether any came back full (a backlog: don't wait).
        let polled_at = Instant::now();
        let mut any_full = false;

        for source in sources.iter_mut() {
            let batch = timed(Stage::SourceRead, || source.poll(opts.max_batch))?;
            if batch.is_empty() {
                continue;
            }
            progressed = true;
            any_full |= batch.len() >= opts.max_batch;

            // Dispatched in delivery order, never re-batch: `sequence` is per
            // entity (producer contract §1), so sorting a batch by it reorders
            // different entities against each other (an edge before its nodes,
            // a child row before its parent) and makes the outcome depend on
            // where batches happen to split. `decide()` resolves each entity's
            // own order through its guard lane (decision 2026-11-25-batch-order).
            //
            // The batch is committed at its last event in delivery order.
            // Positions are opaque: only the source that made them may order
            // them, so the core never compares them (decision
            // 2026-11-25-source-position-contract).
            let position_to = batch
                .last()
                .map(|se| se.position.clone())
                .unwrap_or_else(|| SourcePosition(String::new()));
            let position_from = batch.first().map(|se| se.position.0.clone());
            let source_id = source.id().to_string();

            let mut retries = 0u32;
            let mut fell_back = false;
            let (o, committed, dlq_count) = loop {
                // A batch that failed once is retried one event at a time:
                // the batch transaction would only roll back again.
                let o = timed(Stage::Dispatch, || {
                    dispatch_source_batch(
                        &batch,
                        retries == 0,
                        &mut adapters,
                        failure_policy,
                        &routing_rules,
                        &adapter_meta,
                        &source_id,
                        &mut reports_buf,
                    )
                });
                fell_back |= o.fell_back;
                if o.hard_failures.is_empty() {
                    break (o, true, 0);
                }
                if retries >= opts.retry_budget {
                    match &opts.dlq_dir {
                        Some(dir) => {
                            for &idx in &o.hard_failures {
                                dlq_write(dir, &batch[idx], &reports_buf[idx])?;
                            }
                            let n = o.hard_failures.len() as u64;
                            break (o, true, n);
                        }
                        None => break (o, false, 0),
                    }
                }
                retries += 1;
            };

            report.events_processed += batch.len() as u64;
            report.events_failed += o.hard_failures.len() as u64;
            report.events_succeeded += batch.len() as u64 - o.hard_failures.len() as u64;
            report.events_dlq += dlq_count;

            report.batches.push(BatchSummary {
                source_id: source_id.clone(),
                position_from,
                position_to: position_to.0.clone(),
                events: batch.len() as u64,
                created: o.created,
                updated: o.updated,
                deleted: o.deleted,
                skipped: o.skipped,
                failed: o.failed,
                dlq: dlq_count,
                retries,
                fell_back,
                committed,
            });

            if committed {
                for (se, event_report) in batch.iter().zip(reports_buf.iter()) {
                    source.on_resolved(se, event_report);
                }
                timed(Stage::Checkpoint, || {
                    source.commit_with_dlq_count(position_to.clone(), dlq_count as u32)
                })?;
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
                } else if !any_full {
                    // Every poll came back short: let the next batch collect
                    // what arrives during the rest of the delay.
                    let waited = polled_at.elapsed();
                    if waited < opts.max_batch_delay {
                        sleep_interruptible(opts.max_batch_delay - waited, stop);
                    }
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
