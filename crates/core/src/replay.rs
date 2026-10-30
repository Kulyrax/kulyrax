//! Phase 7: deterministic replay with adapters built once and streaming event input.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Cursor};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::adapter::document::mapping::DocumentMapping;
use crate::adapter::graph::mapping::GraphMapping;
use crate::adapter::keyvalue::mapping::KvMapping;
use crate::adapter::sql::mapping::SqlMapping;
use crate::batch::{BatchResult, dispatch_batch};
use crate::dispatch::dispatch;
use crate::event::Event;
use crate::execution::{
    AdapterExecutionReport, AdapterReportError, ExecutionReport, ExecutionStatus,
};
use crate::routing::{AdapterId, StorageKind, route_with_rules};
use crate::runtime::build_adapters_from_config;
use crate::runtime::config::{FailurePolicy, KulyraxConfig};
use crate::runtime::dependency_graph::{AdapterExecutionMeta, adapter_metadata_map};

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub enum ReplayLoadError {
    Io(std::io::Error),
    Json(String),
}

impl std::fmt::Display for ReplayLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ReplayLoadError::Io(e) => write!(f, "{}", e),
            ReplayLoadError::Json(s) => write!(f, "{}", s),
        }
    }
}

impl std::error::Error for ReplayLoadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            ReplayLoadError::Io(e) => Some(e),
            _ => None,
        }
    }
}

impl From<std::io::Error> for ReplayLoadError {
    fn from(e: std::io::Error) -> Self {
        ReplayLoadError::Io(e)
    }
}

// ---------------------------------------------------------------------------
// Streaming loader: Iterator<Item = Result<Event, ReplayLoadError>>
// ---------------------------------------------------------------------------

/// Lazily yields events from a directory (sorted `*.json`) or a file (single JSON or NDJSON).
///
/// Directory entries are parsed with [`serde_json::from_reader`] (no full-file string allocation).
/// Event files are read once; NDJSON reuses the same bytes via an in-memory cursor (no second open).
pub fn events_from_path(path: impl AsRef<Path>) -> Result<EventsFromPath, ReplayLoadError> {
    let path = path.as_ref();
    if path.is_dir() {
        let mut paths: Vec<PathBuf> = fs::read_dir(path)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| {
                p.extension()
                    .and_then(|s| s.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("json"))
                    .unwrap_or(false)
            })
            .collect();
        paths.sort();
        Ok(EventsFromPath::Directory { paths, index: 0 })
    } else {
        let raw = fs::read_to_string(path)?;
        let trimmed = raw.trim();
        if trimmed.is_empty() {
            return Ok(EventsFromPath::Empty);
        }
        if let Ok(event) = serde_json::from_str::<Event>(trimmed) {
            Ok(EventsFromPath::Single {
                done: false,
                event: Some(event),
            })
        } else {
            Ok(EventsFromPath::Ndjson {
                reader: BufReader::new(Cursor::new(raw.into_bytes())),
                line: String::new(),
            })
        }
    }
}

pub enum EventsFromPath {
    Directory {
        paths: Vec<PathBuf>,
        index: usize,
    },
    Ndjson {
        reader: BufReader<Cursor<Vec<u8>>>,
        line: String,
    },
    Single {
        done: bool,
        event: Option<Event>,
    },
    Empty,
}

impl Iterator for EventsFromPath {
    type Item = Result<Event, ReplayLoadError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            EventsFromPath::Empty => None,
            EventsFromPath::Single { done, event } => {
                if *done {
                    return None;
                }
                *done = true;
                event.take().map(Ok)
            }
            EventsFromPath::Directory { paths, index } => {
                if *index >= paths.len() {
                    return None;
                }
                let p = paths[*index].clone();
                *index += 1;
                match File::open(&p) {
                    Ok(f) => match serde_json::from_reader(BufReader::new(f)) {
                        Ok(e) => Some(Ok(e)),
                        Err(e) => Some(Err(ReplayLoadError::Json(format!(
                            "{}: {}",
                            p.display(),
                            e
                        )))),
                    },
                    Err(e) => Some(Err(e.into())),
                }
            }
            EventsFromPath::Ndjson { reader, line } => loop {
                line.clear();
                match reader.read_line(line) {
                    Ok(0) => return None,
                    Ok(_) => {
                        let t = line.trim();
                        if t.is_empty() {
                            continue;
                        }
                        return match serde_json::from_str::<Event>(t) {
                            Ok(e) => Some(Ok(e)),
                            Err(e) => Some(Err(ReplayLoadError::Json(e.to_string()))),
                        };
                    }
                    Err(e) => return Some(Err(e.into())),
                }
            },
        }
    }
}

// ---------------------------------------------------------------------------
// ReplayReport
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PerEventReplaySummary {
    pub event_id: String,
    pub event_type: String,
    pub status: ExecutionStatus,
}

fn default_replay_report_version() -> String {
    "1.0".to_string()
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplayReport {
    #[serde(default = "default_replay_report_version")]
    pub report_version: String,

    pub events_processed: u64,
    pub events_succeeded: u64,
    pub events_failed: u64,

    pub stopped_early: bool,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_failure_event_id: Option<String>,

    #[serde(with = "crate::execution::time")]
    pub started_at: SystemTime,

    #[serde(with = "crate::execution::time_option")]
    pub finished_at: Option<SystemTime>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,

    pub per_event: Vec<PerEventReplaySummary>,

    /// Successful events not listed in `per_event` because of [`ReplayRunOptions::max_per_event_summaries`].
    #[serde(default)]
    pub per_event_summaries_omitted: u64,
}

impl ReplayReport {
    fn new(started_at: SystemTime) -> Self {
        Self {
            report_version: default_replay_report_version(),
            events_processed: 0,
            events_succeeded: 0,
            events_failed: 0,
            stopped_early: false,
            first_failure_event_id: None,
            started_at,
            finished_at: None,
            duration_ms: None,
            per_event: Vec::new(),
            per_event_summaries_omitted: 0,
        }
    }

    fn finish(mut self, finished_at: SystemTime) -> Self {
        self.finished_at = Some(finished_at);
        self.duration_ms = finished_at
            .duration_since(self.started_at)
            .ok()
            .map(|d| d.as_millis() as u64);
        self
    }
}

// ---------------------------------------------------------------------------
// ReplayRunOptions
// ---------------------------------------------------------------------------

/// Optional replay behavior: cap per-event rows, progress, strict routing.
#[derive(Debug, Clone, Default)]
pub struct ReplayRunOptions {
    /// When `Some(n)`, only the first `n` successful events get a `per_event` row; failures are always recorded.
    /// `None` = record every event (default).
    pub max_per_event_summaries: Option<usize>,
    /// When `Some(k)`, print progress to stderr every `k` processed events.
    pub progress_interval: Option<usize>,
    /// If true, events with no routing targets fail without calling adapters.
    pub validate_routing: bool,
    /// Events per batch transaction (see [`crate::batch`]). `None` =
    /// [`DEFAULT_REPLAY_BATCH`]; `Some(1)` dispatches one event at a time.
    pub max_batch: Option<usize>,
}

/// Default events per batch for replay and rebuild: catching up favours
/// larger transactions than live ingest does.
pub const DEFAULT_REPLAY_BATCH: usize = 5000;

fn unrouted_event_report(event: &Event) -> ExecutionReport {
    let now = SystemTime::now();
    let trace_id = format!("trace-{}", event.id());
    let mut report = ExecutionReport::new(
        event.event_id.clone(),
        event.event_type.clone(),
        trace_id,
        now,
    )
    .with_source_version(event.version());
    let adapter_report = AdapterExecutionReport::new("routing".to_string(), StorageKind::Sql, now)
        .finish_failed(
            now,
            AdapterReportError::WriteFailed {
                message: format!("no routing rule for event_type {:?}", event.event_type),
            },
        );
    report.push_adapter_report(adapter_report);
    report.finish(now)
}

// ---------------------------------------------------------------------------
// ReplayContext
// ---------------------------------------------------------------------------

pub struct ReplayContext {
    adapters: Vec<Box<dyn crate::adapter::StorageAdapter>>,
    failure_policy: FailurePolicy,
    routing_rules: HashMap<String, Vec<AdapterId>>,
    adapter_meta: HashMap<AdapterId, AdapterExecutionMeta>,
}

impl ReplayContext {
    /// Build adapters once (multiple SQL or file adapters each get a clone of the mapping bundle).
    /// Runs with an empty [crate::upcast::UpcasterRegistry]; use [ReplayContext::new_with_upcasters]
    /// to replay mixed-version streams through registered upcasters (Phase 10.3/10.4).
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: &KulyraxConfig,
        routing_rules: HashMap<String, Vec<AdapterId>>,
        sql_mappings: HashMap<String, SqlMapping>,
        document_mappings: HashMap<String, DocumentMapping>,
        kv_mappings: HashMap<String, KvMapping>,
        graph_mappings: HashMap<String, GraphMapping>,
    ) -> Self {
        Self::new_with_upcasters(
            config,
            routing_rules,
            sql_mappings,
            document_mappings,
            kv_mappings,
            graph_mappings,
            std::sync::Arc::new(crate::upcast::UpcasterRegistry::new()),
        )
    }

    /// Same as [ReplayContext::new], with an explicit [crate::upcast::UpcasterRegistry].
    #[allow(clippy::too_many_arguments)]
    pub fn new_with_upcasters(
        config: &KulyraxConfig,
        routing_rules: HashMap<String, Vec<AdapterId>>,
        sql_mappings: HashMap<String, SqlMapping>,
        document_mappings: HashMap<String, DocumentMapping>,
        kv_mappings: HashMap<String, KvMapping>,
        graph_mappings: HashMap<String, GraphMapping>,
        upcasters: std::sync::Arc<crate::upcast::UpcasterRegistry>,
    ) -> Self {
        Self {
            adapters: build_adapters_from_config(
                config,
                crate::runtime::MappingSet {
                    sql: sql_mappings,
                    document: document_mappings,
                    kv: kv_mappings,
                    graph: graph_mappings,
                },
                upcasters,
                &crate::runtime::AdapterRegistry::new(),
            ),
            failure_policy: config.failure_policy,
            routing_rules,
            adapter_meta: adapter_metadata_map(config),
        }
    }

    /// Wrap an already-built adapter set (Phase 27's
    /// [`crate::runtime::engine::KulyraxRuntime`]) instead of building a
    /// fresh one — avoids rebuilding/reconnecting adapters the caller already
    /// owns.
    pub(crate) fn from_parts(
        adapters: Vec<Box<dyn crate::adapter::StorageAdapter>>,
        failure_policy: FailurePolicy,
        routing_rules: HashMap<String, Vec<AdapterId>>,
        adapter_meta: HashMap<AdapterId, AdapterExecutionMeta>,
    ) -> Self {
        Self {
            adapters,
            failure_policy,
            routing_rules,
            adapter_meta,
        }
    }

    /// Take back the adapters this context owns, so a caller that lent them
    /// in via [`ReplayContext::from_parts`] can keep using the same built set.
    pub(crate) fn into_adapters(self) -> Vec<Box<dyn crate::adapter::StorageAdapter>> {
        self.adapters
    }

    /// Process events with default options (full `per_event` list).
    pub fn run_stream(
        &mut self,
        events: impl Iterator<Item = Result<Event, ReplayLoadError>>,
    ) -> Result<ReplayReport, ReplayLoadError> {
        self.run_stream_with_options(events, &ReplayRunOptions::default())
    }

    /// Process events with options.
    pub fn run_stream_with_options(
        &mut self,
        events: impl Iterator<Item = Result<Event, ReplayLoadError>>,
        opts: &ReplayRunOptions,
    ) -> Result<ReplayReport, ReplayLoadError> {
        // Before any event: an adapter that fails to prepare then fails each
        // event routed to it, with the prepare error in the report.
        let _ = crate::adapter::prepare_adapters(&self.adapters);
        let started_at = SystemTime::now();
        let mut report = ReplayReport::new(started_at);

        // PHASE 15.5: replay is sequence-ordered, not file-name / line ordered.
        // `Event.sequence` has been captured since Phase 11 and used only for
        // *gating*; ordering the whole stream by it delivers every entity's
        // events in the order `decide()` needs — the create before any facet
        // update for the same entity — regardless of how the input happened to
        // be named or laid out. A stable sort keeps the loader's order
        // (sorted file names / line order) for equal sequences, so replay
        // stays deterministic. The doc frames this as `(entity_key, sequence)`;
        // a global stable sort by `sequence` is equivalent for the guarantee
        // (each entity's events keep their relative order) without needing to
        // resolve routing/mappings inside the loader.
        let mut buffered: Vec<Event> = Vec::new();
        for item in events {
            buffered.push(item?);
        }
        buffered.sort_by_key(|e| e.sequence);

        let batch_size = opts.max_batch.unwrap_or(DEFAULT_REPLAY_BATCH).max(1);
        for chunk in buffered.chunks(batch_size) {
            // One transaction per adapter for the chunk. A chunk with an
            // unrouted event under `validate_routing` (reported without
            // dispatch), or one that rolls back, goes one event at a time.
            let unrouted = opts.validate_routing
                && chunk
                    .iter()
                    .any(|e| route_with_rules(e, &self.routing_rules).is_empty());
            if batch_size > 1 && !unrouted {
                let events: Vec<&Event> = chunk.iter().collect();
                if let BatchResult::Committed(reports) = dispatch_batch(
                    &events,
                    &mut self.adapters,
                    self.failure_policy,
                    &self.routing_rules,
                    &self.adapter_meta,
                ) {
                    for (event, exec) in chunk.iter().zip(reports) {
                        record_replayed(&mut report, event, &exec, opts);
                    }
                    continue;
                }
            }
            if !self.replay_one_by_one(chunk, opts, &mut report) {
                break;
            }
        }

        Ok(report.finish(SystemTime::now()))
    }

    /// Dispatch `events` one at a time, recording each. `false` when a
    /// failure stopped the replay (`FailurePolicy::FailFast`).
    fn replay_one_by_one(
        &mut self,
        events: &[Event],
        opts: &ReplayRunOptions,
        report: &mut ReplayReport,
    ) -> bool {
        for event in events {
            let exec = if opts.validate_routing
                && route_with_rules(event, &self.routing_rules).is_empty()
            {
                unrouted_event_report(event)
            } else {
                dispatch(
                    event,
                    &mut self.adapters,
                    self.failure_policy,
                    &self.routing_rules,
                    &self.adapter_meta,
                )
            };
            let succeeded = record_replayed(report, event, &exec, opts);
            if !succeeded && self.failure_policy == FailurePolicy::FailFast {
                report.stopped_early = true;
                return false;
            }
        }
        true
    }
}

/// Count one replayed event in `report` and record its summary; returns
/// whether it succeeded.
fn record_replayed(
    report: &mut ReplayReport,
    event: &Event,
    exec: &ExecutionReport,
    opts: &ReplayRunOptions,
) -> bool {
    let succeeded = exec.status == ExecutionStatus::Succeeded;

    report.events_processed += 1;
    if succeeded {
        report.events_succeeded += 1;
    } else {
        report.events_failed += 1;
        if report.first_failure_event_id.is_none() {
            report.first_failure_event_id = Some(event.event_id.clone());
        }
    }

    let cap = opts.max_per_event_summaries;
    let record_summary = !succeeded || cap.is_none_or(|c| report.per_event.len() < c);
    if record_summary {
        report.per_event.push(PerEventReplaySummary {
            event_id: event.event_id.clone(),
            event_type: event.event_type.clone(),
            status: exec.status,
        });
    } else if succeeded {
        report.per_event_summaries_omitted += 1;
    }

    if let Some(k) = opts.progress_interval
        && k > 0
        && report.events_processed.is_multiple_of(k as u64)
    {
        eprintln!(
            "kulyrax replay: {} events processed",
            report.events_processed
        );
    }
    succeeded
}

/// Build context and run stream. Fails on first event load error.
#[allow(clippy::too_many_arguments)]
pub fn replay_stream(
    config: &KulyraxConfig,
    routing_rules: HashMap<String, Vec<AdapterId>>,
    sql_mappings: HashMap<String, SqlMapping>,
    document_mappings: HashMap<String, DocumentMapping>,
    kv_mappings: HashMap<String, KvMapping>,
    graph_mappings: HashMap<String, GraphMapping>,
    events: impl Iterator<Item = Result<Event, ReplayLoadError>>,
) -> Result<ReplayReport, ReplayLoadError> {
    replay_stream_with_options(
        config,
        routing_rules,
        sql_mappings,
        document_mappings,
        kv_mappings,
        graph_mappings,
        events,
        &ReplayRunOptions::default(),
    )
}

#[allow(clippy::too_many_arguments)]
pub fn replay_stream_with_options(
    config: &KulyraxConfig,
    routing_rules: HashMap<String, Vec<AdapterId>>,
    sql_mappings: HashMap<String, SqlMapping>,
    document_mappings: HashMap<String, DocumentMapping>,
    kv_mappings: HashMap<String, KvMapping>,
    graph_mappings: HashMap<String, GraphMapping>,
    events: impl Iterator<Item = Result<Event, ReplayLoadError>>,
    opts: &ReplayRunOptions,
) -> Result<ReplayReport, ReplayLoadError> {
    let mut ctx = ReplayContext::new(
        config,
        routing_rules,
        sql_mappings,
        document_mappings,
        kv_mappings,
        graph_mappings,
    );
    ctx.run_stream_with_options(events, opts)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn events_from_directory_sorted_lexicographic() {
        let tmp = tempfile::tempdir().unwrap();
        let z = Event {
            event_id: "z".into(),
            event_type: "T".into(),
            payload: "{}".into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        };
        let a = Event {
            event_id: "a".into(),
            event_type: "T".into(),
            payload: "{}".into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        };
        fs::write(
            tmp.path().join("z.json"),
            serde_json::to_string(&z).unwrap(),
        )
        .unwrap();
        fs::write(
            tmp.path().join("a.json"),
            serde_json::to_string(&a).unwrap(),
        )
        .unwrap();
        let mut it = events_from_path(tmp.path()).unwrap();
        assert_eq!(it.next().unwrap().unwrap().event_id, "a");
        assert_eq!(it.next().unwrap().unwrap().event_id, "z");
        assert!(it.next().is_none());
    }

    #[test]
    fn ndjson_skips_blank_lines_without_recursion_stack() {
        let tmp = tempfile::tempdir().unwrap();
        let p = tmp.path().join("e.ndjson");
        let e1 = Event {
            event_id: "1".into(),
            event_type: "T".into(),
            payload: "{}".into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        };
        let e2 = Event {
            event_id: "2".into(),
            event_type: "T".into(),
            payload: "{}".into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        };
        fs::write(
            &p,
            format!(
                "\n\n{}\n\n{}\n",
                serde_json::to_string(&e1).unwrap(),
                serde_json::to_string(&e2).unwrap()
            ),
        )
        .unwrap();
        let mut it = events_from_path(&p).unwrap();
        assert_eq!(it.next().unwrap().unwrap().event_id, "1");
        assert_eq!(it.next().unwrap().unwrap().event_id, "2");
        assert!(it.next().is_none());
    }
}
