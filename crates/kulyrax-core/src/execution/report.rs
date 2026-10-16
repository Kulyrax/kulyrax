//! Execution reporting types for observability and audit.
//!
//! All timestamps are serialized as milliseconds since Unix epoch.
//! Time is injected by the runtime — never generated inside report structs.
//! Reports are deterministic in structure; timestamps are runtime-controlled.

use std::time::{Duration, SystemTime};

use serde::{Deserialize, Serialize};

use crate::adapter::{AdapterError, SkipReason};
use crate::routing::StorageKind;

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Overall status of a single event dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionStatus {
    Succeeded,
    Failed,
}

/// Execution mode: normal (writes) or dry-run (no side effects).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    /// Normal execution; adapters perform writes.
    Run,
    /// Simulated execution; adapters should not persist (no writes).
    DryRun,
}

/// Outcome of a single adapter invocation (Phase 12.1; extended Phase 13.1
/// with `Deleted`). `Skipped` carries the machine-readable [`SkipReason`]
/// directly — no more string-sniffing a message to tell an idempotent skip
/// from a stale-sequence rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum AdapterOutcome {
    /// A new entity was written (creation, or a resurrection — Phase 13.4).
    Created,
    /// An existing entity was overwritten (`on_existing: replace` only, Phase 12).
    Updated,
    /// The entity was removed and its tombstone recorded (Phase 13.2/13.3).
    Deleted,
    /// Nothing was written; see `reason`.
    Skipped { reason: SkipReason },
    /// The adapter failed; see the sibling `error` field for detail.
    Failed,
}

/// Structured error for adapter execution. Populated only when `outcome` is
/// [`AdapterOutcome::Failed`] — skips carry their reason on the outcome
/// itself, not here (Phase 12.1).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AdapterReportError {
    WriteFailed { message: String },
    UnsupportedVersion { message: String },
}

impl From<AdapterError> for AdapterReportError {
    fn from(e: AdapterError) -> Self {
        match e {
            AdapterError::WriteFailed(msg) => AdapterReportError::WriteFailed { message: msg },
            AdapterError::UnsupportedVersion(msg) => {
                AdapterReportError::UnsupportedVersion { message: msg }
            }
        }
    }
}

impl From<&AdapterError> for AdapterReportError {
    fn from(e: &AdapterError) -> Self {
        match e {
            AdapterError::WriteFailed(msg) => AdapterReportError::WriteFailed {
                message: msg.clone(),
            },
            AdapterError::UnsupportedVersion(msg) => AdapterReportError::UnsupportedVersion {
                message: msg.clone(),
            },
        }
    }
}

// ---------------------------------------------------------------------------
// ExecutionReport (event-level)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionReport {
    /// Schema version for forward evolution.
    #[serde(default = "default_report_version")]
    pub report_version: String,

    pub event_id: String,
    pub event_type: String,
    pub trace_id: String,

    #[serde(with = "crate::execution::time")]
    pub started_at: SystemTime,

    #[serde(with = "crate::execution::time_option")]
    pub finished_at: Option<SystemTime>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,

    /// Adapter reports in execution order.
    pub adapter_reports: Vec<AdapterExecutionReport>,

    pub status: ExecutionStatus,

    /// Source event payload version (Phase 10.2/10.3). Unversioned legacy events default to 1.
    #[serde(default = "default_source_version")]
    pub source_version: u32,

    /// Phase 17.4: the [`EventSource`](crate::source::EventSource) id this event
    /// was polled from, when it was dispatched by the source run loop rather
    /// than a one-shot `run`/`dry-run`. Observability only — routing is by
    /// `event_type` regardless of source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<String>,

    /// Phase 17.4: the [`SourcePosition`](crate::source::SourcePosition) string
    /// for this event — the position a `commit` past this event would record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_position: Option<String>,
}

fn default_report_version() -> String {
    "1.0".to_string()
}

fn default_source_version() -> u32 {
    1
}

impl ExecutionReport {
    /// Create a new report with injected start time.
    pub fn new(
        event_id: String,
        event_type: String,
        trace_id: String,
        started_at: SystemTime,
    ) -> Self {
        Self {
            report_version: default_report_version(),
            event_id,
            event_type,
            trace_id,
            started_at,
            finished_at: None,
            duration_ms: None,
            adapter_reports: Vec::new(),
            status: ExecutionStatus::Succeeded, // provisional, recalculated on finish
            source_version: default_source_version(),
            source_id: None,
            source_position: None,
        }
    }

    /// Set the source event payload version (defaults to 1 if never called).
    pub fn with_source_version(mut self, source_version: u32) -> Self {
        self.source_version = source_version;
        self
    }

    /// Phase 17.4: attach the source id + position this event was polled from.
    pub fn with_source(
        mut self,
        source_id: impl Into<String>,
        position: impl Into<String>,
    ) -> Self {
        self.source_id = Some(source_id.into());
        self.source_position = Some(position.into());
        self
    }

    /// Add adapter report (preserves order).
    pub fn push_adapter_report(&mut self, report: AdapterExecutionReport) {
        self.adapter_reports.push(report);
    }

    /// Finalize report and compute duration + status.
    pub fn finish(mut self, finished_at: SystemTime) -> Self {
        self.finished_at = Some(finished_at);

        self.duration_ms = finished_at
            .duration_since(self.started_at)
            .ok()
            .map(duration_to_ms);

        self.status = if self
            .adapter_reports
            .iter()
            .any(|r| matches!(r.outcome, AdapterOutcome::Failed))
        {
            ExecutionStatus::Failed
        } else {
            ExecutionStatus::Succeeded
        };

        self
    }
}

// ---------------------------------------------------------------------------
// AdapterExecutionReport (adapter-level)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AdapterExecutionReport {
    /// Adapter id from config.
    pub adapter_id: String,

    /// Storage kind (sql, document, etc.).
    pub storage_kind: StorageKind,

    #[serde(with = "crate::execution::time")]
    pub started_at: SystemTime,

    #[serde(with = "crate::execution::time_option")]
    pub finished_at: Option<SystemTime>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration_ms: Option<u64>,

    pub outcome: AdapterOutcome,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<AdapterReportError>,

    /// Event payload version this adapter observed (Phase 10.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_version: Option<u32>,

    /// Mapping's target schema version this adapter projected into (Phase 10.3).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub projected_version: Option<u32>,
}

impl AdapterExecutionReport {
    /// Create new adapter report with injected start time.
    pub fn new(adapter_id: String, storage_kind: StorageKind, started_at: SystemTime) -> Self {
        Self {
            adapter_id,
            storage_kind,
            started_at,
            finished_at: None,
            duration_ms: None,
            outcome: AdapterOutcome::Created, // provisional
            error: None,
            source_version: None,
            projected_version: None,
        }
    }

    /// Attach observed/target schema versions (Phase 10.3).
    pub fn with_versions(
        mut self,
        source_version: Option<u32>,
        projected_version: Option<u32>,
    ) -> Self {
        self.source_version = source_version;
        self.projected_version = projected_version;
        self
    }

    fn finish_at(&mut self, finished_at: SystemTime) {
        self.finished_at = Some(finished_at);
        self.duration_ms = finished_at
            .duration_since(self.started_at)
            .ok()
            .map(duration_to_ms);
    }

    /// Finish: a new entity was created.
    pub fn finish_created(mut self, finished_at: SystemTime) -> Self {
        self.finish_at(finished_at);
        self.outcome = AdapterOutcome::Created;
        self
    }

    /// Finish: an existing entity was overwritten (Phase 12).
    pub fn finish_updated(mut self, finished_at: SystemTime) -> Self {
        self.finish_at(finished_at);
        self.outcome = AdapterOutcome::Updated;
        self
    }

    /// Finish: the entity was removed and its tombstone recorded (Phase 13.2/13.3).
    pub fn finish_deleted(mut self, finished_at: SystemTime) -> Self {
        self.finish_at(finished_at);
        self.outcome = AdapterOutcome::Deleted;
        self
    }

    /// Finish: nothing was written; see `reason`.
    pub fn finish_skipped(mut self, finished_at: SystemTime, reason: SkipReason) -> Self {
        self.finish_at(finished_at);
        self.outcome = AdapterOutcome::Skipped { reason };
        self
    }

    /// Finish with failure.
    pub fn finish_failed(mut self, finished_at: SystemTime, error: AdapterReportError) -> Self {
        self.finish_at(finished_at);
        self.outcome = AdapterOutcome::Failed;
        self.error = Some(error);
        self
    }
}

// ---------------------------------------------------------------------------
// Utilities
// ---------------------------------------------------------------------------

fn duration_to_ms(d: Duration) -> u64 {
    d.as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_report_has_provisional_succeeded_status() {
        let started = SystemTime::now();
        let report = ExecutionReport::new(
            "evt-1".to_string(),
            "TestEvent".to_string(),
            "trace-1".to_string(),
            started,
        );

        assert_eq!(report.status, ExecutionStatus::Succeeded);
        assert!(report.finished_at.is_none());
        assert!(report.duration_ms.is_none());
        assert!(report.adapter_reports.is_empty());
        assert_eq!(report.report_version, "1.0");
    }

    #[test]
    fn finish_computes_duration_and_succeeded_status() {
        let started = SystemTime::now();
        let finished = started + Duration::from_millis(250);

        let mut report = ExecutionReport::new(
            "e".to_string(),
            "T".to_string(),
            "trace".to_string(),
            started,
        );
        report.push_adapter_report(
            AdapterExecutionReport::new("sql".to_string(), StorageKind::Sql, started)
                .finish_created(finished),
        );
        let report = report.finish(finished);

        assert_eq!(report.status, ExecutionStatus::Succeeded);
        assert_eq!(report.duration_ms, Some(250));
        assert_eq!(report.finished_at, Some(finished));
    }

    #[test]
    fn finish_marks_failed_when_any_adapter_write_failed() {
        let started = SystemTime::now();
        let finished = started + Duration::from_millis(10);

        let mut report = ExecutionReport::new(
            "e".to_string(),
            "T".to_string(),
            "trace".to_string(),
            started,
        );
        report.push_adapter_report(
            AdapterExecutionReport::new("sql".to_string(), StorageKind::Sql, started)
                .finish_created(finished),
        );
        report.push_adapter_report(
            AdapterExecutionReport::new("doc".to_string(), StorageKind::Document, started)
                .finish_failed(
                    finished,
                    AdapterReportError::WriteFailed {
                        message: "boom".to_string(),
                    },
                ),
        );
        let report = report.finish(finished);

        assert_eq!(report.status, ExecutionStatus::Failed);
    }

    #[test]
    fn finish_marks_failed_when_adapter_has_unsupported_version() {
        let started = SystemTime::now();
        let finished = started + Duration::from_millis(10);

        let mut report = ExecutionReport::new(
            "e".to_string(),
            "T".to_string(),
            "trace".to_string(),
            started,
        )
        .with_source_version(1);
        report.push_adapter_report(
            AdapterExecutionReport::new("sql".to_string(), StorageKind::Sql, started)
                .with_versions(Some(1), Some(2))
                .finish_failed(
                    finished,
                    AdapterReportError::UnsupportedVersion {
                        message: "no upcaster chain".to_string(),
                    },
                ),
        );
        let report = report.finish(finished);

        assert_eq!(report.status, ExecutionStatus::Failed);
        assert_eq!(report.source_version, 1);
        assert_eq!(report.adapter_reports[0].source_version, Some(1));
        assert_eq!(report.adapter_reports[0].projected_version, Some(2));
        assert_eq!(report.adapter_reports[0].outcome, AdapterOutcome::Failed);
    }

    #[test]
    fn finish_skipped_does_not_fail_overall_status() {
        let started = SystemTime::now();

        let mut report = ExecutionReport::new(
            "e".to_string(),
            "T".to_string(),
            "trace".to_string(),
            started,
        );
        let adapter =
            AdapterExecutionReport::new("doc".to_string(), StorageKind::Document, started)
                .finish_skipped(started, SkipReason::AlreadyProjected);

        assert_eq!(
            adapter.outcome,
            AdapterOutcome::Skipped {
                reason: SkipReason::AlreadyProjected
            }
        );

        report.push_adapter_report(adapter);
        let report = report.finish(started);

        assert_eq!(report.status, ExecutionStatus::Succeeded);
    }

    #[test]
    fn finish_leaves_duration_none_when_finished_before_started() {
        let started = SystemTime::now();
        let finished = started - Duration::from_millis(50);

        let report = ExecutionReport::new(
            "e".to_string(),
            "T".to_string(),
            "trace".to_string(),
            started,
        )
        .finish(finished);

        assert_eq!(report.duration_ms, None);
        assert_eq!(report.finished_at, Some(finished));
    }

    #[test]
    fn adapter_error_conversions_preserve_message() {
        let err = AdapterError::WriteFailed("oops".to_string());

        let by_ref: AdapterReportError = (&err).into();
        assert_eq!(
            by_ref,
            AdapterReportError::WriteFailed {
                message: "oops".to_string()
            }
        );

        let owned: AdapterReportError = err.into();
        assert_eq!(
            owned,
            AdapterReportError::WriteFailed {
                message: "oops".to_string()
            }
        );

        let unsupported: AdapterReportError =
            AdapterError::UnsupportedVersion("no upcaster chain".to_string()).into();
        assert_eq!(
            unsupported,
            AdapterReportError::UnsupportedVersion {
                message: "no upcaster chain".to_string()
            }
        );
    }
}
