use kulyrax_core::AdapterExecutionMeta;
use kulyrax_core::adapter::{AdapterError, AdapterResult, StorageAdapter};
use kulyrax_core::dispatch::dispatch;
use kulyrax_core::event::Event;
use kulyrax_core::execution::{AdapterOutcome, ExecutionStatus};
use kulyrax_core::routing::{AdapterId, StorageKind, load_routing, route_with_rules};
use kulyrax_core::runtime::config::FailurePolicy;

use std::collections::HashMap;

// ------------------------------------------------------------
// Test Adapter
// ------------------------------------------------------------

struct TestAdapter {
    id: &'static str,
    kind: StorageKind,
    succeed: bool,
}

impl StorageAdapter for TestAdapter {
    fn id(&self) -> &str {
        self.id
    }

    fn kind(&self) -> StorageKind {
        self.kind
    }

    fn priority(&self) -> u32 {
        0
    }

    fn handle(&self, _event: &Event) -> AdapterResult {
        // adapter_id 🔴 THIS MUST MATCH routing.json
        if self.succeed {
            AdapterResult::created(self.id.to_string(), self.kind)
        } else {
            AdapterResult::failure(
                self.id.to_string(),
                self.kind,
                AdapterError::WriteFailed("forced failure".to_string()),
            )
        }
    }
}

// ------------------------------------------------------------
// Helpers
// ------------------------------------------------------------

fn test_routing() -> HashMap<String, Vec<AdapterId>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("routing.json");

    load_routing(path).expect("routing fixture loads")
}

fn test_event(event_type: &str) -> Event {
    Event {
        event_id: "evt-1".into(),
        event_type: event_type.into(),
        payload: "{}".into(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    }
}

/// Matches typical config: SQL before document (priorities 10 / 20).
fn fixture_adapter_meta() -> HashMap<AdapterId, AdapterExecutionMeta> {
    HashMap::from([
        (
            "sql-primary".into(),
            AdapterExecutionMeta {
                priority: 10,
                depends_on: vec![],
            },
        ),
        (
            "doc-readmodel".into(),
            AdapterExecutionMeta {
                priority: 20,
                depends_on: vec![],
            },
        ),
        (
            "kv-cache".into(),
            AdapterExecutionMeta {
                priority: 5,
                depends_on: vec![],
            },
        ),
    ])
}

// ------------------------------------------------------------
// Tests
// ------------------------------------------------------------

#[test]
fn dispatch_executes_all_targeted_adapters() {
    let event = test_event("UserCreated");

    let mut adapters: Vec<Box<dyn StorageAdapter>> = vec![
        Box::new(TestAdapter {
            id: "sql-primary",
            kind: StorageKind::Sql,
            succeed: true,
        }),
        Box::new(TestAdapter {
            id: "doc-readmodel",
            kind: StorageKind::Document,
            succeed: true,
        }),
    ];

    let meta = fixture_adapter_meta();
    let report = dispatch(
        &event,
        &mut adapters,
        FailurePolicy::FailFast,
        &test_routing(),
        &meta,
    );

    assert_eq!(report.adapter_reports.len(), 2);
    assert_eq!(report.adapter_reports[0].adapter_id, "sql-primary");
    assert_eq!(report.adapter_reports[1].adapter_id, "doc-readmodel");
    assert_eq!(report.status, ExecutionStatus::Succeeded);
    assert!(
        report
            .adapter_reports
            .iter()
            .all(|r| r.outcome == AdapterOutcome::Created)
    );
}

#[test]
fn dispatch_stops_on_adapter_failure() {
    let event = test_event("FailingEvent");

    let targets = route_with_rules(&event, &test_routing());
    assert!(!targets.is_empty(), "FailingEvent must be routed");

    let mut adapters: Vec<Box<dyn StorageAdapter>> = vec![
        Box::new(TestAdapter {
            id: "sql-primary",
            kind: StorageKind::Sql,
            succeed: false,
        }),
        Box::new(TestAdapter {
            id: "doc-readmodel",
            kind: StorageKind::Document,
            succeed: true,
        }),
    ];

    let meta = fixture_adapter_meta();
    let report = dispatch(
        &event,
        &mut adapters,
        FailurePolicy::FailFast,
        &test_routing(),
        &meta,
    );

    assert_eq!(report.adapter_reports.len(), 1);
    assert_eq!(report.adapter_reports[0].adapter_id, "sql-primary");
    assert_eq!(report.adapter_reports[0].outcome, AdapterOutcome::Failed);
    assert_eq!(report.status, ExecutionStatus::Failed);
}

#[test]
fn dispatch_continue_on_error_runs_all_adapters() {
    let event = test_event("UserCreated");

    let mut adapters: Vec<Box<dyn StorageAdapter>> = vec![
        Box::new(TestAdapter {
            id: "sql-primary",
            kind: StorageKind::Sql,
            succeed: false,
        }),
        Box::new(TestAdapter {
            id: "doc-readmodel",
            kind: StorageKind::Document,
            succeed: true,
        }),
    ];

    let meta = fixture_adapter_meta();
    let report = dispatch(
        &event,
        &mut adapters,
        FailurePolicy::ContinueOnError,
        &test_routing(),
        &meta,
    );

    assert_eq!(report.adapter_reports.len(), 2);
    assert_eq!(report.adapter_reports[0].outcome, AdapterOutcome::Failed);
    assert_eq!(report.adapter_reports[1].outcome, AdapterOutcome::Created);
    assert_eq!(report.status, ExecutionStatus::Failed);
}

#[test]
fn dispatch_only_runs_routed_adapters() {
    let event = test_event("UserCreated");

    let mut adapters: Vec<Box<dyn StorageAdapter>> = vec![
        Box::new(TestAdapter {
            id: "sql-primary",
            kind: StorageKind::Sql,
            succeed: true,
        }),
        Box::new(TestAdapter {
            id: "doc-readmodel",
            kind: StorageKind::Document,
            succeed: true,
        }),
        Box::new(TestAdapter {
            id: "kv-cache",
            kind: StorageKind::KeyValue,
            succeed: true,
        }),
    ];

    let meta = fixture_adapter_meta();
    let report = dispatch(
        &event,
        &mut adapters,
        FailurePolicy::FailFast,
        &test_routing(),
        &meta,
    );

    // routing.json selects only Sql + Document
    assert_eq!(report.adapter_reports.len(), 2);
    assert!(
        report
            .adapter_reports
            .iter()
            .all(|r| r.storage_kind != StorageKind::KeyValue)
    );
}

#[test]
fn dispatch_runs_dependency_before_dependent() {
    let event = test_event("UserCreated");
    let mut adapters: Vec<Box<dyn StorageAdapter>> = vec![
        Box::new(TestAdapter {
            id: "sql-primary",
            kind: StorageKind::Sql,
            succeed: true,
        }),
        Box::new(TestAdapter {
            id: "doc-readmodel",
            kind: StorageKind::Document,
            succeed: true,
        }),
    ];
    let mut meta = fixture_adapter_meta();
    meta.insert(
        "doc-readmodel".into(),
        AdapterExecutionMeta {
            priority: 5,
            depends_on: vec!["sql-primary".into()],
        },
    );
    let report = dispatch(
        &event,
        &mut adapters,
        FailurePolicy::FailFast,
        &test_routing(),
        &meta,
    );
    assert_eq!(report.adapter_reports[0].adapter_id, "sql-primary");
    assert_eq!(report.adapter_reports[1].adapter_id, "doc-readmodel");
}

#[test]
fn dispatch_cycle_yields_failed_report_not_panic() {
    let event = test_event("UserCreated");
    let mut adapters: Vec<Box<dyn StorageAdapter>> = vec![
        Box::new(TestAdapter {
            id: "sql-primary",
            kind: StorageKind::Sql,
            succeed: true,
        }),
        Box::new(TestAdapter {
            id: "doc-readmodel",
            kind: StorageKind::Document,
            succeed: true,
        }),
    ];
    let mut meta = fixture_adapter_meta();
    meta.insert(
        "sql-primary".into(),
        AdapterExecutionMeta {
            priority: 10,
            depends_on: vec!["doc-readmodel".into()],
        },
    );
    meta.insert(
        "doc-readmodel".into(),
        AdapterExecutionMeta {
            priority: 20,
            depends_on: vec!["sql-primary".into()],
        },
    );
    let report = dispatch(
        &event,
        &mut adapters,
        FailurePolicy::FailFast,
        &test_routing(),
        &meta,
    );
    assert_eq!(report.status, ExecutionStatus::Failed);
    assert_eq!(report.adapter_reports.len(), 1);
    assert_eq!(report.adapter_reports[0].adapter_id, "_dispatch");
    assert!(
        report.adapter_reports[0]
            .error
            .as_ref()
            .is_some_and(|e| format!("{:?}", e).contains("cyclic"))
    );
}
