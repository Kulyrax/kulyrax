//! Phase 16's pass condition: adding the graph adapter requires **zero**
//! `StorageKind::Graph` special-casing in `dispatch.rs` /
//! `runtime/dependency_graph.rs`. A graph adapter that `depends_on` a SQL
//! adapter runs in the correct execution frontier through the same
//! `dispatch()` / `AdapterExecutionMeta` machinery every other kind uses.

use kulyrax_core::AdapterExecutionMeta;
use kulyrax_core::adapter::{AdapterError, AdapterResult, StorageAdapter};
use kulyrax_core::dispatch::dispatch;
use kulyrax_core::event::Event;
use kulyrax_core::routing::{AdapterId, StorageKind, load_routing};
use kulyrax_core::runtime::config::FailurePolicy;

use std::collections::HashMap;

struct TestAdapter {
    id: &'static str,
    kind: StorageKind,
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
        AdapterResult::created(self.id.to_string(), self.kind)
    }
}

struct FailingAdapter;

impl StorageAdapter for FailingAdapter {
    fn id(&self) -> &str {
        "sql-primary"
    }
    fn kind(&self) -> StorageKind {
        StorageKind::Sql
    }
    fn priority(&self) -> u32 {
        0
    }
    fn handle(&self, _event: &Event) -> AdapterResult {
        AdapterResult::failure(
            "sql-primary".to_string(),
            StorageKind::Sql,
            AdapterError::WriteFailed("forced".to_string()),
        )
    }
}

fn test_routing() -> HashMap<String, Vec<AdapterId>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("routing.json");
    load_routing(path).expect("routing fixture loads")
}

fn test_event() -> Event {
    Event {
        event_id: "evt-1".into(),
        event_type: "UserGraphSynced".into(),
        payload: "{}".into(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    }
}

fn meta() -> HashMap<AdapterId, AdapterExecutionMeta> {
    HashMap::from([
        (
            "sql-primary".into(),
            AdapterExecutionMeta {
                priority: 10,
                depends_on: vec![],
            },
        ),
        (
            "graph-social".into(),
            AdapterExecutionMeta {
                priority: 5, // lower priority, but the dependency must still win
                depends_on: vec!["sql-primary".into()],
            },
        ),
    ])
}

#[test]
fn graph_adapter_runs_after_its_sql_dependency_via_ordinary_dispatch() {
    let event = test_event();

    let mut adapters: Vec<Box<dyn StorageAdapter>> = vec![
        Box::new(TestAdapter {
            id: "sql-primary",
            kind: StorageKind::Sql,
        }),
        Box::new(TestAdapter {
            id: "graph-social",
            kind: StorageKind::Graph,
        }),
    ];
    let m = meta();
    let rules = test_routing();
    let report = dispatch(&event, &mut adapters, FailurePolicy::FailFast, &rules, &m);

    assert_eq!(report.adapter_reports.len(), 2);
    assert_eq!(report.adapter_reports[0].adapter_id, "sql-primary");
    assert_eq!(report.adapter_reports[1].adapter_id, "graph-social");
    assert_eq!(report.adapter_reports[1].storage_kind, StorageKind::Graph);

    let mut failing: Vec<Box<dyn StorageAdapter>> = vec![
        Box::new(FailingAdapter),
        Box::new(TestAdapter {
            id: "graph-social",
            kind: StorageKind::Graph,
        }),
    ];
    let failed = dispatch(&event, &mut failing, FailurePolicy::FailFast, &rules, &m);
    assert_eq!(
        failed.adapter_reports.len(),
        1,
        "graph-social must not run after its dependency fails"
    );
}
