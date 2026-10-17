//! Phase 14.3's pass condition: adding the key-value adapter requires **zero**
//! `StorageKind::KeyValue` special-casing in `dispatch.rs`. A KV adapter that
//! `depends_on` a SQL adapter runs in the correct execution frontier through
//! the exact same `dispatch()` / `AdapterExecutionMeta` machinery every other
//! storage kind uses — no dispatch code path specific to key-value.

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

fn test_routing() -> HashMap<String, Vec<AdapterId>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("routing.yaml");

    load_routing(path).expect("routing fixture loads")
}

fn test_event() -> Event {
    Event {
        event_id: "evt-1".into(),
        event_type: "UserCacheSynced".into(),
        payload: "{}".into(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    }
}

#[test]
fn kv_adapter_runs_after_its_sql_dependency_via_ordinary_dispatch() {
    let event = test_event();

    let mut adapters: Vec<Box<dyn StorageAdapter>> = vec![
        Box::new(TestAdapter {
            id: "sql-primary",
            kind: StorageKind::Sql,
        }),
        Box::new(TestAdapter {
            id: "kv-cache",
            kind: StorageKind::KeyValue,
        }),
    ];

    // routing.yaml routes "UserCacheSynced" to ["sql-primary", "kv-cache"];
    // kv-cache depends_on sql-primary.
    let meta: HashMap<AdapterId, AdapterExecutionMeta> = HashMap::from([
        (
            "sql-primary".into(),
            AdapterExecutionMeta {
                priority: 10,
                depends_on: vec![],
            },
        ),
        (
            "kv-cache".into(),
            AdapterExecutionMeta {
                priority: 5, // lower priority, but the dependency must still win
                depends_on: vec!["sql-primary".into()],
            },
        ),
    ]);

    let report = dispatch(
        &event,
        &mut adapters,
        FailurePolicy::FailFast,
        &test_routing(),
        &meta,
    );

    assert_eq!(report.adapter_reports.len(), 2);
    assert_eq!(
        report.adapter_reports[0].adapter_id, "sql-primary",
        "the dependency must run first regardless of priority"
    );
    assert_eq!(report.adapter_reports[1].adapter_id, "kv-cache");
    assert_eq!(
        report.adapter_reports[1].storage_kind,
        StorageKind::KeyValue
    );

    // Sanity: a failure in the dependency still fails fast, same as any other kind.
    let mut failing_adapters: Vec<Box<dyn StorageAdapter>> = vec![
        Box::new(FailingAdapter { id: "sql-primary" }),
        Box::new(TestAdapter {
            id: "kv-cache",
            kind: StorageKind::KeyValue,
        }),
    ];
    let failed_report = dispatch(
        &event,
        &mut failing_adapters,
        FailurePolicy::FailFast,
        &test_routing(),
        &meta,
    );
    assert_eq!(
        failed_report.adapter_reports.len(),
        1,
        "kv-cache must not run after its dependency fails"
    );
}

struct FailingAdapter {
    id: &'static str,
}

impl StorageAdapter for FailingAdapter {
    fn id(&self) -> &str {
        self.id
    }

    fn kind(&self) -> StorageKind {
        StorageKind::Sql
    }

    fn priority(&self) -> u32 {
        0
    }

    fn handle(&self, _event: &Event) -> AdapterResult {
        AdapterResult::failure(
            self.id.to_string(),
            StorageKind::Sql,
            AdapterError::WriteFailed("forced failure".to_string()),
        )
    }
}
