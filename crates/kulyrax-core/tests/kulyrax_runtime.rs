//! Phase 27: `KulyraxRuntime` is the facade replacing the free-function pile
//! (`execute_event*`) and the old process-wide adapter-factory registry.
//! Verifies: a factory registered on one runtime instance resolves a
//! `Custom` adapter through `run_once` exactly like `build_adapters_from_config`
//! does directly (`registry_adapter_dispatches_identically.rs`), adapters
//! build lazily on first use, and — the whole point of instance-scoping —
//! two runtimes with the same registered type name don't see each other's
//! factories.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use kulyrax_core::adapter::{AdapterResult, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::execution::{AdapterOutcome, ExecutionStatus};
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::KulyraxRuntime;
use kulyrax_core::runtime::config::{
    AdapterConfig, CustomAdapterConfig, KulyraxConfig, RoutingConfig,
};

struct CountingAdapter {
    id: String,
    priority: u32,
    calls: Arc<AtomicUsize>,
}

impl StorageAdapter for CountingAdapter {
    fn kind(&self) -> StorageKind {
        StorageKind::Sql
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn priority(&self) -> u32 {
        self.priority
    }
    fn handle(&self, _event: &Event) -> AdapterResult {
        self.calls.fetch_add(1, Ordering::SeqCst);
        AdapterResult::created(self.id.clone(), StorageKind::Sql)
    }
}

fn user_created_event() -> Event {
    Event {
        event_id: "evt-1".into(),
        event_type: "UserCreated".into(),
        payload: "{}".into(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    }
}

fn config_with_one_custom_adapter(type_name: &str) -> KulyraxConfig {
    KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "routing.json".into(),
        },
        adapters: vec![AdapterConfig::Custom(CustomAdapterConfig {
            type_name: type_name.into(),
            id: "sql-primary".into(),
            priority: 10,
            raw: serde_yaml::Value::Null,
            capabilities: None,
            depends_on: vec![],
        })],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    }
}

fn routing_to_sql_primary() -> HashMap<String, Vec<String>> {
    HashMap::from([("UserCreated".to_string(), vec!["sql-primary".to_string()])])
}

#[test]
fn registered_factory_resolves_through_run_once_and_builds_adapters_once() {
    let type_name = "test-counting-sql-27";
    let build_calls = Arc::new(AtomicUsize::new(0));
    let handle_calls = Arc::new(AtomicUsize::new(0));
    let build_calls_in_factory = Arc::clone(&build_calls);
    let handle_calls_for_adapter = Arc::clone(&handle_calls);

    let config = config_with_one_custom_adapter(type_name);
    let mut runtime = KulyraxRuntime::from_parts(
        config,
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        routing_to_sql_primary(),
    );
    runtime.register_adapter_factory(type_name, move |_raw| {
        build_calls_in_factory.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(CountingAdapter {
            id: "sql-primary".to_string(),
            priority: 10,
            calls: Arc::clone(&handle_calls_for_adapter),
        }) as Box<dyn StorageAdapter>)
    });

    let report = runtime.run_once(user_created_event());
    assert_eq!(report.status, ExecutionStatus::Succeeded);
    assert_eq!(report.adapter_reports.len(), 1);
    assert_eq!(report.adapter_reports[0].adapter_id, "sql-primary");
    assert_eq!(report.adapter_reports[0].outcome, AdapterOutcome::Created);

    // A second run_once must not rebuild the adapter set (build-once, not
    // build-per-event) — the factory itself runs exactly once, while the
    // adapter's handle() runs once per event.
    let report2 = runtime.run_once(user_created_event());
    assert_eq!(report2.status, ExecutionStatus::Succeeded);
    assert_eq!(
        build_calls.load(Ordering::SeqCst),
        1,
        "the factory must run once, at first-use build, not once per run_once call"
    );
    assert_eq!(
        handle_calls.load(Ordering::SeqCst),
        2,
        "the same adapter instance must handle both events"
    );
}

#[test]
fn two_runtimes_with_the_same_type_name_do_not_share_factories() {
    let type_name = "test-shared-name-27";

    let mut runtime_a = KulyraxRuntime::from_parts(
        config_with_one_custom_adapter(type_name),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        routing_to_sql_primary(),
    );
    runtime_a.register_adapter_factory(type_name, |_raw| {
        Ok(Box::new(CountingAdapter {
            id: "sql-primary".to_string(),
            priority: 10,
            calls: Arc::new(AtomicUsize::new(0)),
        }) as Box<dyn StorageAdapter>)
    });

    // runtime_b never registers `type_name` — its Custom adapter must become
    // a FailedAdapter placeholder, not silently reuse runtime_a's factory.
    let mut runtime_b = KulyraxRuntime::from_parts(
        config_with_one_custom_adapter(type_name),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        routing_to_sql_primary(),
    );

    let report_a = runtime_a.run_once(user_created_event());
    assert_eq!(report_a.status, ExecutionStatus::Succeeded);

    let report_b = runtime_b.run_once(user_created_event());
    assert_eq!(report_b.status, ExecutionStatus::Failed);
    let msg = format!("{:?}", report_b.adapter_reports[0].error);
    assert!(
        msg.contains(type_name),
        "runtime_b must fail with an unregistered-type error, not reuse runtime_a's factory: {msg}"
    );
}
