//! Phase 25.5: the registry mechanism itself is correct — a `type:` with a
//! registered factory produces an adapter that participates in routing,
//! dependency ordering, and dispatch exactly like a built-in one. (A specific
//! third-party backend's own correctness is that backend's responsibility,
//! same as any Rust trait implementation — not what this test is for.)
//!
//! Mirrors `dispatch.rs`'s own `set_test_routing()` / fixture-routing
//! pattern: `tests/fixtures/routing.json` already sends `"UserCreated"` to
//! `["sql-primary", "doc-readmodel"]`, so this test registers a mock factory
//! under `id: "sql-primary"` and runs a real built-in (`file`) adapter under
//! `id: "doc-readmodel"`, wired so the built-in depends on the registered
//! one — proving priority, dependency order, and the execution report treat
//! them identically.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::{AdapterResult, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::execution::{AdapterOutcome, ExecutionStatus};
use kulyrax_core::routing::{AdapterId, StorageKind, load_routing};
use kulyrax_core::runtime::build_adapters_from_config;
use kulyrax_core::runtime::config::{
    AdapterConfig, CustomAdapterConfig, FileAdapterConfig, FileConfig, KulyraxConfig, RoutingConfig,
};
use kulyrax_core::runtime::{AdapterExecutionMeta, AdapterRegistry, adapter_metadata_map};

fn test_routing() -> HashMap<String, Vec<AdapterId>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("routing.json");
    load_routing(path).expect("routing fixture loads")
}

/// A trivial adapter a registered factory hands back — proof the registry
/// path produces a real `Box<dyn StorageAdapter>`, nothing special-cased.
struct MockAdapter {
    id: String,
    priority: u32,
}

impl StorageAdapter for MockAdapter {
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
        AdapterResult::created(self.id.clone(), StorageKind::Sql)
    }
}

fn user_created_event() -> Event {
    Event {
        event_id: "evt-1".into(),
        event_type: "UserCreated".into(),
        payload: serde_json::json!({ "id": "u1", "name": "Ada" }).to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    }
}

fn doc_mapping() -> DocumentMapping {
    serde_yaml::from_str(
        r#"
event: UserCreated
collection: users
version: 1
id: payload.id
document:
  id: payload.id
  name: payload.name
"#,
    )
    .unwrap()
}

#[test]
fn registered_adapter_participates_identically_in_dependency_order_and_dispatch() {
    let type_name = "test-mock-sql-25-5";
    let invoked = Arc::new(AtomicBool::new(false));
    let invoked_in_factory = Arc::clone(&invoked);
    let mut registry = AdapterRegistry::new();
    registry.register(type_name, move |raw| {
        invoked_in_factory.store(true, Ordering::SeqCst);
        // The registry hands the factory the *entire* raw adapter entry
        // (id/priority/type/config/...), not just a `config:` sub-block —
        // enough for the factory to build a self-describing adapter.
        let id = raw
            .get("id")
            .and_then(|v| v.as_str())
            .expect("id present in raw entry")
            .to_string();
        let priority = raw
            .get("priority")
            .and_then(|v| v.as_u64())
            .expect("priority present in raw entry") as u32;
        Ok(Box::new(MockAdapter { id, priority }) as Box<dyn StorageAdapter>)
    });

    let tmp = tempfile::tempdir().unwrap();
    let config = KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "routing.json".into(),
        },
        adapters: vec![
            AdapterConfig::Custom(CustomAdapterConfig {
                type_name: type_name.into(),
                id: "sql-primary".into(),
                priority: 10,
                raw: serde_yaml::to_value(serde_json::json!({
                    "type": type_name,
                    "id": "sql-primary",
                    "priority": 10,
                }))
                .unwrap(),
                capabilities: None,
                depends_on: vec![],
            }),
            AdapterConfig::File(FileAdapterConfig {
                id: "doc-readmodel".into(),
                priority: 20,
                config: FileConfig {
                    root: tmp.path().to_string_lossy().into_owned(),
                },
                capabilities: None,
                depends_on: vec!["sql-primary".into()],
            }),
        ],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    };
    config
        .validate()
        .expect("config with a Custom adapter validates like any other");

    let mut doc_mappings = HashMap::new();
    doc_mappings.insert("UserCreated".to_string(), doc_mapping());

    let mut adapters = build_adapters_from_config(
        &config,
        HashMap::new(),
        doc_mappings,
        HashMap::new(),
        HashMap::new(),
        Arc::new(kulyrax_core::UpcasterRegistry::new()),
        &registry,
    );

    assert!(
        invoked.load(Ordering::SeqCst),
        "the registered factory must actually run during build_adapters_from_config"
    );
    // Priority-sorted just like built-ins (factory.rs's final `sort_by_key`).
    assert_eq!(adapters[0].id(), "sql-primary");
    assert_eq!(adapters[1].id(), "doc-readmodel");

    let meta: HashMap<String, AdapterExecutionMeta> = adapter_metadata_map(&config);
    let report = kulyrax_core::dispatch::dispatch(
        &user_created_event(),
        &mut adapters,
        config.failure_policy,
        &test_routing(),
        &meta,
    );

    assert_eq!(report.status, ExecutionStatus::Succeeded);
    assert_eq!(report.adapter_reports.len(), 2);
    // Dependency order (doc-readmodel depends_on sql-primary) put the
    // registered adapter first, exactly as it would for two built-ins.
    assert_eq!(report.adapter_reports[0].adapter_id, "sql-primary");
    assert_eq!(report.adapter_reports[0].outcome, AdapterOutcome::Created);
    assert_eq!(report.adapter_reports[1].adapter_id, "doc-readmodel");
    assert_eq!(report.adapter_reports[1].outcome, AdapterOutcome::Created);
}

#[test]
fn unregistered_custom_type_becomes_a_failed_placeholder_not_a_panic() {
    let config = KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "routing.json".into(),
        },
        // `CacheInvalidated` routes to `["kv-cache"]` alone in the shared
        // routing fixture, so a single adapter fully covers this route.
        adapters: vec![AdapterConfig::Custom(CustomAdapterConfig {
            type_name: "definitely-unregistered-25-5".into(),
            id: "kv-cache".into(),
            priority: 10,
            raw: serde_yaml::Value::Null,
            capabilities: None,
            depends_on: vec![],
        })],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    };

    let mut adapters = build_adapters_from_config(
        &config,
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        Arc::new(kulyrax_core::UpcasterRegistry::new()),
        &AdapterRegistry::new(),
    );
    assert_eq!(
        adapters.len(),
        1,
        "still infallible — one placeholder, not a panic or a missing adapter"
    );

    let meta = adapter_metadata_map(&config);
    let cache_event = Event {
        event_id: "evt-2".into(),
        event_type: "CacheInvalidated".into(),
        payload: "{}".into(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    };
    let report = kulyrax_core::dispatch::dispatch(
        &cache_event,
        &mut adapters,
        config.failure_policy,
        &test_routing(),
        &meta,
    );
    assert_eq!(report.status, ExecutionStatus::Failed);
    assert_eq!(report.adapter_reports[0].outcome, AdapterOutcome::Failed);
    let msg = format!("{:?}", report.adapter_reports[0].error);
    assert!(
        msg.contains("definitely-unregistered-25-5"),
        "the failure should name the unregistered type: {msg}"
    );
}
