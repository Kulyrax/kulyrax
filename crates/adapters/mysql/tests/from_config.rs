//! `MySqlAdapter::from_config` is the public constructor (decision
//! 2026-11-26-adapter-from-config). It builds the same adapter as the
//! `type: mysql` factory, and validates the same way. No server is needed.

use std::sync::Arc;

use kulyrax_adapter_mysql::{MySqlAdapter, MySqlConfig, factory};
use kulyrax_core::AdapterBuildContext;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::runtime::config::{CustomAdapterConfig, MigrationPolicy};
use kulyrax_core::upcast::UpcasterRegistry;

fn ctx() -> AdapterBuildContext {
    AdapterBuildContext::new(
        Default::default(),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

#[test]
fn from_config_builds_the_adapter_the_builder_describes() {
    let cfg = MySqlConfig::new("mysql://app@127.0.0.1:1/app")
        .with_pool_size(2)
        .with_id("a1")
        .with_priority(7);
    let adapter = MySqlAdapter::from_config(&cfg, &ctx()).expect("from_config");
    assert_eq!((adapter.id(), adapter.priority()), ("a1", 7));
}

#[test]
fn the_factory_takes_id_and_priority_from_the_entry() {
    let raw = serde_json::json!({
        "type": "mysql", "id": "a1", "priority": 7, "config": { "url": "mysql://app@127.0.0.1:1/app", "pool_size": 2 },
    });
    let entry = CustomAdapterConfig::new("mysql", "a1", 7, raw);
    let adapter = factory(&entry, &ctx()).expect("factory");
    assert_eq!((adapter.id(), adapter.priority()), ("a1", 7));
}

#[test]
fn an_id_inside_the_config_block_is_not_the_adapter_id() {
    let mut block = serde_json::json!({ "url": "mysql://app@127.0.0.1:1/app", "pool_size": 2 });
    block["id"] = "inner".into();
    block["priority"] = 99.into();
    let cfg: MySqlConfig = serde_json::from_value(block).expect("config block");
    assert_eq!((cfg.id.as_str(), cfg.priority), ("", 0));
}

#[test]
fn from_config_rejects_an_empty_url() {
    let err = MySqlAdapter::from_config(&MySqlConfig::new("").with_id("a1"), &ctx())
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("`url` is empty"), "{err}");
}
