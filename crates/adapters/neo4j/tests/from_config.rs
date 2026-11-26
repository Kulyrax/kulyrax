//! `Neo4jAdapter::from_config` is the public constructor (decision
//! 2026-11-26-adapter-from-config). It builds the same adapter as the
//! `type: neo4j` factory, and validates the same way. No server is needed.

use std::sync::Arc;

use kulyrax_adapter_neo4j::{Neo4jAdapter, Neo4jConfig, factory};
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
    let cfg = Neo4jConfig::new("bolt://127.0.0.1:1", "neo4j", "secret")
        .with_database("neo4j")
        .with_id("a1")
        .with_priority(7);
    let adapter = Neo4jAdapter::from_config(&cfg, &ctx()).expect("from_config");
    assert_eq!((adapter.id(), adapter.priority()), ("a1", 7));
}

#[test]
fn the_factory_takes_id_and_priority_from_the_entry() {
    let raw = serde_json::json!({
        "type": "neo4j", "id": "a1", "priority": 7, "config": { "uri": "bolt://127.0.0.1:1", "user": "neo4j", "password": "secret", "database": "neo4j" },
    });
    let entry = CustomAdapterConfig::new("neo4j", "a1", 7, raw);
    let adapter = factory(&entry, &ctx()).expect("factory");
    assert_eq!((adapter.id(), adapter.priority()), ("a1", 7));
}

/// `id` and `priority` belong to the entry. Inside the `config:` block they
/// are unknown keys (D5, `deny_unknown_fields`).
#[test]
fn an_id_inside_the_config_block_is_refused() {
    for key in ["id", "priority"] {
        let mut block = serde_json::json!({ "uri": "bolt://127.0.0.1:1", "user": "neo4j", "password": "secret", "database": "neo4j" });
        block[key] = "x".into();
        let err = serde_json::from_value::<Neo4jConfig>(block)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(err.contains(&format!("unknown field `{key}`")), "{err}");
    }
}

#[test]
fn the_factory_refuses_a_misspelt_key_naming_it_and_the_adapter() {
    let mut block = serde_json::json!({ "uri": "bolt://127.0.0.1:1", "user": "neo4j", "password": "secret", "database": "neo4j" });
    block["pool_sise"] = 4.into();
    let raw = serde_json::json!({
        "type": "neo4j", "id": "a1", "priority": 7, "config": block,
    });
    let entry = CustomAdapterConfig::new("neo4j", "a1", 7, raw);
    let err = factory(&entry, &ctx())
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("unknown field `pool_sise`"), "{err}");
    assert!(err.contains("\"a1\""), "{err}");
}

#[test]
fn from_config_rejects_an_empty_uri() {
    let err = Neo4jAdapter::from_config(
        &Neo4jConfig::new("", "neo4j", "secret").with_id("a1"),
        &ctx(),
    )
    .err()
    .map(|e| e.to_string())
    .unwrap_or_default();
    assert!(err.contains("`uri` is empty"), "{err}");
}
