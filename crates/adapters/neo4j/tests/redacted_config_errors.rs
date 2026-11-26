//! A Neo4j password (its own field, or in the URI) never shows: not in the
//! factory's error, not in the config's Debug.

use std::sync::Arc;

use kulyrax_adapter_neo4j::{Neo4jConfig, factory};
use kulyrax_core::runtime::AdapterBuildContext;
use kulyrax_core::runtime::config::{CustomAdapterConfig, MigrationPolicy};
use kulyrax_core::upcast::UpcasterRegistry;

const PASSWORD: &str = "Hunter2Secret";
const TOKEN: &str = "TokenS3cretValue";

fn config() -> serde_json::Value {
    serde_json::json!({
        "uri": format!("bolt://app:{PASSWORD}@db.internal:76x87?token={TOKEN}"),
        "user": "neo4j",
        "password": PASSWORD,
    })
}

fn assert_hidden(what: &str, text: &str) {
    for secret in [PASSWORD, TOKEN] {
        assert!(!text.contains(secret), "{what} shows a secret: {text}");
    }
}

#[test]
fn the_factory_error_hides_the_secrets() {
    let raw = serde_json::to_value(serde_json::json!({
        "type": "neo4j", "id": "x", "priority": 10, "config": config(),
    }))
    .expect("raw");
    let entry = CustomAdapterConfig::new("neo4j", "x", 10, raw);
    let ctx = AdapterBuildContext::new(
        Default::default(),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );
    let err = factory(&entry, &ctx)
        .err()
        .expect("an unparseable uri is refused");
    assert_hidden("Display", &err.to_string());
    assert_hidden("Debug", &format!("{err:?}"));
}

#[test]
fn the_config_debug_hides_the_secrets() {
    let cfg: Neo4jConfig = serde_json::from_value(config()).expect("config");
    let debug = format!("{cfg:?}");
    assert!(debug.contains("***"), "{debug}");
    assert_hidden("Debug", &debug);
}
