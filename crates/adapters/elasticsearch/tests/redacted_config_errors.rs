//! A elasticsearch URL holding a password and a secret query parameter never shows
//! either one: not in the factory's error (Display or Debug), not in the
//! config's Debug.

use std::sync::Arc;

use kulyrax_adapter_elasticsearch::{ElasticsearchConfig, factory};
use kulyrax_core::runtime::AdapterBuildContext;
use kulyrax_core::runtime::config::{CustomAdapterConfig, MigrationPolicy};
use kulyrax_core::upcast::UpcasterRegistry;

const PASSWORD: &str = "Hunter2Secret";
const TOKEN: &str = "TokenS3cretValue";

/// Unparseable on purpose (the port), so the factory reports it.
fn url() -> String {
    format!("http://app:{PASSWORD}@db.internal:54x32/app?password={TOKEN}&apiKey={TOKEN}")
}

fn config() -> serde_json::Value {
    serde_json::json!({ "url": url(), "password": PASSWORD })
}

fn assert_hidden(what: &str, text: &str) {
    for secret in [PASSWORD, TOKEN] {
        assert!(!text.contains(secret), "{what} shows a secret: {text}");
    }
}

#[test]
fn the_factory_error_hides_the_secrets() {
    let raw = serde_json::to_value(serde_json::json!({
        "type": "elasticsearch", "id": "x", "priority": 10, "config": config(),
    }))
    .expect("raw");
    let entry = CustomAdapterConfig {
        type_name: "elasticsearch".into(),
        id: "x".into(),
        priority: 10,
        raw,
        capabilities: None,
        depends_on: Vec::new(),
    };
    let ctx = AdapterBuildContext {
        mappings: Default::default(),
        upcasters: Arc::new(UpcasterRegistry::new()),
        migration_policy: MigrationPolicy::default(),
    };
    let err = factory(&entry, &ctx)
        .err()
        .expect("an unparseable url is refused");
    let display = err.to_string();
    assert!(
        display.contains("does not parse") && display.contains("***"),
        "{display}"
    );
    assert_hidden("Display", &display);
    assert_hidden("Debug", &format!("{err:?}"));
}

#[test]
fn the_config_debug_hides_the_secrets() {
    let cfg: ElasticsearchConfig = serde_json::from_value(config()).expect("config");
    let debug = format!("{cfg:?}");
    assert!(debug.contains("***"), "{debug}");
    assert_hidden("Debug", &debug);
}
