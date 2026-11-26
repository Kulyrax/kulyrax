//! Phase 14.4: `operation: delete` on a key-value mapping implies
//! `requires_capabilities: [delete]` — routing it to a `keyvalue` adapter
//! that doesn't declare `AdapterCapability::Delete` fails validation at
//! startup, not at runtime. Same rule Phase 13.5 already enforces for SQL
//! and document, now covering the third storage kind.

use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::keyvalue::mapping::KvMapping;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::runtime::config::{
    AdapterCapability, AdapterConfig, KeyValueAdapterConfig, KeyValueConfig, KulyraxConfig,
};
use kulyrax_core::validate_projection_config;

use std::collections::HashMap;

fn config_with_kv_capabilities(capabilities: Option<Vec<AdapterCapability>>) -> KulyraxConfig {
    KulyraxConfig::new(
        "r.json",
        vec![AdapterConfig::KeyValue({
            let mut c = KeyValueAdapterConfig::new("kv-cache", 10, KeyValueConfig::new("/tmp"));
            c.capabilities = capabilities;
            c
        })],
    )
}

fn delete_kv_mapping() -> KvMapping {
    kulyrax_core::yaml::from_str(
        r#"
event: CacheInvalidated
namespace: user-view
version: 1
key: payload.user_id
operation: delete
value: {}
"#,
    )
    .unwrap()
}

fn routing() -> HashMap<String, Vec<String>> {
    let mut r = HashMap::new();
    r.insert("CacheInvalidated".to_string(), vec!["kv-cache".into()]);
    r
}

#[test]
fn delete_kv_mapping_on_non_delete_adapter_fails_validation() {
    // No capabilities declared → effective capabilities default to {Write} only.
    let config = config_with_kv_capabilities(None);
    config.validate().unwrap();

    let mut kv = HashMap::new();
    kv.insert("CacheInvalidated".to_string(), delete_kv_mapping());
    let sql: HashMap<String, SqlMapping> = HashMap::new();
    let doc: HashMap<String, DocumentMapping> = HashMap::new();

    let err = validate_projection_config(&config, &routing(), &sql, &doc, &kv, &HashMap::new())
        .unwrap_err();
    assert!(
        err.to_string().contains("delete"),
        "expected a delete capability mismatch: {err}"
    );
}

#[test]
fn delete_kv_mapping_on_declared_delete_adapter_validates() {
    let config = config_with_kv_capabilities(Some(vec![
        AdapterCapability::Write,
        AdapterCapability::Delete,
    ]));
    config.validate().unwrap();

    let mut kv = HashMap::new();
    kv.insert("CacheInvalidated".to_string(), delete_kv_mapping());
    let sql: HashMap<String, SqlMapping> = HashMap::new();
    let doc: HashMap<String, DocumentMapping> = HashMap::new();

    validate_projection_config(&config, &routing(), &sql, &doc, &kv, &HashMap::new()).unwrap();
}
