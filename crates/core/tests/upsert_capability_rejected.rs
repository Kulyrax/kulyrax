//! Phase 12.5: `on_existing: replace` implies `requires_capabilities: [upsert]`
//! for that mapping — routing it to an adapter that doesn't declare `Upsert`
//! fails validation at startup, not at runtime.

use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::runtime::config::{
    AdapterCapability, AdapterConfig, FileAdapterConfig, FileConfig, KulyraxConfig, RoutingConfig,
    SqliteAdapterConfig, SqliteConfig,
};
use kulyrax_core::validate_projection_config;

use std::collections::HashMap;

fn config_with_sqlite_capabilities(capabilities: Option<Vec<AdapterCapability>>) -> KulyraxConfig {
    KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "r.json".into(),
        },
        adapters: vec![
            AdapterConfig::Sqlite(SqliteAdapterConfig {
                id: "sql-primary".into(),
                priority: 10,
                config: SqliteConfig {
                    path: ":memory:".into(),
                },
                capabilities,
                depends_on: vec![],
            }),
            AdapterConfig::File(FileAdapterConfig {
                id: "doc-readmodel".into(),
                priority: 20,
                config: FileConfig {
                    root: "/tmp".into(),
                },
                capabilities: None,
                depends_on: vec![],
            }),
        ],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    }
}

fn replace_sql_mapping() -> SqlMapping {
    serde_yaml::from_str(
        r#"
event: UserUpdated
table: users
primary_key: id
version: 1
on_existing: replace
columns:
  id: payload.id
"#,
    )
    .unwrap()
}

fn ignore_doc_mapping() -> DocumentMapping {
    serde_yaml::from_str(
        r#"
event: UserUpdated
collection: users
version: 1
id: payload.id
document:
  id: payload.id
"#,
    )
    .unwrap()
}

fn routing() -> HashMap<String, Vec<String>> {
    let mut r = HashMap::new();
    r.insert(
        "UserUpdated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    r
}

#[test]
fn replace_mapping_on_non_upsert_adapter_fails_validation() {
    // No capabilities declared → effective capabilities default to {Write} only.
    let config = config_with_sqlite_capabilities(None);
    config.validate().unwrap();

    let mut sql = HashMap::new();
    sql.insert("UserUpdated".to_string(), replace_sql_mapping());
    let mut doc = HashMap::new();
    doc.insert("UserUpdated".to_string(), ignore_doc_mapping());

    let err = validate_projection_config(
        &config,
        &routing(),
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("upsert"),
        "expected an upsert capability mismatch: {err}"
    );
}

#[test]
fn replace_mapping_on_declared_upsert_adapter_validates() {
    let config = config_with_sqlite_capabilities(Some(vec![
        AdapterCapability::Write,
        AdapterCapability::Upsert,
    ]));
    config.validate().unwrap();

    let mut sql = HashMap::new();
    sql.insert("UserUpdated".to_string(), replace_sql_mapping());
    let mut doc = HashMap::new();
    doc.insert("UserUpdated".to_string(), ignore_doc_mapping());

    validate_projection_config(
        &config,
        &routing(),
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap();
}
