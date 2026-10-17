//! Phase 15.6 (15.1): a faceted table that also has a mapping using
//! `on_existing: replace` → `validate_projection_config` fails at startup.
//! Whole-entity replace and facets are mutually exclusive — a full replace at
//! sequence N would clobber facet columns whose lanes are at a higher sequence.

use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::runtime::config::{
    AdapterCapability, AdapterConfig, KulyraxConfig, RoutingConfig, SqliteAdapterConfig,
    SqliteConfig,
};
use kulyrax_core::validate_projection_config;

use std::collections::HashMap;

fn config() -> KulyraxConfig {
    KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "r.json".into(),
        },
        adapters: vec![AdapterConfig::Sqlite(SqliteAdapterConfig {
            id: "sql-primary".into(),
            priority: 10,
            config: SqliteConfig {
                path: ":memory:".into(),
            },
            capabilities: Some(vec![AdapterCapability::Write, AdapterCapability::Upsert]),
            depends_on: vec![],
        })],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    }
}

#[test]
fn faceted_table_with_replace_mapping_is_rejected() {
    let config = config();
    config.validate().unwrap();

    let mut sqlm = HashMap::new();
    // Whole-entity replace on `users`...
    sqlm.insert(
        "UserUpserted".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserUpserted
table: users
primary_key: id
version: 1
on_existing: replace
columns:
  id: payload.id
  display_name: payload.display_name
"#,
        )
        .unwrap(),
    );
    // ...and a facet on the same table.
    sqlm.insert(
        "UserEmailChanged".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserEmailChanged
table: users
primary_key: id
version: 1
facet: contact
columns:
  id: payload.id
  email: payload.email
"#,
        )
        .unwrap(),
    );

    let mut routing = HashMap::new();
    routing.insert("UserUpserted".to_string(), vec!["sql-primary".into()]);
    routing.insert("UserEmailChanged".to_string(), vec!["sql-primary".into()]);

    let err = validate_projection_config(
        &config,
        &routing,
        &sqlm,
        &HashMap::new(),
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(
        err.to_string()
            .contains("is faceted, so no mapping may use 'on_existing: replace'"),
        "{err}"
    );
}
