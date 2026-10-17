//! Phase 15.6 (15.1): two facets of one table both claiming the `email`
//! column → `validate_projection_config` fails at startup (they would race
//! their independent sequence gates).

use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::runtime::config::{
    KulyraxConfig, RoutingConfig, SqliteAdapterConfig, SqliteConfig,
};
use kulyrax_core::validate_projection_config;

use std::collections::HashMap;

fn config() -> KulyraxConfig {
    KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "r.json".into(),
        },
        adapters: vec![kulyrax_core::runtime::config::AdapterConfig::Sqlite(
            SqliteAdapterConfig {
                id: "sql-primary".into(),
                priority: 10,
                config: SqliteConfig {
                    path: ":memory:".into(),
                },
                capabilities: Some(vec![
                    kulyrax_core::runtime::config::AdapterCapability::Write,
                    kulyrax_core::runtime::config::AdapterCapability::Upsert,
                ]),
                depends_on: vec![],
            },
        )],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    }
}

fn sql(event: &str, facet: &str, col: &str) -> SqlMapping {
    serde_yaml::from_str(&format!(
        r#"
event: {event}
table: users
primary_key: id
version: 1
facet: {facet}
columns:
  id: payload.id
  {col}: payload.{col}
"#
    ))
    .unwrap()
}

#[test]
fn two_facets_claiming_the_same_column_is_rejected() {
    let config = config();
    config.validate().unwrap();

    let mut sqlm = HashMap::new();
    sqlm.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserCreated
table: users
primary_key: id
version: 1
columns:
  id: payload.id
  display_name: payload.display_name
"#,
        )
        .unwrap(),
    );
    sqlm.insert("A".to_string(), sql("A", "contact", "email"));
    sqlm.insert("B".to_string(), sql("B", "billing", "email"));

    let mut routing = HashMap::new();
    routing.insert("UserCreated".to_string(), vec!["sql-primary".into()]);
    routing.insert("A".to_string(), vec!["sql-primary".into()]);
    routing.insert("B".to_string(), vec!["sql-primary".into()]);

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
        err.to_string().contains("both claiming field \"email\""),
        "{err}"
    );
}
