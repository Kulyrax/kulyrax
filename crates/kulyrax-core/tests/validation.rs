//! Phase 8 validation tests.

use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::runtime::config::{
    AdapterCapability, AdapterConfig, FileAdapterConfig, FileConfig, KulyraxConfig, RoutingConfig,
    SqliteAdapterConfig, SqliteConfig,
};
use kulyrax_core::{
    ValidationIssue, ValidationReport, validate_projection_config,
    validate_routing_and_dependencies_for_event_type, validate_routing_for_event_type,
};

use std::collections::HashMap;

fn base_config() -> KulyraxConfig {
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
                capabilities: None,
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

fn sql_uc() -> SqlMapping {
    serde_yaml::from_str(
        r#"
event: UserCreated
table: users
primary_key: id
version: 1
columns:
  id: payload.id
"#,
    )
    .unwrap()
}

fn doc_uc() -> DocumentMapping {
    serde_yaml::from_str(
        r#"
event: UserCreated
collection: users
version: 1
id: payload.id
document:
  id: payload.id
"#,
    )
    .unwrap()
}

#[test]
fn validate_routing_for_event_ignores_unrelated_routes() {
    let config = base_config();
    config.validate().unwrap();
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    routing.insert("CacheInvalidated".to_string(), vec!["kv-cache".into()]);
    validate_routing_for_event_type(&config, &routing, "UserCreated").unwrap();
}

#[test]
fn rejects_unknown_adapter_in_routing() {
    let config = base_config();
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["ghost-adapter".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("ghost-adapter"));
}

#[test]
fn rejects_missing_sql_mapping_for_routed_sqlite() {
    let config = base_config();
    config.validate().unwrap();
    let sql = HashMap::new();
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("no SQL mapping"));
}

#[test]
fn rejects_missing_document_mapping() {
    let config = base_config();
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    let doc = HashMap::new();
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("no document mapping"));
}

#[test]
fn rejects_capability_mismatch() {
    let mut config = base_config();
    if let AdapterConfig::Sqlite(ref mut s) = config.adapters[0] {
        s.capabilities = None;
    }
    config.validate().unwrap();
    let mut sm = sql_uc();
    sm.requires_capabilities = vec![AdapterCapability::Idempotent];
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sm);
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("idempotent"));
}

#[test]
fn accepts_idempotent_when_adapter_declares_it() {
    let mut config = base_config();
    if let AdapterConfig::Sqlite(ref mut s) = config.adapters[0] {
        s.capabilities = Some(vec![
            AdapterCapability::Write,
            AdapterCapability::Idempotent,
        ]);
    }
    config.validate().unwrap();
    let mut sm = sql_uc();
    sm.requires_capabilities = vec![AdapterCapability::Idempotent];
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sm);
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap();
}

#[test]
fn accepts_empty_requires_capabilities() {
    let config = base_config();
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap();
}

#[test]
fn rejects_unrouted_sql_mapping() {
    let config = base_config();
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("OrderPlaced".to_string(), sql_uc());
    sql.get_mut("OrderPlaced").unwrap().event = "OrderPlaced".into();
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("OrderPlaced") || r.to_string().contains("never routed"));
}

#[test]
fn rejects_sql_mapping_when_route_has_only_file_adapters() {
    let config = KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "r.json".into(),
        },
        adapters: vec![AdapterConfig::File(FileAdapterConfig {
            id: "doc-readmodel".into(),
            priority: 10,
            config: FileConfig {
                root: "/tmp".into(),
            },
            capabilities: None,
            depends_on: vec![],
        })],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    };
    config.validate().unwrap();
    let mut sql = HashMap::new();
    let mut sm = sql_uc();
    sm.event = "OnlyDoc".into();
    sql.insert("OnlyDoc".to_string(), sm);
    let mut doc = HashMap::new();
    let mut dm = doc_uc();
    dm.event = "OnlyDoc".into();
    doc.insert("OnlyDoc".to_string(), dm);
    let mut routing = HashMap::new();
    routing.insert("OnlyDoc".to_string(), vec!["doc-readmodel".into()]);
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("no sqlite adapter") || r.to_string().contains("SQL mapping"));
}

#[test]
fn rejects_document_mapping_without_file_on_route() {
    let config = KulyraxConfig {
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
            capabilities: None,
            depends_on: vec![],
        })],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    };
    config.validate().unwrap();
    let mut sql = HashMap::new();
    let mut sm = sql_uc();
    sm.event = "SqlOnly".into();
    sql.insert("SqlOnly".to_string(), sm);
    let mut doc = HashMap::new();
    let mut dm = doc_uc();
    dm.event = "SqlOnly".into();
    doc.insert("SqlOnly".to_string(), dm);
    let mut routing = HashMap::new();
    routing.insert("SqlOnly".to_string(), vec!["sql-primary".into()]);
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("file adapter") || r.to_string().contains("document mapping"));
}

#[test]
fn rejects_mapping_key_mismatch_sql() {
    let config = base_config();
    config.validate().unwrap();
    let mut sm = sql_uc();
    sm.event = "UserCreated".into();
    let mut sql = HashMap::new();
    sql.insert("WrongKey".to_string(), sm);
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("map key"));
    assert!(r.to_string().contains("WrongKey"));
}

#[test]
fn rejects_duplicate_adapter_on_same_route() {
    let config = base_config();
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec![
            "sql-primary".into(),
            "sql-primary".into(),
            "doc-readmodel".into(),
        ],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("more than once"));
    assert!(r.to_string().contains("sql-primary"));
}

#[test]
fn rejects_duplicate_adapter_ids_in_config() {
    let config = KulyraxConfig {
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
                capabilities: None,
                depends_on: vec![],
            }),
            AdapterConfig::Sqlite(SqliteAdapterConfig {
                id: "sql-primary".into(),
                priority: 11,
                config: SqliteConfig {
                    path: ":memory:".into(),
                },
                capabilities: None,
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
    };
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("duplicate adapter id"));
}

#[test]
fn rejects_empty_document_template_object() {
    let config = base_config();
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    let mut dm = doc_uc();
    dm.document = serde_json::json!({});
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), dm);
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("empty") || r.to_string().contains("{}"));
}

#[test]
fn rejects_sql_mapping_with_zero_version() {
    let config = base_config();
    config.validate().unwrap();
    let mut sm = sql_uc();
    sm.version = 0;
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sm);
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("version must be >= 1"));
}

#[test]
fn rejects_document_mapping_with_zero_version() {
    let config = base_config();
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    let mut dm = doc_uc();
    dm.version = 0;
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), dm);
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("version must be >= 1"));
}

#[test]
fn rejects_dependency_not_on_route() {
    let mut config = base_config();
    if let AdapterConfig::File(ref mut f) = config.adapters[1] {
        f.depends_on = vec!["sql-primary".into()];
    }
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert("UserCreated".to_string(), vec!["doc-readmodel".into()]);
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("not on this route"));
}

#[test]
fn rejects_dependency_cycle_adapters_sorted() {
    let mut config = base_config();
    if let AdapterConfig::Sqlite(ref mut s) = config.adapters[0] {
        s.depends_on = vec!["doc-readmodel".into()];
    }
    if let AdapterConfig::File(ref mut f) = config.adapters[1] {
        f.depends_on = vec!["sql-primary".into()];
    }
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    let ValidationReport(issues) = r;
    let cycle = issues
        .iter()
        .find_map(|i| match i {
            ValidationIssue::DependencyCycle { adapters, .. } => Some(adapters.clone()),
            _ => None,
        })
        .expect("cycle issue");
    assert_eq!(cycle, vec!["doc-readmodel", "sql-primary"]);
}

#[test]
fn validate_routing_and_dependencies_returns_execution_order() {
    let mut config = base_config();
    if let AdapterConfig::File(ref mut f) = config.adapters[1] {
        f.depends_on = vec!["sql-primary".into()];
    }
    config.validate().unwrap();
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["doc-readmodel".into(), "sql-primary".into()],
    );
    let order =
        validate_routing_and_dependencies_for_event_type(&config, &routing, "UserCreated").unwrap();
    assert_eq!(order, vec!["sql-primary", "doc-readmodel"]);
}

#[test]
fn config_rejects_self_dependency() {
    let mut config = base_config();
    if let AdapterConfig::Sqlite(ref mut s) = config.adapters[0] {
        s.depends_on = vec!["sql-primary".into()];
    }
    let e = config.validate().unwrap_err();
    assert!(e.to_string().contains("itself") || e.to_string().contains("depend"));
}

#[test]
fn config_rejects_unknown_dependency() {
    let mut config = base_config();
    if let AdapterConfig::Sqlite(ref mut s) = config.adapters[0] {
        s.depends_on = vec!["ghost".into()];
    }
    let e = config.validate().unwrap_err();
    assert!(e.to_string().contains("ghost") || e.to_string().contains("unknown"));
}

// ------------------------------------------------------------
// Phase 11.1: entity identity resolution — validated at startup.
// ------------------------------------------------------------

#[test]
fn rejects_sql_primary_key_not_in_columns() {
    let config = base_config();
    config.validate().unwrap();
    let mut sm = sql_uc();
    sm.primary_key = "ghost_pk".into();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sm);
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("ghost_pk"));
    assert!(r.to_string().contains("not found in columns"));
}

#[test]
fn rejects_document_mapping_with_empty_id() {
    let config = base_config();
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    let mut dm = doc_uc();
    dm.id = "".into();
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), dm);
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("empty id"));
}

#[test]
fn rejects_sql_composite_primary_key_with_one_missing_column() {
    use kulyrax_core::adapter::sql::mapping::OneOrMany;

    let config = base_config();
    config.validate().unwrap();
    let mut sm = sql_uc();
    sm.primary_key = OneOrMany::Many(vec!["id".into(), "ghost_col".into()]);
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sm);
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("ghost_col"));
    assert!(r.to_string().contains("not found in columns"));
}

#[test]
fn rejects_document_mapping_with_id_not_a_payload_or_metadata_path() {
    let config = base_config();
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    let mut dm = doc_uc();
    dm.id = "id".into();
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), dm);
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(r.to_string().contains("'payload.' or 'metadata.' path"));
}

#[test]
fn phase15_rejects_delete_on_a_named_facet() {
    let config = base_config();
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    sql.insert(
        "UserEmailCleared".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            "event: UserEmailCleared\ntable: users\nprimary_key: id\nversion: 1\nfacet: contact\noperation: delete\ncolumns:\n  id: payload.id\n",
        )
        .unwrap(),
    );
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    routing.insert("UserEmailCleared".to_string(), vec!["sql-primary".into()]);
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(
        r.to_string().contains("delete is default-facet only"),
        "{r}"
    );
}

#[test]
fn phase15_rejects_facet_identity_that_differs_from_the_entity() {
    let config = base_config();
    config.validate().unwrap();
    let mut sql = HashMap::new();
    sql.insert("UserCreated".to_string(), sql_uc());
    sql.insert(
        "UserEmailChanged".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            "event: UserEmailChanged\ntable: users\nprimary_key: email\nversion: 1\nfacet: contact\ncolumns:\n  email: payload.email\n  note: payload.note\n",
        )
        .unwrap(),
    );
    let mut doc = HashMap::new();
    doc.insert("UserCreated".to_string(), doc_uc());
    let mut routing = HashMap::new();
    routing.insert(
        "UserCreated".to_string(),
        vec!["sql-primary".into(), "doc-readmodel".into()],
    );
    routing.insert("UserEmailChanged".to_string(), vec!["sql-primary".into()]);
    let r = validate_projection_config(
        &config,
        &routing,
        &sql,
        &doc,
        &HashMap::new(),
        &HashMap::new(),
    )
    .unwrap_err();
    assert!(
        r.to_string()
            .contains("differs from the entity's primary key"),
        "{r}"
    );
}
