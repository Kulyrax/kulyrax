//! Phase 16.5: `operation: delete` on a graph mapping implies
//! `requires_capabilities: [delete]` — routing it to a `graph` adapter that
//! doesn't declare `AdapterCapability::Delete` fails validation at startup.
//! The same rule already enforced for SQL / document / key-value, now covering
//! the fourth storage kind.

use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::keyvalue::mapping::KvMapping;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::runtime::config::{
    AdapterCapability, AdapterConfig, GraphAdapterConfig, GraphConfig, KulyraxConfig, RoutingConfig,
};
use kulyrax_core::validate_projection_config;

use std::collections::HashMap;

fn config_with_graph_capabilities(capabilities: Option<Vec<AdapterCapability>>) -> KulyraxConfig {
    KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "r.json".into(),
        },
        adapters: vec![AdapterConfig::Graph(GraphAdapterConfig {
            id: "graph-social".into(),
            priority: 10,
            config: GraphConfig {
                root: "/tmp".into(),
            },
            capabilities,
            depends_on: vec![],
        })],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    }
}

fn delete_node_mapping() -> GraphMapping {
    serde_yaml::from_str(
        "kind: node\nevent: UserDeregistered\nlabel: User\nversion: 1\nkey: payload.user_id\noperation: delete\n",
    )
    .unwrap()
}

fn routing() -> HashMap<String, Vec<String>> {
    let mut r = HashMap::new();
    r.insert("UserDeregistered".to_string(), vec!["graph-social".into()]);
    r
}

fn empties() -> (
    HashMap<String, SqlMapping>,
    HashMap<String, DocumentMapping>,
    HashMap<String, KvMapping>,
) {
    (HashMap::new(), HashMap::new(), HashMap::new())
}

#[test]
fn delete_graph_mapping_on_non_delete_adapter_fails_validation() {
    let config = config_with_graph_capabilities(None);
    config.validate().unwrap();

    let mut graph = HashMap::new();
    graph.insert("UserDeregistered".to_string(), delete_node_mapping());
    let (sql, doc, kv) = empties();

    let err = validate_projection_config(&config, &routing(), &sql, &doc, &kv, &graph).unwrap_err();
    assert!(err.to_string().contains("delete"), "{err}");
}

#[test]
fn delete_graph_mapping_on_declared_delete_adapter_validates() {
    let config = config_with_graph_capabilities(Some(vec![
        AdapterCapability::Write,
        AdapterCapability::Delete,
    ]));
    config.validate().unwrap();

    let mut graph = HashMap::new();
    graph.insert("UserDeregistered".to_string(), delete_node_mapping());
    let (sql, doc, kv) = empties();

    validate_projection_config(&config, &routing(), &sql, &doc, &kv, &graph).unwrap();
}
