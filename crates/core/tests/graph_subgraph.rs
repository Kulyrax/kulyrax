//! `kind: subgraph`: one event projects several graph records (here an
//! order node, its customer node, and the edge between them), each with its
//! own guard lane, applied together.

use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::runtime::GraphRuntimeBuilder;
use kulyrax_core::adapter::graph::store::GraphStore;
use kulyrax_core::adapter::keyvalue::mapping::KvMapping;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::{
    AdapterCapability, AdapterConfig, GraphAdapterConfig, GraphConfig, KulyraxConfig,
    MigrationPolicy, RoutingConfig,
};
use kulyrax_core::upcast::UpcasterRegistry;
use kulyrax_core::validate_projection_config;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

const ORDER_PLACED: &str = "kind: subgraph
event: OrderPlaced
version: 1
records:
  - kind: node
    label: Order
    key: payload.order_id
    on_existing: replace
    properties:
      total: payload.total
  - kind: node
    label: Customer
    key: payload.customer_id
    properties:
      name: payload.customer
  - kind: edge
    edge_type: PLACED
    from: payload.customer_id
    to: payload.order_id
    properties:
      at: payload.at
";

fn placed(seq: u64, order: &str, customer: &str) -> Event {
    Event {
        event_id: format!("evt-{order}-{seq}"),
        event_type: "OrderPlaced".into(),
        payload: serde_json::json!({
            "order_id": order,
            "customer_id": customer,
            "customer": format!("{customer} name"),
            "total": seq * 10,
            "at": format!("t{seq}"),
        })
        .to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn mapping() -> GraphMapping {
    serde_yaml::from_str(ORDER_PLACED).unwrap()
}

fn store(root: &Path) -> GraphStore {
    GraphStore::new(
        "graph".into(),
        root.to_path_buf(),
        10,
        GraphRuntimeBuilder::new(HashMap::from([("OrderPlaced".to_string(), mapping())])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn read(path: impl AsRef<Path>) -> Option<serde_json::Value> {
    std::fs::read_to_string(path)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
}

fn count(dir: impl AsRef<Path>) -> usize {
    std::fs::read_dir(dir).map(|d| d.count()).unwrap_or(0)
}

#[test]
fn one_event_projects_every_record() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let g = store(root);

    let r = g.handle(&placed(1, "o1", "c1"));
    assert!(
        matches!(r.outcome, AdapterOutcome::Created),
        "{:?}",
        r.outcome
    );
    assert_eq!(
        read(root.join("nodes/Order/o1.json")).map(|v| v["total"].clone()),
        Some(10.into())
    );
    assert_eq!(
        read(root.join("nodes/Customer/c1.json")).map(|v| v["name"].clone()),
        Some("c1 name".into())
    );
    assert_eq!(count(root.join("edges/PLACED")), 1);
    // A guard lane per record.
    assert!(root.join(".kulyrax/guard/nodes/Order/o1.json").exists());
    assert!(root.join(".kulyrax/guard/nodes/Customer/c1.json").exists());
    assert_eq!(count(root.join(".kulyrax/guard/edges/PLACED")), 1);

    // Redelivered: every record is stale.
    let again = g.handle(&placed(1, "o1", "c1"));
    assert!(
        matches!(
            again.outcome,
            AdapterOutcome::Skipped(SkipReason::StaleSequence)
        ),
        "{:?}",
        again.outcome
    );

    // A second order by the same customer: the order and its edge are new,
    // the customer (`on_existing: ignore`) is kept as first written.
    let second = g.handle(&placed(2, "o2", "c1"));
    assert!(
        matches!(second.outcome, AdapterOutcome::Created),
        "{:?}",
        second.outcome
    );
    assert_eq!(count(root.join("nodes/Order")), 2);
    assert_eq!(count(root.join("nodes/Customer")), 1);
    assert_eq!(count(root.join("edges/PLACED")), 2);

    // A newer event for o1 updates the order and the edge.
    let update = g.handle(&placed(3, "o1", "c1"));
    assert!(
        matches!(update.outcome, AdapterOutcome::Updated),
        "{:?}",
        update.outcome
    );
    assert_eq!(
        read(root.join("nodes/Order/o1.json")).map(|v| v["total"].clone()),
        Some(30.into())
    );
}

/// A record that cannot be resolved fails the event, and none of its
/// records is written, not even those before it.
#[test]
fn a_failing_record_writes_nothing_of_the_event() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    let g = store(root);
    let mut event = placed(1, "o1", "c1");
    // The edge's `at` is missing; the two nodes before it resolve.
    event.payload = serde_json::json!({
        "order_id": "o1", "customer_id": "c1", "customer": "c", "total": 1
    })
    .to_string();

    let r = g.handle(&event);
    assert!(
        matches!(r.outcome, AdapterOutcome::Failed(_)),
        "{:?}",
        r.outcome
    );
    assert!(!root.join("nodes").exists(), "no node written");
    assert!(!root.join(".kulyrax/guard").exists(), "no guard written");
}

#[test]
fn records_take_event_and_version_from_the_subgraph() {
    let GraphMapping::Subgraph(s) = mapping() else {
        panic!("a subgraph mapping");
    };
    assert_eq!(s.records.len(), 3);
    assert!(
        s.records
            .iter()
            .all(|r| r.event() == "OrderPlaced" && r.version() == 1)
    );

    let err = |yaml: &str| {
        serde_yaml::from_str::<GraphMapping>(yaml)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default()
    };
    let own_event = ORDER_PLACED.replacen("    label: Order", "    label: Order\n    event: X", 1);
    assert!(
        err(&own_event).contains("records[0]: `event` belongs to the subgraph"),
        "{}",
        err(&own_event)
    );
    let nested =
        "kind: subgraph\nevent: E\nversion: 1\nrecords:\n  - kind: subgraph\n    records: []\n";
    assert!(
        err(nested).contains("records[0]: a subgraph's records are nodes and edges"),
        "{}",
        err(nested)
    );
}

fn config(capabilities: Vec<AdapterCapability>) -> KulyraxConfig {
    KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "r.json".into(),
        },
        adapters: vec![AdapterConfig::Graph(GraphAdapterConfig {
            id: "g".into(),
            priority: 10,
            config: GraphConfig {
                root: "/tmp".into(),
            },
            capabilities: Some(capabilities),
            depends_on: vec![],
        })],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    }
}

fn validate(config: &KulyraxConfig, mapping: GraphMapping) -> Result<(), String> {
    validate_projection_config(
        config,
        &HashMap::from([("OrderPlaced".to_string(), vec!["g".to_string()])]),
        &HashMap::<String, SqlMapping>::new(),
        &HashMap::<String, DocumentMapping>::new(),
        &HashMap::<String, KvMapping>::new(),
        &HashMap::from([("OrderPlaced".to_string(), mapping)]),
    )
    .map_err(|r| r.to_string())
}

#[test]
fn every_record_is_validated() {
    use AdapterCapability::{Upsert, Write};
    assert_eq!(validate(&config(vec![Write, Upsert]), mapping()), Ok(()));

    // The order node replaces: an adapter without `upsert` cannot take it.
    let err = validate(&config(vec![Write]), mapping()).unwrap_err();
    assert!(err.to_lowercase().contains("upsert"), "{err}");

    let empty_label = ORDER_PLACED.replacen("label: Customer", "label: ' '", 1);
    let err = validate(
        &config(vec![Write, Upsert]),
        serde_yaml::from_str(&empty_label).unwrap(),
    )
    .unwrap_err();
    assert!(err.contains("records[1]: empty label"), "{err}");

    let none = "kind: subgraph\nevent: OrderPlaced\nversion: 1\nrecords: []\n";
    let err = validate(
        &config(vec![Write, Upsert]),
        serde_yaml::from_str(none).unwrap(),
    )
    .unwrap_err();
    assert!(
        err.contains("a subgraph needs at least one record"),
        "{err}"
    );
}
