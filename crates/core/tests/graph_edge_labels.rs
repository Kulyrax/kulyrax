//! G0 (F-01b in the file graph store, and the edge-without-properties
//! failure). A node is its label and its id, and an edge names both for each
//! end (`from_label`, `to_label`). The incidence index a node delete cascades
//! through is kept per `(label, id)`, so deleting one label's node never
//! touches another label's edges, and an edge that arrives before its nodes
//! is indexed under the labels the nodes will have.
//!
//! Decision `2026-11-28-edge-labels`.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::runtime::GraphRuntimeBuilder;
use kulyrax_core::adapter::graph::store::GraphStore;
use kulyrax_core::adapter::{AdapterOutcome, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

fn mapping(yaml: &str) -> GraphMapping {
    kulyrax_core::yaml::from_str(yaml).expect("mapping")
}

fn store(root: &Path, mappings: HashMap<String, GraphMapping>) -> GraphStore {
    GraphStore::new(
        "graph".into(),
        root.to_path_buf(),
        10,
        GraphRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn users_and_orgs(root: &Path) -> GraphStore {
    let node = |event: &str, label: &str| {
        mapping(&format!(
            "kind: node\nevent: {event}\nlabel: {label}\nkey: payload.id\nversion: 1\nproperties:\n  name: payload.name\n"
        ))
    };
    let delete = |event: &str, label: &str| {
        mapping(&format!(
            "kind: node\nevent: {event}\nlabel: {label}\nkey: payload.id\nversion: 1\noperation: delete\n"
        ))
    };
    store(
        root,
        HashMap::from([
            ("UserCreated".to_string(), node("UserCreated", "User")),
            ("UserDeleted".to_string(), delete("UserDeleted", "User")),
            ("OrgCreated".to_string(), node("OrgCreated", "Org")),
            ("OrgDeleted".to_string(), delete("OrgDeleted", "Org")),
            (
                "Followed".to_string(),
                mapping(
                    "kind: edge\nevent: Followed\nedge_type: FOLLOWS\nfrom_label: User\nto_label: User\nversion: 1\nfrom: payload.from\nto: payload.to\nproperties:\n  via: payload.via\n",
                ),
            ),
        ]),
    )
}

fn ev(id: &str, event_type: &str, seq: u64, payload: serde_json::Value) -> Event {
    Event::new(id, event_type, seq, payload.to_string())
}

fn handle(s: &GraphStore, e: &Event) -> AdapterOutcome {
    let o = s.handle(e).outcome;
    assert!(
        !matches!(o, AdapterOutcome::Failed(_)),
        "{}: {o:?}",
        e.event_id
    );
    o
}

/// The edge files under `edges/<edge_type>/`.
fn edges(root: &Path, edge_type: &str) -> usize {
    std::fs::read_dir(root.join("edges").join(edge_type))
        .map(|d| d.filter_map(Result::ok).count())
        .unwrap_or(0)
}

fn user(id: &str, seq: u64) -> Event {
    ev(
        &format!("u-{id}-{seq}"),
        "UserCreated",
        seq,
        serde_json::json!({ "id": id, "name": id }),
    )
}

fn follow(seq: u64) -> Event {
    ev(
        "f1",
        "Followed",
        seq,
        serde_json::json!({ "from": "u1", "to": "u2", "via": "web" }),
    )
}

/// `User u1` follows `User u2`; deleting `Org u1`, another label's node with
/// the same id, leaves that edge alone (it deleted it before G0).
#[test]
fn deleting_another_labels_node_keeps_the_edge() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = users_and_orgs(dir.path());
    s.prepare().expect("prepare");
    handle(&s, &user("u1", 1));
    handle(&s, &user("u2", 1));
    handle(
        &s,
        &ev(
            "o1",
            "OrgCreated",
            1,
            serde_json::json!({ "id": "u1", "name": "Acme" }),
        ),
    );
    handle(&s, &follow(1));
    assert_eq!(edges(dir.path(), "FOLLOWS"), 1);

    handle(
        &s,
        &ev("d1", "OrgDeleted", 2, serde_json::json!({ "id": "u1" })),
    );
    assert_eq!(edges(dir.path(), "FOLLOWS"), 1, "after deleting Org u1");

    // The edge's own endpoint still cascades.
    handle(
        &s,
        &ev("d2", "UserDeleted", 2, serde_json::json!({ "id": "u1" })),
    );
    assert_eq!(edges(dir.path(), "FOLLOWS"), 0, "after deleting User u1");
}

/// An edge that arrives before its nodes is indexed under the mapped labels
/// (the file store's placeholder); the real nodes then own it, so deleting
/// one of them deletes it, while a same-id node of another label does not.
#[test]
fn an_edge_before_its_nodes_joins_them_by_label() {
    let dir = tempfile::tempdir().expect("tempdir");
    let s = users_and_orgs(dir.path());
    s.prepare().expect("prepare");
    assert!(matches!(handle(&s, &follow(1)), AdapterOutcome::Created));
    let incident = dir.path().join(".kulyrax").join("incident");
    for id in ["u1", "u2"] {
        assert!(
            incident.join("User").join(format!("{id}.json")).exists(),
            "{id}"
        );
    }
    assert!(!incident.join("Org").exists());

    handle(&s, &user("u1", 2));
    handle(&s, &user("u2", 2));
    handle(
        &s,
        &ev(
            "o1",
            "OrgCreated",
            2,
            serde_json::json!({ "id": "u1", "name": "Acme" }),
        ),
    );
    handle(
        &s,
        &ev("d1", "OrgDeleted", 3, serde_json::json!({ "id": "u1" })),
    );
    assert_eq!(edges(dir.path(), "FOLLOWS"), 1, "after deleting Org u1");
    handle(
        &s,
        &ev("d2", "UserDeleted", 3, serde_json::json!({ "id": "u2" })),
    );
    assert_eq!(edges(dir.path(), "FOLLOWS"), 0, "after deleting User u2");
}

/// The subgraph example in `docs/mapping-reference.md`, whose edge has no
/// `properties`. Before G0 the edge's properties resolved to `null`: this
/// store wrote `null` as the edge's file, and Neo4j failed the event.
#[test]
fn the_docs_subgraph_example_projects() {
    let dir = tempfile::tempdir().expect("tempdir");
    let placed = mapping(
        "kind: subgraph
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
      name: payload.customer_name
  - kind: edge
    edge_type: PLACED
    from_label: Customer
    to_label: Order
    from: payload.customer_id
    to: payload.order_id
",
    );
    let s = store(
        dir.path(),
        HashMap::from([("OrderPlaced".to_string(), placed)]),
    );
    s.prepare().expect("prepare");
    let o = handle(
        &s,
        &ev(
            "p1",
            "OrderPlaced",
            1,
            serde_json::json!({ "order_id": "o1", "total": 42, "customer_id": "c1", "customer_name": "Ada" }),
        ),
    );
    assert!(matches!(o, AdapterOutcome::Created), "{o:?}");
    let files: Vec<_> = std::fs::read_dir(dir.path().join("edges").join("PLACED"))
        .expect("edges")
        .filter_map(Result::ok)
        .collect();
    assert_eq!(files.len(), 1);
    let value: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(files[0].path()).expect("edge"))
            .expect("json");
    assert_eq!(value, serde_json::json!({}), "an edge with no properties");
}

/// An edge mapping without `from_label` or `to_label` does not parse.
#[test]
fn an_edge_without_labels_is_refused() {
    for missing in ["from_label", "to_label"] {
        let yaml = "kind: edge\nevent: E\nedge_type: T\nfrom_label: A\nto_label: B\nversion: 1\nfrom: payload.a\nto: payload.b\n"
            .lines()
            .filter(|l| !l.starts_with(missing))
            .collect::<Vec<_>>()
            .join("\n");
        let err = kulyrax_core::yaml::from_str::<GraphMapping>(&yaml)
            .err()
            .unwrap_or_else(|| panic!("parsed without {missing}"));
        assert!(err.to_string().contains(missing), "{missing}: {err}");
    }
}

fn graph_marker(root: &Path) -> std::path::PathBuf {
    root.join(".kulyrax").join("layout").join("graph")
}

/// A graph store an earlier version wrote (marker 2) that holds edges is
/// refused (exit 4): their keys lack the endpoints' labels, which were never
/// stored, so only a replay can rebuild them. Nothing is re-stamped.
#[test]
fn an_earlier_graph_store_with_edges_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let s = users_and_orgs(dir.path());
        s.prepare().expect("prepare");
        handle(&s, &follow(1));
    }
    std::fs::write(graph_marker(dir.path()), "2\n").expect("marker");

    let s = users_and_orgs(dir.path());
    let err = s.prepare().expect_err("an earlier store with edges opened");
    assert!(
        matches!(err, kulyrax_core::adapter::AdapterError::Format(_)),
        "{err:?}"
    );
    let msg = err.to_string();
    assert!(msg.contains("Clear the store and replay"), "{msg}");
    assert_eq!(
        std::fs::read_to_string(graph_marker(dir.path())).expect("marker"),
        "2\n"
    );
}

/// The same store with nodes only opens, and is re-stamped to the current
/// format in place.
#[test]
fn an_earlier_graph_store_with_nodes_only_is_restamped() {
    let dir = tempfile::tempdir().expect("tempdir");
    {
        let s = users_and_orgs(dir.path());
        s.prepare().expect("prepare");
        handle(&s, &user("u1", 1));
    }
    std::fs::write(graph_marker(dir.path()), "2\n").expect("marker");

    let s = users_and_orgs(dir.path());
    s.prepare().expect("prepare");
    assert_eq!(
        std::fs::read_to_string(graph_marker(dir.path())).expect("marker"),
        "3\n"
    );
    assert!(matches!(
        s.handle(&user("u1", 1)).outcome,
        AdapterOutcome::Skipped(_)
    ));
}
