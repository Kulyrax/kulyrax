//! G0, F-01b (task 2026-11-28-g0-schema-and-edge-labels, decision
//! 2026-11-28-edge-labels): an edge names each end's label (`from_label`,
//! `to_label`) and is written with `MERGE (a:<from_label> {id})
//! MERGE (b:<to_label> {id})`, so it attaches only to nodes of those labels,
//! and an edge that arrives first creates labelled placeholders that the real
//! nodes merge into. Before G0 the endpoints were matched with no label.
//!
//! Also the edge-without-properties failure, through the subgraph example in
//! `docs/mapping-reference.md`.
//!
//! **Gated on `KULYRAX_NEO4J_URI`.** Each test leases the whole database.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use kulyrax_adapter_neo4j::Neo4jAdapter;
use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::runtime::GraphRuntimeBuilder;
use kulyrax_core::adapter::{AdapterOutcome, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

type R = Result<(), Box<dyn std::error::Error>>;

fn mapping(yaml: &str) -> GraphMapping {
    kulyrax_core::yaml::from_str(yaml).expect("mapping")
}

fn adapter(uri: &str, mappings: HashMap<String, GraphMapping>) -> Neo4jAdapter {
    Neo4jAdapter::new(
        "neo4j".into(),
        uri,
        &common::user(),
        &common::password(),
        None,
        10,
        GraphRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

/// `UserCreated` / `OrgCreated` → `(:User)` / `(:Org)`, `OrgDeleted` deletes
/// an `Org`, `Followed` → `(:User)-[:FOLLOWS]->(:User)`.
fn users_and_orgs(uri: &str) -> Neo4jAdapter {
    let node = |event: &str, label: &str| {
        mapping(&format!(
            "kind: node\nevent: {event}\nlabel: {label}\nkey: payload.id\nversion: 1\nproperties:\n  name: payload.name\n"
        ))
    };
    adapter(
        uri,
        HashMap::from([
            ("UserCreated".to_string(), node("UserCreated", "User")),
            ("OrgCreated".to_string(), node("OrgCreated", "Org")),
            (
                "OrgDeleted".to_string(),
                mapping(
                    "kind: node\nevent: OrgDeleted\nlabel: Org\nkey: payload.id\nversion: 1\noperation: delete\n",
                ),
            ),
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

fn handle(a: &Neo4jAdapter, e: &Event) -> AdapterOutcome {
    let o = a.handle(e).outcome;
    assert!(
        !matches!(o, AdapterOutcome::Failed(_)),
        "{}: {o:?}",
        e.event_id
    );
    o
}

fn user(id: &str, name: &str) -> Event {
    ev(
        &format!("c-{id}"),
        "UserCreated",
        1,
        serde_json::json!({ "id": id, "name": name }),
    )
}

fn follow() -> Event {
    ev(
        "f1",
        "Followed",
        1,
        serde_json::json!({ "from": "u1", "to": "u2", "via": "web" }),
    )
}

fn count(uri: &str, cypher: &str) -> Result<i64, Box<dyn std::error::Error>> {
    let rt = tokio::runtime::Runtime::new()?;
    rt.block_on(async {
        let graph = neo4rs::Graph::new(uri, common::user(), common::password()).await?;
        let mut rows = graph
            .execute(neo4rs::Query::new(cypher.to_string()))
            .await?;
        let row = rows.next().await?.ok_or("no row")?;
        Ok(row.get::<i64>("n")?)
    })
}

/// An edge whose event arrives before its nodes' (producer contract §7,
/// "permissive: resolves once the node arrives") creates `User`
/// placeholders, and the real nodes merge into them: one node each, with
/// their properties, the edge between them, no unlabelled node.
#[test]
fn an_edge_before_its_nodes_joins_them() -> R {
    let Some(lease) = common::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return Ok(());
    };
    let uri = lease.uri();
    let a = users_and_orgs(uri);
    a.prepare()?;
    assert!(matches!(handle(&a, &follow()), AdapterOutcome::Created));
    assert_eq!(count(uri, "MATCH (n:User) RETURN count(n) AS n")?, 2);
    handle(&a, &user("u1", "Ada"));
    handle(&a, &user("u2", "Grace"));

    assert_eq!(
        count(
            uri,
            "MATCH (:User {id: 'u1', name: 'Ada'})-[r:FOLLOWS]->(:User {id: 'u2', name: 'Grace'}) \
             RETURN count(r) AS n",
        )?,
        1,
        "the edge between the real users"
    );
    assert_eq!(
        count(uri, "MATCH (n:User) RETURN count(n) AS n")?,
        2,
        "placeholders merged"
    );
    assert_eq!(
        count(
            uri,
            "MATCH (n) WHERE size(labels(n)) = 0 RETURN count(n) AS n"
        )?,
        0,
        "unlabelled nodes"
    );
    Ok(())
}

/// Two nodes share an id under different labels (`User u1`, `Org u1`). The
/// edge meant for the user does not touch the organisation, and deleting the
/// organisation leaves it.
#[test]
fn an_edge_does_not_attach_to_another_labels_node() -> R {
    let Some(lease) = common::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return Ok(());
    };
    let uri = lease.uri();
    let a = users_and_orgs(uri);
    a.prepare()?;
    handle(&a, &user("u1", "Ada"));
    handle(
        &a,
        &ev(
            "o1",
            "OrgCreated",
            1,
            serde_json::json!({ "id": "u1", "name": "Acme" }),
        ),
    );
    handle(&a, &user("u2", "Grace"));
    handle(&a, &follow());
    let from = |label: &str| {
        count(
            uri,
            &format!("MATCH (:{label} {{id: 'u1'}})-[r:FOLLOWS]->() RETURN count(r) AS n"),
        )
    };
    assert_eq!((from("Org")?, from("User")?), (0, 1), "(Org, User)");

    handle(
        &a,
        &ev("d1", "OrgDeleted", 2, serde_json::json!({ "id": "u1" })),
    );
    assert_eq!(from("User")?, 1, "after deleting Org u1");
    Ok(())
}

/// The subgraph example in `docs/mapping-reference.md`, whose edge has no
/// `properties`: it failed before G0.
#[test]
fn the_docs_subgraph_example_projects() -> R {
    let Some(lease) = common::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return Ok(());
    };
    let uri = lease.uri();
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
    let a = adapter(uri, HashMap::from([("OrderPlaced".to_string(), placed)]));
    a.prepare()?;
    let o = handle(
        &a,
        &ev(
            "p1",
            "OrderPlaced",
            1,
            serde_json::json!({ "order_id": "o1", "total": 42, "customer_id": "c1", "customer_name": "Ada" }),
        ),
    );
    assert!(matches!(o, AdapterOutcome::Created), "{o:?}");
    assert_eq!(
        count(
            uri,
            "MATCH (:Customer {id: 'c1'})-[r:PLACED]->(:Order {id: 'o1'}) RETURN count(r) AS n"
        )?,
        1
    );
    Ok(())
}
