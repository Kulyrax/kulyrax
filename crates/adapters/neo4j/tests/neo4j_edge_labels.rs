//! F-01b (task 2026-11-25-neo4j-edge-endpoints): the Neo4j adapter writes an
//! edge with `MERGE (a {id: $edge_from}) MERGE (b {id: $edge_to})`, with no
//! label, while it writes nodes as `MERGE (n:<Label> {id: …})`.
//!
//! These tests reproduce the two consequences. They are expected to FAIL until
//! it is fixed.
//!
//! **Gated on `KULYRAX_NEO4J_URI`.** Each test leases the whole database.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use kulyrax_adapter_neo4j::Neo4jAdapter;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::runtime::GraphRuntimeBuilder;
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

type R = Result<(), Box<dyn std::error::Error>>;

fn mapping(yaml: &str) -> GraphMapping {
    kulyrax_core::yaml::from_str(yaml).expect("mapping")
}

/// `UserCreated` → `(:User)`, `OrgCreated` → `(:Org)`, `Followed` → an edge
/// `FOLLOWS` from `payload.from` to `payload.to` (with a property, so these
/// tests reach the endpoint `MERGE`s), `Placed` → an edge `PLACED` with no
/// `properties` (optional, per mapping-reference).
fn adapter(uri: &str) -> Neo4jAdapter {
    let mappings = HashMap::from([
        (
            "UserCreated".to_string(),
            mapping(
                "kind: node\nevent: UserCreated\nlabel: User\nkey: payload.id\nversion: 1\nproperties:\n  name: payload.name\n",
            ),
        ),
        (
            "OrgCreated".to_string(),
            mapping(
                "kind: node\nevent: OrgCreated\nlabel: Org\nkey: payload.id\nversion: 1\nproperties:\n  name: payload.name\n",
            ),
        ),
        (
            "Followed".to_string(),
            mapping(
                "kind: edge\nevent: Followed\nedge_type: FOLLOWS\nversion: 1\nfrom: payload.from\nto: payload.to\nproperties:\n  via: payload.via\n",
            ),
        ),
        (
            "Placed".to_string(),
            mapping(
                "kind: edge\nevent: Placed\nedge_type: PLACED\nversion: 1\nfrom: payload.from\nto: payload.to\n",
            ),
        ),
    ]);
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

fn ev(id: &str, event_type: &str, seq: u64, payload: serde_json::Value) -> Event {
    Event::new(id, event_type, seq, payload.to_string())
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

/// An edge whose event arrives before its nodes' (producer contract §7 calls
/// this "permissive: resolves once the node arrives"): it should end up
/// between the two `User` nodes, with no other nodes left behind.
#[test]
fn an_edge_before_its_nodes_joins_them() -> R {
    let Some(lease) = common::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return Ok(());
    };
    let uri = lease.uri();
    let a = adapter(uri);
    a.prepare()?;
    for e in [
        ev(
            "f1",
            "Followed",
            1,
            serde_json::json!({ "from": "u1", "to": "u2", "via": "web" }),
        ),
        ev(
            "c1",
            "UserCreated",
            1,
            serde_json::json!({ "id": "u1", "name": "Ada" }),
        ),
        ev(
            "c2",
            "UserCreated",
            1,
            serde_json::json!({ "id": "u2", "name": "Grace" }),
        ),
    ] {
        let o = a.handle(&e).outcome;
        eprintln!("{}: {o:?}", e.event_id);
    }
    let joined = count(
        uri,
        "MATCH (:User {id: 'u1'})-[r:FOLLOWS]->(:User {id: 'u2'}) RETURN count(r) AS n",
    )?;
    let unlabelled = count(
        uri,
        "MATCH (n) WHERE size(labels(n)) = 0 RETURN count(n) AS n",
    )?;
    eprintln!("edges between the User nodes: {joined}; nodes with no label: {unlabelled}");
    assert_eq!(
        (joined, unlabelled),
        (1, 0),
        "(edges between the users, label-less nodes)"
    );
    Ok(())
}

/// Two nodes share an id under different labels (`User u1`, `Org u1`). An
/// edge meant for the user should not touch the organisation.
#[test]
fn an_edge_does_not_attach_to_another_labels_node() -> R {
    let Some(lease) = common::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return Ok(());
    };
    let uri = lease.uri();
    let a = adapter(uri);
    a.prepare()?;
    for e in [
        ev(
            "c1",
            "UserCreated",
            1,
            serde_json::json!({ "id": "u1", "name": "Ada" }),
        ),
        ev(
            "o1",
            "OrgCreated",
            1,
            serde_json::json!({ "id": "u1", "name": "Acme" }),
        ),
        ev(
            "c2",
            "UserCreated",
            1,
            serde_json::json!({ "id": "u2", "name": "Grace" }),
        ),
        ev(
            "f1",
            "Followed",
            1,
            serde_json::json!({ "from": "u1", "to": "u2", "via": "web" }),
        ),
    ] {
        let o = a.handle(&e).outcome;
        eprintln!("{}: {o:?}", e.event_id);
    }
    let from_org = count(
        uri,
        "MATCH (:Org {id: 'u1'})-[r:FOLLOWS]->() RETURN count(r) AS n",
    )?;
    let from_user = count(
        uri,
        "MATCH (:User {id: 'u1'})-[r:FOLLOWS]->() RETURN count(r) AS n",
    )?;
    eprintln!("FOLLOWS from Org u1: {from_org}; from User u1: {from_user}");
    assert_eq!(
        (from_org, from_user),
        (0, 1),
        "(from the Org, from the User)"
    );
    Ok(())
}

/// An edge mapping without `properties` (optional for edges, per
/// mapping-reference, and so written in its own subgraph example) should
/// project its edge.
#[test]
fn an_edge_without_properties_is_written() -> R {
    let Some(lease) = common::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return Ok(());
    };
    let uri = lease.uri();
    let a = adapter(uri);
    a.prepare()?;
    let mut outcomes = Vec::new();
    for e in [
        ev(
            "c1",
            "UserCreated",
            1,
            serde_json::json!({ "id": "u1", "name": "Ada" }),
        ),
        ev(
            "c2",
            "UserCreated",
            1,
            serde_json::json!({ "id": "u2", "name": "Grace" }),
        ),
        ev(
            "p1",
            "Placed",
            1,
            serde_json::json!({ "from": "u1", "to": "u2" }),
        ),
    ] {
        let o = a.handle(&e).outcome;
        eprintln!("{}: {o:?}", e.event_id);
        outcomes.push(format!("{o:?}"));
    }
    let placed = count(uri, "MATCH ()-[r:PLACED]->() RETURN count(r) AS n")?;
    assert_eq!(placed, 1, "outcomes: {outcomes:?}");
    Ok(())
}
