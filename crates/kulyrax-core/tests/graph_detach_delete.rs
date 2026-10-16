//! Phase 16.4: DETACH DELETE. A node `u1` with three incident edges, then
//! `deregister u1 s5` → the node **and all three edges** are absent, every
//! edge lane tombstoned at seq 5. A later `follow(u1, x) s7` (a fresh edge to
//! the now-deleted node) is `Skipped(EntityAbsent)` — you cannot attach to a
//! tombstoned node.

use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::runtime::GraphRuntimeBuilder;
use kulyrax_core::adapter::graph::store::GraphStore;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

fn registered(seq: u64, id: &str) -> Event {
    Event {
        event_id: format!("evt-reg-{id}-{seq}"),
        event_type: "UserRegistered".to_string(),
        payload: format!(r#"{{ "user_id": "{id}", "name": "{id}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn followed(seq: u64, from: &str, to: &str) -> Event {
    Event {
        event_id: format!("evt-follow-{from}-{to}-{seq}"),
        event_type: "UserFollowed".to_string(),
        payload: format!(r#"{{ "follower": "{from}", "followee": "{to}", "ts": "t{seq}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn deregistered(seq: u64, id: &str) -> Event {
    Event {
        event_id: format!("evt-dereg-{id}-{seq}"),
        event_type: "UserDeregistered".to_string(),
        payload: format!(r#"{{ "user_id": "{id}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn mappings() -> HashMap<String, GraphMapping> {
    let mut m = HashMap::new();
    m.insert(
        "UserRegistered".to_string(),
        serde_yaml::from_str::<GraphMapping>(
            "kind: node\nevent: UserRegistered\nlabel: User\nversion: 1\nkey: payload.user_id\non_existing: replace\nproperties:\n  name: payload.name\n",
        )
        .unwrap(),
    );
    m.insert(
        "UserDeregistered".to_string(),
        serde_yaml::from_str::<GraphMapping>(
            "kind: node\nevent: UserDeregistered\nlabel: User\nversion: 1\nkey: payload.user_id\noperation: delete\n",
        )
        .unwrap(),
    );
    m.insert(
        "UserFollowed".to_string(),
        serde_yaml::from_str::<GraphMapping>(
            "kind: edge\nevent: UserFollowed\nedge_type: FOLLOWS\nversion: 1\nfrom: payload.follower\nto: payload.followee\non_existing: replace\nproperties:\n  since: payload.ts\n",
        )
        .unwrap(),
    );
    m
}

#[test]
fn detach_delete_removes_all_incident_edges() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let store = GraphStore::new(
        "graph".to_string(),
        dir.path().to_path_buf(),
        10,
        GraphRuntimeBuilder::new(mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    for id in ["u1", "u2", "u3", "u4"] {
        store.handle(&registered(1, id));
    }
    store.handle(&followed(2, "u1", "u2"));
    store.handle(&followed(3, "u3", "u1"));
    store.handle(&followed(4, "u1", "u4"));
    assert_eq!(
        std::fs::read_dir(dir.path().join("edges").join("FOLLOWS"))?.count(),
        3
    );

    let del = store.handle(&deregistered(5, "u1"));
    assert!(
        matches!(del.outcome, AdapterOutcome::Deleted),
        "{:?}",
        del.outcome
    );

    assert!(
        !dir.path()
            .join("nodes")
            .join("User")
            .join("u1.json")
            .exists()
    );
    assert_eq!(
        std::fs::read_dir(dir.path().join("edges").join("FOLLOWS"))?.count(),
        0,
        "every incident edge must be detached"
    );
    // The surviving endpoints' incident indexes no longer reference u1's edges.
    for id in ["u2", "u3", "u4"] {
        let p = dir
            .path()
            .join(".conduit")
            .join("incident")
            .join(format!("{id}.json"));
        assert!(!p.exists(), "{id} incident index should be empty/removed");
    }

    // A fresh edge to the tombstoned node is refused.
    let after = store.handle(&followed(7, "u1", "u4"));
    assert!(
        matches!(
            after.outcome,
            AdapterOutcome::Skipped(SkipReason::EntityAbsent)
        ),
        "{:?}",
        after.outcome
    );

    Ok(())
}
