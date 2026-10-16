//! Phase 16 + Phase 15: a node *is* an entity, so facets apply verbatim. A
//! `User` node with a `profile` facet (seq 5) and a `stats` facet (seq 6),
//! delivered in both orders → both land; neither facet's lane sees the other's
//! sequence.

use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::runtime::GraphRuntimeBuilder;
use kulyrax_core::adapter::graph::store::GraphStore;
use kulyrax_core::adapter::{AdapterOutcome, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

fn registered(seq: u64) -> Event {
    Event {
        event_id: format!("evt-reg-{seq}"),
        event_type: "UserRegistered".to_string(),
        payload: r#"{ "user_id": "u1", "name": "Ada" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn profile_set(seq: u64, bio: &str) -> Event {
    Event {
        event_id: format!("evt-profile-{seq}"),
        event_type: "ProfileUpdated".to_string(),
        payload: format!(r#"{{ "user_id": "u1", "bio": "{bio}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn stats_set(seq: u64, followers: i64) -> Event {
    Event {
        event_id: format!("evt-stats-{seq}"),
        event_type: "StatsRecomputed".to_string(),
        payload: format!(r#"{{ "user_id": "u1", "followers": {followers} }}"#),
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
            "kind: node\nevent: UserRegistered\nlabel: User\nversion: 1\nkey: payload.user_id\nproperties:\n  name: payload.name\n",
        )
        .unwrap(),
    );
    m.insert(
        "ProfileUpdated".to_string(),
        serde_yaml::from_str::<GraphMapping>(
            "kind: node\nevent: ProfileUpdated\nlabel: User\nversion: 1\nkey: payload.user_id\nfacet: profile\nproperties:\n  bio: payload.bio\n",
        )
        .unwrap(),
    );
    m.insert(
        "StatsRecomputed".to_string(),
        serde_yaml::from_str::<GraphMapping>(
            "kind: node\nevent: StatsRecomputed\nlabel: User\nversion: 1\nkey: payload.user_id\nfacet: stats\nproperties:\n  followers: payload.followers\n",
        )
        .unwrap(),
    );
    m
}

fn run(order: &[Event]) -> serde_json::Value {
    let dir = tempdir().unwrap();
    let store = GraphStore::new(
        "graph".to_string(),
        dir.path().to_path_buf(),
        10,
        GraphRuntimeBuilder::new(mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );
    for e in order {
        let r = store.handle(e);
        assert!(
            !matches!(r.outcome, AdapterOutcome::Failed(_)),
            "{:?}",
            r.outcome
        );
    }
    serde_json::from_str(
        &std::fs::read_to_string(dir.path().join("nodes").join("User").join("u1.json")).unwrap(),
    )
    .unwrap()
}

#[test]
fn node_facets_land_regardless_of_order() {
    let a = run(&[registered(1), profile_set(5, "hi"), stats_set(6, 42)]);
    let b = run(&[registered(1), stats_set(6, 42), profile_set(5, "hi")]);

    assert_eq!(a["name"], "Ada");
    assert_eq!(a["bio"], "hi");
    assert_eq!(a["followers"], 42);
    assert_eq!(a, b);
}
