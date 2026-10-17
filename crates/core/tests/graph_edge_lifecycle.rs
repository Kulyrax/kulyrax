//! Phase 16.6: `[follow s1, unfollow s2]` → the edge is absent and its guard
//! is a tombstone at sequence 2.

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

fn followed(seq: u64) -> Event {
    Event {
        event_id: format!("evt-follow-{seq}"),
        event_type: "UserFollowed".to_string(),
        payload: r#"{ "follower": "u1", "followee": "u2", "ts": "t1" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn unfollowed(seq: u64) -> Event {
    Event {
        event_id: format!("evt-unfollow-{seq}"),
        event_type: "UserUnfollowed".to_string(),
        payload: r#"{ "follower": "u1", "followee": "u2" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn mappings() -> HashMap<String, GraphMapping> {
    let mut m = HashMap::new();
    m.insert(
        "UserFollowed".to_string(),
        serde_yaml::from_str::<GraphMapping>(
            "kind: edge\nevent: UserFollowed\nedge_type: FOLLOWS\nversion: 1\nfrom: payload.follower\nto: payload.followee\non_existing: replace\nproperties:\n  since: payload.ts\n",
        )
        .unwrap(),
    );
    m.insert(
        "UserUnfollowed".to_string(),
        serde_yaml::from_str::<GraphMapping>(
            "kind: edge\nevent: UserUnfollowed\nedge_type: FOLLOWS\nversion: 1\nfrom: payload.follower\nto: payload.followee\noperation: delete\n",
        )
        .unwrap(),
    );
    m
}

#[test]
fn edge_lifecycle_ends_absent() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let store = GraphStore::new(
        "graph".to_string(),
        dir.path().to_path_buf(),
        10,
        GraphRuntimeBuilder::new(mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let r1 = store.handle(&followed(1));
    assert!(
        matches!(r1.outcome, AdapterOutcome::Created),
        "{:?}",
        r1.outcome
    );
    let r2 = store.handle(&unfollowed(2));
    assert!(
        matches!(r2.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r2.outcome
    );

    let edges_dir = dir.path().join("edges").join("FOLLOWS");
    let remaining: Vec<_> = std::fs::read_dir(&edges_dir)
        .map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.path()).collect())
        .unwrap_or_default();
    assert!(remaining.is_empty(), "{remaining:?}");

    // The u1 incident index is now empty → removed.
    assert!(
        !dir.path()
            .join(".conduit")
            .join("incident")
            .join("u1.json")
            .exists()
    );

    Ok(())
}
