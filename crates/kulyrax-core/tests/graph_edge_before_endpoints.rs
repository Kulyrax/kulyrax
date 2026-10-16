//! Phase 16.3: the file-backed graph adapter is **permissive** about endpoint
//! ids — a `follow` whose endpoints were never projected as nodes still writes
//! the edge (dangling ids are a producer / enforcing-backend concern, 16.3).

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
        payload: r#"{ "follower": "ghost1", "followee": "ghost2", "ts": "t1" }"#.to_string(),
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
            "kind: edge\nevent: UserFollowed\nedge_type: FOLLOWS\nversion: 1\nfrom: payload.follower\nto: payload.followee\nproperties:\n  since: payload.ts\n",
        )
        .unwrap(),
    );
    m
}

#[test]
fn edge_written_even_when_endpoints_never_projected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let store = GraphStore::new(
        "graph".to_string(),
        dir.path().to_path_buf(),
        10,
        GraphRuntimeBuilder::new(mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let r = store.handle(&followed(1));
    assert!(
        matches!(r.outcome, AdapterOutcome::Created),
        "{:?}",
        r.outcome
    );

    let edges_dir = dir.path().join("edges").join("FOLLOWS");
    assert_eq!(std::fs::read_dir(&edges_dir)?.count(), 1);

    // Both endpoints have an incident index even though no node exists.
    for endpoint in ["ghost1", "ghost2"] {
        assert!(
            dir.path()
                .join(".conduit")
                .join("incident")
                .join(format!("{endpoint}.json"))
                .exists()
        );
    }

    Ok(())
}
