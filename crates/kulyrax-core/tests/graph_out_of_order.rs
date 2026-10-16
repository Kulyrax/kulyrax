//! Phase 16.6: edge upsert events delivered `3, 1, 2` for one edge key →
//! the edge's projected properties are the sequence-3 state, in every arrival
//! order. Same `decide()` gate as SQL / document / KV.

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

fn rated(seq: u64, score: &str) -> Event {
    Event {
        event_id: format!("evt-rated-{seq}"),
        event_type: "UserRated".to_string(),
        payload: format!(r#"{{ "from": "u1", "to": "m1", "score": "{score}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn mappings() -> HashMap<String, GraphMapping> {
    let mut m = HashMap::new();
    m.insert(
        "UserRated".to_string(),
        serde_yaml::from_str::<GraphMapping>(
            "kind: edge\nevent: UserRated\nedge_type: RATED\nversion: 1\nfrom: payload.from\nto: payload.to\non_existing: replace\nproperties:\n  score: payload.score\n",
        )
        .unwrap(),
    );
    m
}

fn run(order: &[Event]) -> String {
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
    let edges_dir = dir.path().join("edges").join("RATED");
    let file = std::fs::read_dir(&edges_dir)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let v: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(file).unwrap()).unwrap();
    v["score"].as_str().unwrap().to_string()
}

#[test]
fn edge_converges_to_highest_sequence_regardless_of_order() {
    assert_eq!(run(&[rated(3, "s3"), rated(1, "s1"), rated(2, "s2")]), "s3");
    assert_eq!(run(&[rated(1, "s1"), rated(2, "s2"), rated(3, "s3")]), "s3");
    assert_eq!(run(&[rated(2, "s2"), rated(3, "s3"), rated(1, "s1")]), "s3");
}
