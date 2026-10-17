//! Phase 16.1/16.3: `discriminator` makes a multigraph. Two `RATED` edges with
//! the same `(from, to)` but different `discriminator` are two distinct edges;
//! the same `(from, to, discriminator)` is one edge (an upsert).

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

fn rated(seq: u64, rating_id: &str, score: &str) -> Event {
    Event {
        event_id: format!("evt-rated-{rating_id}-{seq}"),
        event_type: "UserRated".to_string(),
        payload: format!(
            r#"{{ "from": "u1", "to": "m1", "rating_id": "{rating_id}", "score": "{score}" }}"#
        ),
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
            "kind: edge\nevent: UserRated\nedge_type: RATED\nversion: 1\nfrom: payload.from\nto: payload.to\ndiscriminator: payload.rating_id\non_existing: replace\nproperties:\n  score: payload.score\n",
        )
        .unwrap(),
    );
    m
}

#[test]
fn discriminator_distinguishes_parallel_edges() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let store = GraphStore::new(
        "graph".to_string(),
        dir.path().to_path_buf(),
        10,
        GraphRuntimeBuilder::new(mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let a = store.handle(&rated(1, "r1", "5"));
    assert!(
        matches!(a.outcome, AdapterOutcome::Created),
        "{:?}",
        a.outcome
    );
    let b = store.handle(&rated(2, "r2", "3"));
    assert!(
        matches!(b.outcome, AdapterOutcome::Created),
        "{:?}",
        b.outcome
    );

    let edges_dir = dir.path().join("edges").join("RATED");
    assert_eq!(
        std::fs::read_dir(&edges_dir)?.count(),
        2,
        "different discriminators → two edges"
    );

    // Same (from, to, discriminator) → upsert of the existing edge.
    let c = store.handle(&rated(3, "r1", "4"));
    assert!(
        matches!(c.outcome, AdapterOutcome::Updated),
        "{:?}",
        c.outcome
    );
    assert_eq!(std::fs::read_dir(&edges_dir)?.count(), 2);

    // u1's incident index lists both parallel edges.
    let idx: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        dir.path().join(".conduit").join("incident").join("u1.json"),
    )?)?;
    assert_eq!(idx["edges"].as_array().unwrap().len(), 2);

    Ok(())
}
