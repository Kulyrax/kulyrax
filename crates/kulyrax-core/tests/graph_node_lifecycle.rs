//! Phase 16.6: `[register s1, rename s2, delete s3]` on a graph **node** →
//! the node is absent and its guard is a tombstone recorded at sequence 3 —
//! the Phase 12/13 lifecycle, verbatim, on the fourth storage kind.

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

fn registered(seq: u64, name: &str) -> Event {
    Event {
        event_id: format!("evt-reg-{seq}"),
        event_type: "UserRegistered".to_string(),
        payload: format!(r#"{{ "user_id": "u1", "name": "{name}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn renamed(seq: u64, name: &str) -> Event {
    Event {
        event_id: format!("evt-rename-{seq}"),
        event_type: "UserRenamed".to_string(),
        payload: format!(r#"{{ "user_id": "u1", "name": "{name}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn deregistered(seq: u64) -> Event {
    Event {
        event_id: format!("evt-dereg-{seq}"),
        event_type: "UserDeregistered".to_string(),
        payload: r#"{ "user_id": "u1" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn mappings() -> HashMap<String, GraphMapping> {
    let mut m = HashMap::new();
    for (event, extra) in [
        ("UserRegistered", "on_existing: replace"),
        ("UserRenamed", "on_existing: replace"),
    ] {
        m.insert(
            event.to_string(),
            serde_yaml::from_str::<GraphMapping>(&format!(
                "kind: node\nevent: {event}\nlabel: User\nversion: 1\nkey: payload.user_id\n{extra}\nproperties:\n  name: payload.name\n"
            ))
            .unwrap(),
        );
    }
    m.insert(
        "UserDeregistered".to_string(),
        serde_yaml::from_str::<GraphMapping>(
            "kind: node\nevent: UserDeregistered\nlabel: User\nversion: 1\nkey: payload.user_id\noperation: delete\n",
        )
        .unwrap(),
    );
    m
}

#[test]
fn node_lifecycle_ends_absent_with_tombstone() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let store = GraphStore::new(
        "graph".to_string(),
        dir.path().to_path_buf(),
        10,
        GraphRuntimeBuilder::new(mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let r1 = store.handle(&registered(1, "Ada"));
    assert!(
        matches!(r1.outcome, AdapterOutcome::Created),
        "{:?}",
        r1.outcome
    );
    let r2 = store.handle(&renamed(2, "Ada L."));
    assert!(
        matches!(r2.outcome, AdapterOutcome::Updated),
        "{:?}",
        r2.outcome
    );
    let r3 = store.handle(&deregistered(3));
    assert!(
        matches!(r3.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r3.outcome
    );

    assert!(
        !dir.path()
            .join("nodes")
            .join("User")
            .join("u1.json")
            .exists()
    );

    let guard: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        dir.path()
            .join(".conduit")
            .join("guard")
            .join("nodes")
            .join("User")
            .join("u1.json"),
    )?)?;
    assert_eq!(guard["last_sequence"], 3);
    assert_eq!(guard["deleted"], true);

    Ok(())
}
