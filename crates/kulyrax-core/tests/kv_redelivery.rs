//! Phase 14.5: redelivering the same event twice → the second is
//! `Skipped(StaleSequence)`; the value file and guard sidecar are
//! byte-identical to after the first delivery.

use kulyrax_core::adapter::keyvalue::mapping::KvMapping;
use kulyrax_core::adapter::keyvalue::runtime::KvRuntimeBuilder;
use kulyrax_core::adapter::keyvalue::store::FileKvStore;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

fn user_view_updated() -> Event {
    Event {
        event_id: "evt-1".to_string(),
        event_type: "UserViewUpdated".to_string(),
        payload: r#"{ "user_id": "u1", "state": "s1" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 5,
    }
}

#[test]
fn kv_redelivery_is_stale_not_already_projected() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let mut mappings = HashMap::new();
    mappings.insert(
        "UserViewUpdated".to_string(),
        serde_yaml::from_str::<KvMapping>(
            r#"
event: UserViewUpdated
namespace: user-view
version: 1
key: payload.user_id
on_existing: replace
value:
  state: payload.state
"#,
        )?,
    );

    let store = FileKvStore::new(
        "kv-cache".to_string(),
        dir.path().to_path_buf(),
        10,
        KvRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let event = user_view_updated();

    let r1 = store.handle(&event);
    assert!(
        matches!(r1.outcome, AdapterOutcome::Created),
        "{:?}",
        r1.outcome
    );

    let value_path = dir.path().join("user-view").join("u1.json");
    let guard_path = dir
        .path()
        .join(".conduit")
        .join("user-view")
        .join("u1.guard.json");
    let value_before = std::fs::read_to_string(&value_path)?;
    let guard_before = std::fs::read_to_string(&guard_path)?;

    let r2 = store.handle(&event);
    assert!(
        matches!(
            r2.outcome,
            AdapterOutcome::Skipped(SkipReason::StaleSequence)
        ),
        "expected Skipped(StaleSequence), got {:?}",
        r2.outcome
    );

    assert_eq!(
        std::fs::read_to_string(&value_path)?,
        value_before,
        "value must be untouched"
    );
    assert_eq!(
        std::fs::read_to_string(&guard_path)?,
        guard_before,
        "guard must be untouched"
    );

    Ok(())
}
