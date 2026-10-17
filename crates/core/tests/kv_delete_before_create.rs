//! Phase 14.5: `invalidate s2` then `set s1` → key stays absent — the delete
//! writes a tombstone even with no prior value, so the lower-sequence set
//! that arrives after is rejected `SkipStale` (Phase 13.1/13.2, reused as-is).

use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::keyvalue::mapping::KvMapping;
use kulyrax_core::adapter::keyvalue::runtime::KvRuntimeBuilder;
use kulyrax_core::adapter::keyvalue::store::FileKvStore;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

fn set_event(sequence: u64) -> Event {
    Event {
        event_id: format!("evt-set-{sequence}"),
        event_type: "UserViewUpdated".to_string(),
        payload: r#"{ "user_id": "u1", "state": "s1" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

fn invalidate_event(sequence: u64) -> Event {
    Event {
        event_id: format!("evt-invalidate-{sequence}"),
        event_type: "CacheInvalidated".to_string(),
        payload: r#"{ "user_id": "u1" }"#.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

#[test]
fn kv_invalidate_then_set_leaves_key_absent() -> Result<(), Box<dyn std::error::Error>> {
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
    mappings.insert(
        "CacheInvalidated".to_string(),
        serde_yaml::from_str::<KvMapping>(
            r#"
event: CacheInvalidated
namespace: user-view
version: 1
key: payload.user_id
operation: delete
value: {}
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

    let r_invalidate = store.handle(&invalidate_event(2));
    assert!(
        matches!(r_invalidate.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r_invalidate.outcome
    );

    let r_set = store.handle(&set_event(1));
    assert!(
        matches!(
            r_set.outcome,
            AdapterOutcome::Skipped(SkipReason::StaleSequence)
        ),
        "expected Skipped(StaleSequence), got {:?}",
        r_set.outcome
    );

    assert!(!dir.path().join("user-view").join("u1.json").exists());

    Ok(())
}
