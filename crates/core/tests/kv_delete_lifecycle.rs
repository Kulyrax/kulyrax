//! Phase 14.5: `[set s1, update s2, invalidate s3]` → key absent; the guard
//! sidecar is a tombstone recorded at sequence 3.

use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::keyvalue::mapping::KvMapping;
use kulyrax_core::adapter::keyvalue::runtime::KvRuntimeBuilder;
use kulyrax_core::adapter::keyvalue::store::FileKvStore;
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

fn set_event(sequence: u64, state: &str) -> Event {
    Event {
        event_id: format!("evt-set-{sequence}"),
        event_type: "UserViewUpdated".to_string(),
        payload: format!(r#"{{ "user_id": "u1", "state": "{state}" }}"#),
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

fn mappings() -> Result<HashMap<String, KvMapping>, Box<dyn std::error::Error>> {
    let mut m = HashMap::new();
    m.insert(
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
    m.insert(
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
    Ok(m)
}

#[test]
fn kv_full_lifecycle_ends_absent_with_tombstone() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;

    let store = FileKvStore::new(
        "kv-cache".to_string(),
        dir.path().to_path_buf(),
        10,
        KvRuntimeBuilder::new(mappings()?),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    store.handle(&set_event(1, "s1"));
    store.handle(&set_event(2, "s2"));
    let r3 = store.handle(&invalidate_event(3));
    assert!(
        matches!(r3.outcome, kulyrax_core::adapter::AdapterOutcome::Deleted),
        "{:?}",
        r3.outcome
    );

    assert!(
        !dir.path().join("user-view").join("u1.json").exists(),
        "key must be absent after invalidation"
    );

    let guard_content = std::fs::read_to_string(
        dir.path()
            .join(".conduit")
            .join("user-view")
            .join("u1.guard.json"),
    )?;
    let guard: serde_json::Value = serde_json::from_str(&guard_content)?;
    assert_eq!(guard["last_sequence"], 3);
    assert_eq!(guard["deleted"], true);

    Ok(())
}
