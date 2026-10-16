//! Phase 14.5: `[set s1, invalidate s2, set s3]` → key present again, at the
//! sequence-3 value (resurrection, Phase 13.4, reused as-is).

use kulyrax_core::adapter::AdapterOutcome;
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

#[test]
fn kv_resurrection_after_invalidate() -> Result<(), Box<dyn std::error::Error>> {
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

    let r1 = store.handle(&set_event(1, "s1"));
    assert!(
        matches!(r1.outcome, AdapterOutcome::Created),
        "{:?}",
        r1.outcome
    );
    let r2 = store.handle(&invalidate_event(2));
    assert!(
        matches!(r2.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r2.outcome
    );
    assert!(!dir.path().join("user-view").join("u1.json").exists());

    let r3 = store.handle(&set_event(3, "s3"));
    assert!(
        matches!(r3.outcome, AdapterOutcome::Created),
        "resurrection must be a Created outcome: {:?}",
        r3.outcome
    );

    let content = std::fs::read_to_string(dir.path().join("user-view").join("u1.json"))?;
    assert!(content.contains("\"s3\""), "{content}");

    Ok(())
}
