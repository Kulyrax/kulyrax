//! Phase 14.5: `invalidate permanent: true` at sequence 2, then a `set` at
//! sequence 5 (which would otherwise resurrect the key) → `Skipped(Tombstoned)`.

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

fn set_event(sequence: u64, state: &str) -> Event {
    Event::new(
        format!("evt-set-{sequence}"),
        "UserViewUpdated".to_string(),
        sequence,
        format!(r#"{{ "user_id": "u1", "state": "{state}" }}"#),
    )
}

fn invalidate_event(sequence: u64) -> Event {
    Event::new(
        format!("evt-invalidate-{sequence}"),
        "CacheInvalidated".to_string(),
        sequence,
        r#"{ "user_id": "u1" }"#.to_string(),
    )
}

#[test]
fn kv_permanent_tombstone_rejects_later_resurrection_attempt()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let mut mappings = HashMap::new();
    mappings.insert(
        "UserViewUpdated".to_string(),
        kulyrax_core::yaml::from_str::<KvMapping>(
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
        kulyrax_core::yaml::from_str::<KvMapping>(
            r#"
event: CacheInvalidated
namespace: user-view
version: 1
key: payload.user_id
operation: delete
permanent: true
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

    store.handle(&set_event(1, "s1"));
    let r2 = store.handle(&invalidate_event(2));
    assert!(
        matches!(r2.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r2.outcome
    );

    let r3 = store.handle(&set_event(5, "s5"));
    assert!(
        matches!(r3.outcome, AdapterOutcome::Skipped(SkipReason::Tombstoned)),
        "expected Skipped(Tombstoned), got {:?}",
        r3.outcome
    );

    assert!(!dir.path().join("user-view").join("u1.json").exists());

    Ok(())
}
