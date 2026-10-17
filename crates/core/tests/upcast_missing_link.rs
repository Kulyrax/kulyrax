//! Phase 10.4: fail-fast error reporting when an upcaster chain is broken.

use kulyrax_core::upcast::{UpcastError, Upcaster, UpcasterRegistry};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Counts invocations so a broken chain further along can be proven to never run.
struct CountingUpcaster {
    version: u32,
    calls: Arc<AtomicUsize>,
}

impl Upcaster for CountingUpcaster {
    fn event_type(&self) -> &str {
        "UserCreated"
    }
    fn source_version(&self) -> u32 {
        self.version
    }
    fn upcast(&self, payload: Value) -> Result<Value, String> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(payload)
    }
}

#[test]
fn missing_final_link_fails_fast_before_running_earlier_steps() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = UpcasterRegistry::new();
    // v1 -> v2 registered; v2 -> v3 is missing.
    registry.register(Box::new(CountingUpcaster {
        version: 1,
        calls: Arc::clone(&calls),
    }));

    let err = registry
        .upcast("UserCreated", json!({ "id": "u1" }), 1, 3)
        .unwrap_err();

    match err {
        UpcastError::MissingLink {
            event_type,
            from_version,
            to_version,
            missing_at,
        } => {
            assert_eq!(event_type, "UserCreated");
            assert_eq!(from_version, 1);
            assert_eq!(to_version, 3);
            assert_eq!(missing_at, 2);
        }
        other => panic!("expected MissingLink, got {:?}", other),
    }

    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "no upcaster should run when the chain is broken further along"
    );
}

#[test]
fn missing_first_link_fails_fast_with_no_calls() {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = UpcasterRegistry::new();
    // Only v2 -> v3 registered; v1 -> v2 (the first hop) is missing.
    registry.register(Box::new(CountingUpcaster {
        version: 2,
        calls: Arc::clone(&calls),
    }));

    let err = registry
        .upcast("UserCreated", json!({ "id": "u1" }), 1, 3)
        .unwrap_err();

    assert!(matches!(
        err,
        UpcastError::MissingLink { missing_at: 1, .. }
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[test]
fn completely_unregistered_event_type_fails_fast() {
    let registry = UpcasterRegistry::new();

    let err = registry
        .upcast("NeverRegistered", json!({}), 1, 2)
        .unwrap_err();

    assert!(matches!(
        err,
        UpcastError::MissingLink { missing_at: 1, .. }
    ));
}

#[test]
fn resolve_chain_surfaces_the_same_error_without_applying_anything() {
    let mut registry = UpcasterRegistry::new();
    registry.register(Box::new(CountingUpcaster {
        version: 1,
        calls: Arc::new(AtomicUsize::new(0)),
    }));

    // resolve_chain is the pre-flight check dispatch/replay can use before
    // committing to a batch; it must fail the same way `upcast` does.
    let err = registry.resolve_chain("UserCreated", 1, 4).unwrap_err();
    assert!(matches!(
        err,
        UpcastError::MissingLink { missing_at: 2, .. }
    ));
}

#[test]
fn downcast_request_is_rejected_without_consulting_the_registry() {
    // Even a fully-populated registry must refuse an event newer than the
    // mapping's target version; downcasting is never valid.
    let calls = Arc::new(AtomicUsize::new(0));
    let mut registry = UpcasterRegistry::new();
    registry.register(Box::new(CountingUpcaster {
        version: 1,
        calls: Arc::clone(&calls),
    }));

    let err = registry
        .upcast("UserCreated", json!({ "id": "u1" }), 3, 1)
        .unwrap_err();

    assert!(matches!(err, UpcastError::Downcast { .. }));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}
