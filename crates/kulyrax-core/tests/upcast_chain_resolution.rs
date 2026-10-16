//! Phase 10.4: multi-step upcaster chain resolution (V1 -> V2 -> V3).

use kulyrax_core::upcast::{Upcaster, UpcasterRegistry};
use serde_json::{Value, json};

/// v1 -> v2: a bare `{"id"}` payload gains a default `email`.
struct AddEmail;

impl Upcaster for AddEmail {
    fn event_type(&self) -> &str {
        "UserCreated"
    }
    fn source_version(&self) -> u32 {
        1
    }
    fn upcast(&self, payload: Value) -> Result<Value, String> {
        let mut obj = payload
            .as_object()
            .cloned()
            .ok_or_else(|| "payload must be an object".to_string())?;
        obj.entry("email".to_string())
            .or_insert_with(|| json!("unknown@example.com"));
        Ok(Value::Object(obj))
    }
}

/// v2 -> v3: gains a default `tier`.
struct AddTier;

impl Upcaster for AddTier {
    fn event_type(&self) -> &str {
        "UserCreated"
    }
    fn source_version(&self) -> u32 {
        2
    }
    fn upcast(&self, payload: Value) -> Result<Value, String> {
        let mut obj = payload
            .as_object()
            .cloned()
            .ok_or_else(|| "payload must be an object".to_string())?;
        obj.entry("tier".to_string())
            .or_insert_with(|| json!("standard"));
        Ok(Value::Object(obj))
    }
}

/// v3 -> v4: gains a default `region`. Registered for a *different* event type
/// (`OrderPlaced`) so cross-event-type isolation can be checked too.
struct AddRegionForOrders;

impl Upcaster for AddRegionForOrders {
    fn event_type(&self) -> &str {
        "OrderPlaced"
    }
    fn source_version(&self) -> u32 {
        1
    }
    fn upcast(&self, payload: Value) -> Result<Value, String> {
        let mut obj = payload
            .as_object()
            .cloned()
            .ok_or_else(|| "payload must be an object".to_string())?;
        obj.entry("region".to_string())
            .or_insert_with(|| json!("us-east"));
        Ok(Value::Object(obj))
    }
}

fn registry() -> UpcasterRegistry {
    let mut r = UpcasterRegistry::new();
    r.register(Box::new(AddEmail));
    r.register(Box::new(AddTier));
    r.register(Box::new(AddRegionForOrders));
    r
}

#[test]
fn three_step_chain_applies_each_step_in_order() {
    let registry = registry();

    let out = registry
        .upcast("UserCreated", json!({ "id": "u1" }), 1, 3)
        .unwrap();

    assert_eq!(
        out,
        json!({ "id": "u1", "email": "unknown@example.com", "tier": "standard" })
    );
}

#[test]
fn single_hop_from_middle_version_only_applies_remaining_step() {
    let registry = registry();

    // Already at v2 (has email); only the v2 -> v3 step should run.
    let out = registry
        .upcast(
            "UserCreated",
            json!({ "id": "u2", "email": "u2@x.com" }),
            2,
            3,
        )
        .unwrap();

    assert_eq!(
        out,
        json!({ "id": "u2", "email": "u2@x.com", "tier": "standard" })
    );
}

#[test]
fn same_version_requires_no_registered_upcaster() {
    let registry = registry();

    let payload = json!({ "id": "u3", "email": "u3@x.com", "tier": "gold" });
    let out = registry
        .upcast("UserCreated", payload.clone(), 3, 3)
        .unwrap();

    assert_eq!(out, payload);
}

#[test]
fn resolve_chain_reports_the_ordered_source_versions() {
    let registry = registry();

    let steps = registry.resolve_chain("UserCreated", 1, 3).unwrap();
    assert_eq!(steps, vec![1, 2]);
}

#[test]
fn chains_are_isolated_per_event_type() {
    let registry = registry();

    // "OrderPlaced" has its own v1 -> v2 upcaster; it must not see UserCreated's steps.
    let out = registry
        .upcast("OrderPlaced", json!({ "order_id": "o1" }), 1, 2)
        .unwrap();
    assert_eq!(out, json!({ "order_id": "o1", "region": "us-east" }));

    // UserCreated has no v1 -> v2 -> ... path registered beyond v1/v2 keys above;
    // asking for a version with no OrderPlaced upcaster fails independently.
    let err = registry
        .upcast("OrderPlaced", json!({ "order_id": "o1" }), 2, 3)
        .unwrap_err();
    assert!(err.to_string().contains("OrderPlaced"));
}
