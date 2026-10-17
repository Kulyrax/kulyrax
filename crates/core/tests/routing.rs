use kulyrax_core::event::Event;
use kulyrax_core::routing::{AdapterId, load_routing, route_with_rules};

use std::collections::HashMap;

fn test_routing() -> HashMap<String, Vec<AdapterId>> {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("routing.yaml");

    load_routing(path).expect("routing fixture loads")
}

fn test_event(event_type: &str) -> Event {
    Event {
        event_id: "evt-1".to_string(),
        event_type: event_type.to_string(),
        payload: "{}".to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    }
}

#[test]
fn user_created_routes_to_sql_and_document() {
    let event = test_event("UserCreated");

    let targets = route_with_rules(&event, &test_routing());

    assert_eq!(
        targets,
        vec!["sql-primary".to_string(), "doc-readmodel".to_string()],
        "UserCreated must route to sql-primary and doc-readmodel adapters"
    );
}
#[test]
fn cache_invalidated_routes_to_key_value() {
    let event = test_event("CacheInvalidated");

    let targets = route_with_rules(&event, &test_routing());

    assert_eq!(
        targets,
        vec!["kv-cache".to_string()],
        "CacheInvalidated must route to kv-cache adapter"
    );
}

#[test]
fn unknown_event_routes_to_document_by_default() {
    let event = test_event("UnknownEvent");

    let targets = route_with_rules(&event, &test_routing());

    assert!(
        targets.is_empty(),
        "Unknown events must not route to any adapter"
    );
}

#[test]
fn json_routing_file_is_rejected_with_conversion_guidance() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routing.json");
    std::fs::write(&path, r#"{"UserCreated": ["sql-primary"]}"#).expect("write");

    let err = load_routing(&path).expect_err("JSON routing is rejected");
    assert!(err.contains("no longer supported"), "{err}");
    assert!(err.contains("routing.yaml"), "{err}");
}

#[test]
fn routing_file_must_be_yaml() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routing.txt");
    std::fs::write(&path, "UserCreated: [sql-primary]\n").expect("write");

    let err = load_routing(&path).expect_err("non-YAML extension is rejected");
    assert!(err.contains("must end in .yaml or .yml"), "{err}");
}

#[test]
fn yml_routing_file_loads() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routing.yml");
    std::fs::write(&path, "UserCreated: [sql-primary, doc-readmodel]\n").expect("write");

    let rules = load_routing(&path).expect("loads");
    assert_eq!(rules["UserCreated"], ["sql-primary", "doc-readmodel"]);
}

#[test]
fn invalid_routing_yaml_names_the_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("routing.yaml");
    std::fs::write(&path, "UserCreated: sql-primary\n").expect("write");

    let err = load_routing(&path).expect_err("a scalar instead of a list is rejected");
    assert!(
        err.starts_with("Invalid routing file") && err.contains("routing.yaml"),
        "{err}"
    );
}
