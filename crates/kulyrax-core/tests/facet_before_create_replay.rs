//! Phase 15.6 (15.5): a replay directory whose file names would deliver the
//! facet event before the entity's create → replay reorders by `sequence`, so
//! the create is applied first and the final document has both the base and
//! the facet fields. Contrast `facet_before_create_live` (no reordering in a
//! live stream).

use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::event::Event;
use kulyrax_core::replay::{ReplayContext, events_from_path};
use kulyrax_core::runtime::config::{
    AdapterConfig, FailurePolicy, FileAdapterConfig, FileConfig, KulyraxConfig, RoutingConfig,
};

use std::collections::HashMap;
use std::fs;

fn doc_mappings() -> HashMap<String, DocumentMapping> {
    let mut m = HashMap::new();
    m.insert(
        "UserCreated".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            r#"
event: UserCreated
collection: users
version: 1
id: payload.id
document:
  id: payload.id
  display_name: payload.display_name
"#,
        )
        .unwrap(),
    );
    m.insert(
        "UserEmailChanged".to_string(),
        serde_yaml::from_str::<DocumentMapping>(
            r#"
event: UserEmailChanged
collection: users
version: 1
id: payload.id
facet: contact
document:
  email: payload.email
"#,
        )
        .unwrap(),
    );
    m
}

#[test]
fn replay_reorders_facet_after_create_by_sequence() {
    let tmp = tempfile::tempdir().unwrap();
    let docs = tmp.path().join("docs");
    fs::create_dir_all(&docs).unwrap();
    let events_dir = tmp.path().join("events");
    fs::create_dir_all(&events_dir).unwrap();

    // File name order puts the facet event first; its sequence (5) is higher.
    fs::write(
        events_dir.join("01-email.json"),
        serde_json::to_string(&Event {
            event_id: "e-email".into(),
            event_type: "UserEmailChanged".into(),
            payload: r#"{"id":"u1","email":"ada@x.com"}"#.into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 5,
        })
        .unwrap(),
    )
    .unwrap();
    fs::write(
        events_dir.join("02-create.json"),
        serde_json::to_string(&Event {
            event_id: "e-create".into(),
            event_type: "UserCreated".into(),
            payload: r#"{"id":"u1","display_name":"Ada"}"#.into(),
            metadata: HashMap::new(),
            version: 1,
            sequence: 1,
        })
        .unwrap(),
    )
    .unwrap();

    let config = KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "routing.json".into(),
        },
        adapters: vec![AdapterConfig::File(FileAdapterConfig {
            id: "doc-readmodel".into(),
            priority: 10,
            config: FileConfig {
                root: docs.to_string_lossy().into(),
            },
            capabilities: None,
            depends_on: vec![],
        })],
        failure_policy: FailurePolicy::FailFast,
        migration_policy: Default::default(),
        sources: Vec::new(),
    };
    config.validate().unwrap();

    let mut rules: HashMap<String, Vec<String>> = HashMap::new();
    rules.insert("UserCreated".to_string(), vec!["doc-readmodel".into()]);
    rules.insert("UserEmailChanged".to_string(), vec!["doc-readmodel".into()]);

    let sql: HashMap<String, SqlMapping> = HashMap::new();
    let mut ctx = ReplayContext::new(
        &config,
        rules,
        sql,
        doc_mappings(),
        HashMap::new(),
        HashMap::new(),
    );
    let report = ctx
        .run_stream(events_from_path(&events_dir).unwrap())
        .unwrap();

    assert_eq!(report.events_processed, 2);
    assert_eq!(report.events_succeeded, 2, "{:?}", report.per_event);
    // Reordered: create (seq 1) before facet (seq 5).
    assert_eq!(report.per_event[0].event_id, "e-create");
    assert_eq!(report.per_event[1].event_id, "e-email");

    let doc: serde_json::Value =
        serde_json::from_str(&fs::read_to_string(docs.join("users").join("u1.json")).unwrap())
            .unwrap();
    assert_eq!(doc["display_name"], "Ada");
    assert_eq!(doc["email"], "ada@x.com");
}
