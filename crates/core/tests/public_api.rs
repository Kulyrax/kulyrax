use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::event::Event;
use kulyrax_core::execute_event;
use kulyrax_core::routing::{AdapterId, load_routing};
use kulyrax_core::runtime::config::{
    AdapterConfig, FileAdapterConfig, FileConfig, KulyraxConfig, SqliteAdapterConfig, SqliteConfig,
};

use std::collections::HashMap;
use std::path::Path;

// ------------------------------------------------------------
// Helpers
// ------------------------------------------------------------

/// `tests/fixtures/routing.yaml` routes `UserCreated` to `["sql-primary", "doc-readmodel"]`
/// — must match the adapter IDs used below.
fn test_routing() -> HashMap<String, Vec<AdapterId>> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("routing.yaml");

    load_routing(path).expect("routing fixture loads")
}

// ------------------------------------------------------------
// Test
// ------------------------------------------------------------

#[test]
fn execute_event_runs_without_panic() {
    // -----------------------------
    // Config
    // -----------------------------
    let config = KulyraxConfig::new(
        "routing.yaml",
        vec![
            AdapterConfig::Sqlite(SqliteAdapterConfig::new(
                "sql-primary",
                10,
                SqliteConfig::new(":memory:"),
            )),
            AdapterConfig::File(FileAdapterConfig::new(
                "doc-readmodel",
                20,
                FileConfig::new("./target/test-docs"),
            )),
        ],
    );

    config.validate().expect("config must be valid");

    // -----------------------------
    // SQL mappings
    // -----------------------------
    let mut sql_mappings = HashMap::new();
    sql_mappings.insert(
        "UserCreated".to_string(),
        kulyrax_core::yaml::from_str::<SqlMapping>(
            r#"
event: UserCreated
table: users
primary_key: id
version: 1
columns:
  id: payload.id
"#,
        )
        .unwrap(),
    );

    // -----------------------------
    // Document mappings
    // -----------------------------
    let mut doc_mappings = HashMap::new();
    doc_mappings.insert(
        "UserCreated".to_string(),
        kulyrax_core::yaml::from_str::<DocumentMapping>(
            r#"
event: UserCreated
collection: users
version: 1
id: payload.id
document:
  id: payload.id
"#,
        )
        .unwrap(),
    );

    // -----------------------------
    // Event
    // -----------------------------
    let event = Event::new("evt-1", "UserCreated", 1, r#"{ "id": "u1" }"#);

    // -----------------------------
    // Execute
    // -----------------------------
    let report = execute_event(
        &config,
        sql_mappings,
        doc_mappings,
        HashMap::new(),
        HashMap::new(),
        &test_routing(),
        event,
    );

    // -----------------------------
    // Assert
    // -----------------------------
    assert!(
        !report.adapter_reports.is_empty(),
        "UserCreated must execute at least one adapter"
    );

    for r in &report.adapter_reports {
        assert!(!r.adapter_id.is_empty(), "adapter_id must always be set");
    }
}
