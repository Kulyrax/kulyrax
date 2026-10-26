//! The "embedded crate" deployment mode (architecture.md §1.3): a host
//! service links `kulyrax-core` and calls `execute_event` directly — no CLI,
//! no config file, no gRPC. Run with `cargo run --manifest-path
//! examples/embedded/Cargo.toml` from the repo root.

use kulyrax_core::event::Event;
use kulyrax_core::execute_event;
use kulyrax_core::runtime::config::{
    AdapterConfig, KulyraxConfig, RoutingConfig, SqliteAdapterConfig, SqliteConfig,
};
use std::collections::HashMap;

fn main() {
    let db_path = std::env::temp_dir().join("kulyrax-embedded-example.db");
    let _ = std::fs::remove_file(&db_path);
    rusqlite::Connection::open(&db_path)
        .unwrap()
        .execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)", [])
        .unwrap();

    // `execute_event` takes routing rules explicitly — no config file or
    // env var needed; a host embedding kulyrax-core builds this map however
    // it likes (loaded from disk via `routing::load_routing`, or built
    // in-process, as here) and passes it into every call.
    let routing_rules: HashMap<String, Vec<String>> =
        HashMap::from([("UserRegistered".to_string(), vec!["sql-primary".to_string()])]);

    let config = KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "routing.yaml".into(),
        },
        adapters: vec![AdapterConfig::Sqlite(SqliteAdapterConfig {
            id: "sql-primary".into(),
            priority: 10,
            config: SqliteConfig {
                path: db_path.to_string_lossy().into(),
            },
            capabilities: None,
            depends_on: vec![],
        })],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    };

    let mut sql_mappings = HashMap::new();
    sql_mappings.insert(
        "UserRegistered".to_string(),
        serde_yaml::from_str(
            "event: UserRegistered\ntable: users\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n  name: payload.name\n",
        )
        .unwrap(),
    );

    let event = Event {
        event_id: "e1".into(),
        event_type: "UserRegistered".into(),
        payload: r#"{"id":"u1","name":"Ada"}"#.into(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    };

    let report = execute_event(
        &config,
        sql_mappings,
        HashMap::new(), // document_mappings
        HashMap::new(), // kv_mappings
        HashMap::new(), // graph_mappings
        &routing_rules,
        event,
    );
    println!("status: {:?}", report.status);
    for a in &report.adapter_reports {
        println!("  {} -> {:?}", a.adapter_id, a.outcome);
    }

    let name: String = rusqlite::Connection::open(&db_path)
        .unwrap()
        .query_row("SELECT name FROM users WHERE id = 'u1'", [], |r| r.get(0))
        .unwrap();
    assert_eq!(name, "Ada");
    println!("projected: users.u1.name = {name:?}");
}
