//! Phase 16.6: `UserRegistered` projected to a SQL row **and** a graph node →
//! the key and properties agree across both back ends, driven by the same
//! `decide()` gate.

use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::runtime::GraphRuntimeBuilder;
use kulyrax_core::adapter::graph::store::GraphStore;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::adapter::{AdapterOutcome, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

fn registered(seq: u64, name: &str) -> Event {
    Event {
        event_id: format!("evt-reg-{seq}"),
        event_type: "UserRegistered".to_string(),
        payload: format!(r#"{{ "user_id": "u1", "name": "{name}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

#[test]
fn sql_row_and_graph_node_agree() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute(
            "CREATE TABLE users (user_id TEXT PRIMARY KEY, name TEXT)",
            [],
        )?;
    }

    let mut sql_m = HashMap::new();
    sql_m.insert(
        "UserRegistered".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            "event: UserRegistered\ntable: users\nprimary_key: user_id\nversion: 1\non_existing: replace\ncolumns:\n  user_id: payload.user_id\n  name: payload.name\n",
        )
        .unwrap(),
    );
    let sql = SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(sql_m),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let mut graph_m = HashMap::new();
    graph_m.insert(
        "UserRegistered".to_string(),
        serde_yaml::from_str::<GraphMapping>(
            "kind: node\nevent: UserRegistered\nlabel: User\nversion: 1\nkey: payload.user_id\non_existing: replace\nproperties:\n  name: payload.name\n",
        )
        .unwrap(),
    );
    let graph = GraphStore::new(
        "graph".to_string(),
        dir.path().join("g"),
        20,
        GraphRuntimeBuilder::new(graph_m),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    for e in [registered(1, "Ada"), registered(2, "Ada L.")] {
        assert!(!matches!(sql.handle(&e).outcome, AdapterOutcome::Failed(_)));
        assert!(!matches!(
            graph.handle(&e).outcome,
            AdapterOutcome::Failed(_)
        ));
    }

    let conn = Connection::open(&db_path)?;
    let (sql_key, sql_name): (String, String) =
        conn.query_row("SELECT user_id, name FROM users", [], |r| {
            Ok((r.get(0)?, r.get(1)?))
        })?;

    let node: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        dir.path()
            .join("g")
            .join("nodes")
            .join("User")
            .join("u1.json"),
    )?)?;

    assert_eq!(sql_key, "u1");
    assert_eq!(sql_name, "Ada L.");
    assert_eq!(node["name"], sql_name);

    Ok(())
}
