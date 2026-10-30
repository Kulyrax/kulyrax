//! Phase 12.6: the same stream of `on_existing: replace` events, fed through
//! replay in three different orders, converges to the identical final state
//! each time — the guarantee under test for the whole phase: final state is
//! a pure function of the *set* of delivered events (highest sequence per
//! entity), independent of delivery order.

mod common;

use common::prepared;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

fn sql_mapping_replace() -> SqlMapping {
    serde_yaml::from_str(
        r#"
event: UserUpdated
table: users
primary_key: id
version: 1
on_existing: replace
columns:
  id: payload.id
  state: payload.state
"#,
    )
    .unwrap()
}

fn user_updated(sequence: u64, state: &str) -> Event {
    Event {
        event_id: format!("evt-seq-{sequence}"),
        event_type: "UserUpdated".to_string(),
        payload: format!(r#"{{ "id": "u1", "state": "{state}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

/// Apply `order` (a permutation of the four events) into a fresh database;
/// return the final (state, last_sequence).
fn apply_order(order: &[u64]) -> (String, i64) {
    let events: HashMap<u64, Event> = [
        (1, user_updated(1, "s1")),
        (2, user_updated(2, "s2")),
        (3, user_updated(3, "s3")),
        (4, user_updated(4, "s4")),
    ]
    .into_iter()
    .collect();

    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, state TEXT)", [])
            .unwrap();
    }

    let mut mappings = HashMap::new();
    mappings.insert("UserUpdated".to_string(), sql_mapping_replace());
    let adapter = prepared(SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ));

    for &sequence in order {
        let result = adapter.handle(&events[&sequence]);
        assert!(result.is_success(), "seq {sequence}: {:?}", result.outcome);
    }

    let conn = Connection::open(&db_path).unwrap();
    let state: String = conn
        .query_row("SELECT state FROM users WHERE id = 'u1'", [], |r| r.get(0))
        .unwrap();
    let last_sequence: i64 = conn
        .query_row(
            "SELECT last_sequence FROM conduit_projection_state WHERE target_table = 'users'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    (state, last_sequence)
}

#[test]
fn three_delivery_orders_converge_to_the_same_final_state() {
    let sorted = apply_order(&[1, 2, 3, 4]);
    let reversed = apply_order(&[4, 3, 2, 1]);
    let shuffled = apply_order(&[3, 1, 4, 2]);

    assert_eq!(sorted, ("s4".to_string(), 4));
    assert_eq!(sorted, reversed);
    assert_eq!(sorted, shuffled);
}
