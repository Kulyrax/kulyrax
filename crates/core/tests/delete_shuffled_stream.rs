//! Phase 13.6: `[create s1, update s2, delete s3]` fed through in three
//! different delivery orders converges to the identical final state (absent,
//! tombstone at sequence 3) each time — delete is sequence-gated symmetrically
//! with upsert (Phase 13.1).

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

fn mappings() -> HashMap<String, SqlMapping> {
    let mut m = HashMap::new();
    m.insert(
        "UserUpdated".to_string(),
        serde_yaml::from_str::<SqlMapping>(
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
        .unwrap(),
    );
    m.insert(
        "UserDeleted".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: UserDeleted
table: users
primary_key: id
version: 1
operation: delete
columns:
  id: payload.id
"#,
        )
        .unwrap(),
    );
    m
}

fn events() -> HashMap<u64, Event> {
    [
        (
            1,
            Event {
                event_id: "e1".to_string(),
                event_type: "UserUpdated".to_string(),
                payload: r#"{"id":"u1","state":"s1"}"#.to_string(),
                metadata: HashMap::new(),
                version: 1,
                sequence: 1,
            },
        ),
        (
            2,
            Event {
                event_id: "e2".to_string(),
                event_type: "UserUpdated".to_string(),
                payload: r#"{"id":"u1","state":"s2"}"#.to_string(),
                metadata: HashMap::new(),
                version: 1,
                sequence: 2,
            },
        ),
        (
            3,
            Event {
                event_id: "e3".to_string(),
                event_type: "UserDeleted".to_string(),
                payload: r#"{"id":"u1"}"#.to_string(),
                metadata: HashMap::new(),
                version: 1,
                sequence: 3,
            },
        ),
    ]
    .into_iter()
    .collect()
}

/// Apply `order` into a fresh database; return (row_count, last_sequence, deleted).
fn apply_order(order: &[u64]) -> (i64, i64, i64) {
    let events = events();
    let dir = tempdir().unwrap();
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path).unwrap();
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, state TEXT)", [])
            .unwrap();
    }

    let adapter = prepared(SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(mappings()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ));

    for &sequence in order {
        let result = adapter.handle(&events[&sequence]);
        assert!(result.is_success(), "seq {sequence}: {:?}", result.outcome);
    }

    let conn = Connection::open(&db_path).unwrap();
    let count: i64 = conn
        .query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))
        .unwrap();
    let (last_sequence, deleted): (i64, i64) = conn
        .query_row(
            "SELECT last_sequence, deleted FROM conduit_projection_state WHERE target_table = 'users'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    (count, last_sequence, deleted)
}

#[test]
fn three_delivery_orders_converge_to_absent_with_a_tombstone_at_sequence_three() {
    let sorted = apply_order(&[1, 2, 3]);
    let reversed = apply_order(&[3, 2, 1]);
    let shuffled = apply_order(&[2, 3, 1]);

    assert_eq!(sorted, (0, 3, 1));
    assert_eq!(sorted, reversed);
    assert_eq!(sorted, shuffled);
}
