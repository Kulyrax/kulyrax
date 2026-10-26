//! Phase 12.6: `on_existing: replace` with a composite `primary_key` (Phase
//! 11.1) — a bridge-table row is updated in place by a newer-sequence event
//! for the same `(left, right)` pair; a different pair inserts a new row.

mod common;

use common::prepared;
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

fn tag_assigned(sequence: u64, post_id: &str, tag_id: &str, note: &str) -> Event {
    Event {
        event_id: format!("evt-{sequence}"),
        event_type: "TagAssigned".to_string(),
        payload: format!(
            r#"{{ "post_id": "{}", "tag_id": "{}", "note": "{}" }}"#,
            post_id, tag_id, note
        ),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

#[test]
fn same_pair_updates_in_place_different_pair_inserts() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute(
            "CREATE TABLE post_tags (
                post_id TEXT NOT NULL,
                tag_id TEXT NOT NULL,
                note TEXT NOT NULL,
                PRIMARY KEY (post_id, tag_id)
            )",
            [],
        )?;
    }

    let mut sql_mappings = HashMap::new();
    sql_mappings.insert(
        "TagAssigned".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: TagAssigned
table: post_tags
primary_key: [post_id, tag_id]
version: 1
on_existing: replace
columns:
  post_id: payload.post_id
  tag_id: payload.tag_id
  note: payload.note
"#,
        )?,
    );

    let adapter = prepared(SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(sql_mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ));

    // First delivery for (post-1, tag-a).
    let r1 = adapter.handle(&tag_assigned(1, "post-1", "tag-a", "first"));
    assert!(
        matches!(r1.outcome, AdapterOutcome::Created),
        "{:?}",
        r1.outcome
    );

    // A newer-sequence event for the SAME pair: update in place, not a new row.
    let r2 = adapter.handle(&tag_assigned(2, "post-1", "tag-a", "second"));
    assert!(
        matches!(r2.outcome, AdapterOutcome::Updated),
        "{:?}",
        r2.outcome
    );

    // A different pair: a fresh row.
    let r3 = adapter.handle(&tag_assigned(1, "post-1", "tag-b", "other"));
    assert!(
        matches!(r3.outcome, AdapterOutcome::Created),
        "{:?}",
        r3.outcome
    );

    let conn = Connection::open(&db_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM post_tags", [], |r| r.get(0))?;
    assert_eq!(count, 2, "one row per distinct (post_id, tag_id) pair");

    let note_a: String = conn.query_row(
        "SELECT note FROM post_tags WHERE post_id = 'post-1' AND tag_id = 'tag-a'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(
        note_a, "second",
        "the pair's row must have been updated in place"
    );

    let note_b: String = conn.query_row(
        "SELECT note FROM post_tags WHERE post_id = 'post-1' AND tag_id = 'tag-b'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(note_b, "other");

    Ok(())
}
