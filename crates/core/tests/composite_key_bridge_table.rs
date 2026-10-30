//! Phase 11.4: a composite `primary_key` (Phase 11.1) — the shape a
//! many-to-many bridge/junction table actually needs, since no single column
//! identifies a row. Two different `event_id`s that resolve to the same
//! `(post_id, tag_id)` pair are deduplicated; a genuinely different pair is not.

mod common;

use common::prepared;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use rusqlite::Connection;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::tempdir;

fn tag_assigned(event_id: &str, sequence: u64, post_id: &str, tag_id: &str) -> Event {
    Event {
        event_id: event_id.to_string(),
        event_type: "TagAssigned".to_string(),
        payload: format!(r#"{{ "post_id": "{}", "tag_id": "{}" }}"#, post_id, tag_id),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

#[test]
fn duplicate_pair_across_event_ids_is_skipped_distinct_pair_is_not()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute(
            "CREATE TABLE post_tags (post_id TEXT NOT NULL, tag_id TEXT NOT NULL)",
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
columns:
  post_id: payload.post_id
  tag_id: payload.tag_id
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

    // Same (post_id, tag_id) pair, two different event_ids.
    let first = tag_assigned("evt-1", 1, "post-1", "tag-a");
    let duplicate = tag_assigned("evt-2", 2, "post-1", "tag-a");
    // A genuinely different pair (same post, different tag).
    let distinct = tag_assigned("evt-3", 3, "post-1", "tag-b");

    let r1 = adapter.handle(&first);
    assert!(r1.is_success(), "{:?}", r1.outcome);

    let r2 = adapter.handle(&duplicate);
    assert!(r2.is_success(), "{:?}", r2.outcome);
    assert!(
        matches!(
            r2.outcome,
            AdapterOutcome::Skipped(SkipReason::AlreadyProjected)
        ),
        "duplicate composite pair must be a clean skip, got {:?}",
        r2.outcome
    );

    let r3 = adapter.handle(&distinct);
    assert!(r3.is_success(), "{:?}", r3.outcome);
    assert!(
        matches!(r3.outcome, AdapterOutcome::Created),
        "a genuinely distinct composite pair must not be skipped, got {:?}",
        r3.outcome
    );

    let conn = Connection::open(&db_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM post_tags", [], |r| r.get(0))?;
    assert_eq!(count, 2, "one row per distinct (post_id, tag_id) pair");

    let mut stmt = conn.prepare("SELECT post_id, tag_id FROM post_tags ORDER BY tag_id")?;
    let rows: Vec<(String, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
        .collect::<Result<_, _>>()?;
    assert_eq!(
        rows,
        vec![
            ("post-1".to_string(), "tag-a".to_string()),
            ("post-1".to_string(), "tag-b".to_string()),
        ]
    );

    Ok(())
}
