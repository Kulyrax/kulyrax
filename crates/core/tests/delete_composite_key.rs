//! Phase 13.6: a bridge-table row (`primary_key: [left, right]`) is removed
//! by a `delete` mapping resolving the same composite key; a different pair
//! is untouched.

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

fn tag_assigned(sequence: u64, post_id: &str, tag_id: &str) -> Event {
    Event {
        event_id: format!("evt-assign-{sequence}"),
        event_type: "TagAssigned".to_string(),
        payload: format!(r#"{{ "post_id": "{}", "tag_id": "{}" }}"#, post_id, tag_id),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

fn tag_removed(sequence: u64, post_id: &str, tag_id: &str) -> Event {
    Event {
        event_id: format!("evt-remove-{sequence}"),
        event_type: "TagRemoved".to_string(),
        payload: format!(r#"{{ "post_id": "{}", "tag_id": "{}" }}"#, post_id, tag_id),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
}

#[test]
fn delete_removes_only_the_matching_pair() -> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute(
            "CREATE TABLE post_tags (
                post_id TEXT NOT NULL,
                tag_id TEXT NOT NULL,
                PRIMARY KEY (post_id, tag_id)
            )",
            [],
        )?;
    }

    let mut mappings = HashMap::new();
    mappings.insert(
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
    mappings.insert(
        "TagRemoved".to_string(),
        serde_yaml::from_str::<SqlMapping>(
            r#"
event: TagRemoved
table: post_tags
primary_key: [post_id, tag_id]
version: 1
operation: delete
columns:
  post_id: payload.post_id
  tag_id: payload.tag_id
"#,
        )?,
    );

    let adapter = SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    adapter.handle(&tag_assigned(1, "post-1", "tag-a"));
    adapter.handle(&tag_assigned(1, "post-1", "tag-b"));

    let r = adapter.handle(&tag_removed(2, "post-1", "tag-a"));
    assert!(
        matches!(r.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r.outcome
    );

    let conn = Connection::open(&db_path)?;
    let count: i64 = conn.query_row("SELECT COUNT(*) FROM post_tags", [], |r| r.get(0))?;
    assert_eq!(count, 1, "only the removed pair's row should be gone");

    let remaining: String = conn.query_row(
        "SELECT tag_id FROM post_tags WHERE post_id = 'post-1'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(remaining, "tag-b", "the untouched pair must remain");

    Ok(())
}
