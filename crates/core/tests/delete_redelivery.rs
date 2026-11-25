//! Phase 13.6: an exact redelivery of a `delete` (same `sequence`) is
//! `Skipped(StaleSequence)` — same "redelivery" contract Phase 12.3 defines
//! for upsert (`event_seq == last_sequence`), just extended to delete. A
//! *different*, newer-sequence delete landing on an already-tombstoned
//! entity is the distinct `Skipped(AlreadyDeleted)` case, which still bumps
//! `last_sequence` on the tombstone. Either way, the table/file stay absent
//! and the tombstone is never disturbed by a rejected event.

mod common;

use common::prepared;
use kulyrax_core::adapter::document::file::FileDocumentAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
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

fn create_event() -> Event {
    Event::new(
        "evt-create".to_string(),
        "UserCreated".to_string(),
        1,
        r#"{ "id": "u1", "state": "s1" }"#.to_string(),
    )
}

fn delete_event(sequence: u64) -> Event {
    Event::new(
        format!("evt-delete-{sequence}"),
        "UserDeleted".to_string(),
        sequence,
        r#"{ "id": "u1" }"#.to_string(),
    )
}

#[test]
fn sql_exact_redelivery_is_stale_newer_delete_is_already_deleted()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let db_path = dir.path().join("test.db");
    {
        let conn = Connection::open(&db_path)?;
        conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, state TEXT)", [])?;
    }

    let mut mappings = HashMap::new();
    mappings.insert(
        "UserCreated".to_string(),
        kulyrax_core::yaml::from_str::<SqlMapping>(
            r#"
event: UserCreated
table: users
primary_key: id
version: 1
on_existing: replace
columns:
  id: payload.id
  state: payload.state
"#,
        )?,
    );
    mappings.insert(
        "UserDeleted".to_string(),
        kulyrax_core::yaml::from_str::<SqlMapping>(
            r#"
event: UserDeleted
table: users
primary_key: id
version: 1
operation: delete
columns:
  id: payload.id
"#,
        )?,
    );

    let adapter = prepared(SqliteAdapter::new(
        "sqlite".to_string(),
        db_path.to_string_lossy().to_string(),
        10,
        SqlRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    ));

    adapter.handle(&create_event());
    let delete_seq2 = delete_event(2);
    let r1 = adapter.handle(&delete_seq2);
    assert!(
        matches!(r1.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r1.outcome
    );

    // Exact redelivery: same event_id, same sequence.
    let r2 = adapter.handle(&delete_seq2);
    assert!(
        matches!(
            r2.outcome,
            AdapterOutcome::Skipped(SkipReason::StaleSequence)
        ),
        "expected Skipped(StaleSequence), got {:?}",
        r2.outcome
    );

    let conn = Connection::open(&db_path)?;
    let last_sequence: i64 = conn.query_row(
        "SELECT last_sequence FROM kulyrax_projection_state WHERE target_table = 'users'",
        [],
        |r| r.get(0),
    )?;
    assert_eq!(
        last_sequence, 2,
        "exact redelivery must not change anything"
    );

    // A distinct, newer delete for an already-deleted entity.
    let r3 = adapter.handle(&delete_event(7));
    assert!(
        matches!(
            r3.outcome,
            AdapterOutcome::Skipped(SkipReason::AlreadyDeleted)
        ),
        "expected Skipped(AlreadyDeleted), got {:?}",
        r3.outcome
    );

    let (last_sequence, deleted): (i64, i64) = conn.query_row(
        "SELECT last_sequence, deleted FROM kulyrax_projection_state WHERE target_table = 'users'",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )?;
    assert_eq!(
        last_sequence, 7,
        "last_sequence must still bump on an already-deleted skip"
    );
    assert_eq!(deleted, 1);

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM users", [], |r| r.get(0))?;
    assert_eq!(count, 0);

    Ok(())
}

#[test]
fn document_exact_redelivery_is_stale_newer_delete_is_already_deleted()
-> Result<(), Box<dyn std::error::Error>> {
    let dir = tempdir()?;
    let root = dir.path();

    let mut mappings = HashMap::new();
    mappings.insert(
        "UserCreated".to_string(),
        kulyrax_core::yaml::from_str::<DocumentMapping>(
            r#"
event: UserCreated
collection: users
version: 1
id: payload.id
on_existing: replace
document:
  id: payload.id
  state: payload.state
"#,
        )?,
    );
    mappings.insert(
        "UserDeleted".to_string(),
        kulyrax_core::yaml::from_str::<DocumentMapping>(
            r#"
event: UserDeleted
collection: users
version: 1
id: payload.id
operation: delete
document: {}
"#,
        )?,
    );

    let adapter = FileDocumentAdapter::new(
        "file".to_string(),
        root.to_path_buf(),
        10,
        DocumentRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    adapter.handle(&create_event());
    let delete_seq2 = delete_event(2);
    let r1 = adapter.handle(&delete_seq2);
    assert!(
        matches!(r1.outcome, AdapterOutcome::Deleted),
        "{:?}",
        r1.outcome
    );

    let r2 = adapter.handle(&delete_seq2);
    assert!(
        matches!(
            r2.outcome,
            AdapterOutcome::Skipped(SkipReason::StaleSequence)
        ),
        "expected Skipped(StaleSequence), got {:?}",
        r2.outcome
    );
    assert!(!root.join("users").join("u1.json").exists());

    let r3 = adapter.handle(&delete_event(7));
    assert!(
        matches!(
            r3.outcome,
            AdapterOutcome::Skipped(SkipReason::AlreadyDeleted)
        ),
        "expected Skipped(AlreadyDeleted), got {:?}",
        r3.outcome
    );

    let guard_content = std::fs::read_to_string(
        root.join(".kulyrax")
            .join("entities")
            .join("users")
            .join("u1.done"),
    )?;
    let guard: serde_json::Value = serde_json::from_str(&guard_content)?;
    assert_eq!(guard["last_sequence"], 7);
    assert_eq!(guard["deleted"], true);

    Ok(())
}
