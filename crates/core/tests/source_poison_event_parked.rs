//! Phase 17.6 (17.5): a deterministically-failing event in a batch → after the
//! retry budget it is DLQ'd (parked, not lost), the batch commits, and the
//! rest of the batch is projected.

mod common;

use common::*;
use kulyrax_core::event::Event;
use kulyrax_core::source::EventSource;
use kulyrax_core::source::directory::DirectorySource;
use kulyrax_core::{RunMode, SourceRunOptions, StoppedReason};

use std::collections::HashMap;
use tempfile::tempdir;

/// A `UserUpserted` event missing `payload.name` → the SQL mapping's
/// `resolve_path` fails deterministically, every time.
fn poison(seq: u64, id: &str) -> Event {
    Event {
        event_id: format!("evt-poison-{seq}"),
        event_type: "UserUpserted".to_string(),
        payload: format!(r#"{{ "id": "{id}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

#[test]
fn poison_event_is_dlqd_and_batch_commits() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("test.db");
    init_db(&db);
    let events = dir.path().join("events");
    let dlq = dir.path().join("dlq");

    write_event_file(&events, "01.json", &user_event(1, "u1", "Ada"));
    write_event_file(&events, "02.json", &poison(1, "u2"));
    write_event_file(&events, "03.json", &user_event(1, "u3", "Grace"));

    let src: Box<dyn EventSource> =
        Box::new(DirectorySource::new("dir", &events, dir.path().join("state")).unwrap());
    let opts = SourceRunOptions {
        mode: RunMode::Once,
        max_batch: 256,
        retry_budget: 1,
        max_batch_delay: std::time::Duration::ZERO,
        dlq_dir: Some(dlq.clone()),
    };
    let report = run_once(&db, vec![src], Some(opts)).unwrap();

    assert!(matches!(report.stopped_reason, StoppedReason::CaughtUp));
    assert_eq!(report.events_dlq, 1);
    assert_eq!(report.batches_committed, 1, "the batch still commits");

    // The good events landed.
    assert_eq!(read_user(&db, "u1"), Some(("Ada".to_string(), 1)));
    assert_eq!(read_user(&db, "u3"), Some(("Grace".to_string(), 1)));
    // The poison event did not.
    assert_eq!(read_user(&db, "u2"), None);

    // It is parked in the DLQ.
    let parked: Vec<_> = std::fs::read_dir(&dlq)
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert_eq!(parked.len(), 1, "{parked:?}");

    // Restart → committed position honoured, nothing re-run.
    let src2: Box<dyn EventSource> =
        Box::new(DirectorySource::new("dir", &events, dir.path().join("state")).unwrap());
    let r2 = run_once(&db, vec![src2], None).unwrap();
    assert_eq!(r2.events_processed, 0);
}
