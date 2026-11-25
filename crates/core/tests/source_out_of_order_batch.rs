//! A batch whose files are named so one entity's events arrive out of sequence
//! order (3, 1, 2). They are dispatched in that delivery order, not re-sorted
//! (decision 2026-11-25-batch-order); under `on_existing: replace` the guard
//! lane still leaves the highest-sequence value: 1 and 2 arrive stale.

mod common;

use common::*;
use kulyrax_core::source::EventSource;
use kulyrax_core::source::directory::DirectorySource;

use tempfile::tempdir;

#[test]
fn out_of_order_events_of_one_entity_converge_under_replace() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("test.db");
    init_db(&db);
    let events = dir.path().join("events");

    // File-name order 3, 1, 2 — the create (seq 1) is NOT first by file name.
    write_event_file(&events, "a.json", &user_event(3, "u1", "third"));
    write_event_file(&events, "b.json", &user_event(1, "u1", "first"));
    write_event_file(&events, "c.json", &user_event(2, "u1", "second"));

    let src: Box<dyn EventSource> =
        Box::new(DirectorySource::new("dir", &events, dir.path().join("state")).unwrap());
    run_once(&db, vec![src], None).unwrap();

    assert_eq!(read_user(&db, "u1"), Some(("third".to_string(), 3)));
}
