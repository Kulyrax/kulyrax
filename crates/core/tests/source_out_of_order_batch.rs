//! Phase 17.6 (15.5): a batch whose files are named so the events arrive out of
//! sequence order — the run loop stable-sorts by `sequence` before dispatch, so
//! the final state is the highest-sequence value.

mod common;

use common::*;
use kulyrax_core::source::EventSource;
use kulyrax_core::source::directory::DirectorySource;

use tempfile::tempdir;

#[test]
fn batch_is_sorted_by_sequence_before_dispatch() {
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
