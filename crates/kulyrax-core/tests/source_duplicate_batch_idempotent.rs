//! Phase 17.6: forcing the same batch through twice (as a pre-commit crash
//! would) leaves the sinks converged and the guard unchanged — the sequence
//! gate absorbs the redelivery.

mod common;

use common::*;
use kulyrax_core::source::EventSource;
use kulyrax_core::source::directory::DirectorySource;

use tempfile::tempdir;

#[test]
fn replaying_a_committed_batch_is_a_no_op() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("test.db");
    init_db(&db);
    let events = dir.path().join("events");
    let state = dir.path().join("state");

    write_event_file(&events, "001.json", &user_event(1, "u1", "Ada"));
    write_event_file(&events, "002.json", &user_event(2, "u1", "Ada L."));

    let src: Box<dyn EventSource> = Box::new(DirectorySource::new("dir", &events, &state).unwrap());
    run_once(&db, vec![src], None).unwrap();
    let after_first = read_user(&db, "u1");
    assert_eq!(after_first, Some(("Ada L.".to_string(), 2)));

    // Wipe the checkpoint → the whole batch is re-polled and re-dispatched.
    std::fs::remove_file(state.join("dir.json")).unwrap();
    let src2: Box<dyn EventSource> =
        Box::new(DirectorySource::new("dir", &events, &state).unwrap());
    let report = run_once(&db, vec![src2], None).unwrap();

    assert_eq!(
        read_user(&db, "u1"),
        after_first,
        "state unchanged by redelivery"
    );
    assert_eq!(report.events_processed, 2);
    // Every adapter outcome on the redelivery was a skip.
    assert_eq!(
        report.batches.iter().map(|b| b.skipped).sum::<u64>(),
        2,
        "{report:?}"
    );
    assert_eq!(
        report
            .batches
            .iter()
            .map(|b| b.created + b.updated)
            .sum::<u64>(),
        0
    );
}
