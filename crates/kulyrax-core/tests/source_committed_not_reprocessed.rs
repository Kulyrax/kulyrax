//! Phase 17.6: after a clean `commit`, a restart does not re-poll the
//! committed files — the position is honoured.

mod common;

use common::*;
use kulyrax_core::source::directory::DirectorySource;
use kulyrax_core::source::{EventSource, SourcePosition};

use tempfile::tempdir;

#[test]
fn committed_files_are_not_repolled() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("test.db");
    init_db(&db);
    let events = dir.path().join("events");
    let state = dir.path().join("state");

    write_event_file(&events, "001.json", &user_event(1, "u1", "Ada"));
    write_event_file(&events, "002.json", &user_event(2, "u1", "Ada L."));

    let src: Box<dyn EventSource> = Box::new(DirectorySource::new("dir", &events, &state).unwrap());
    let report = run_once(&db, vec![src], None).unwrap();
    assert!(report.batches_committed >= 1);

    // Restart.
    let mut src2 = DirectorySource::new("dir", &events, &state).unwrap();
    assert_eq!(
        src2.committed_position(),
        Some(&SourcePosition("002.json".to_string()))
    );
    assert!(
        src2.poll(256).unwrap().is_empty(),
        "committed files must not be re-polled"
    );

    // A new file lands → it, and only it, is polled.
    write_event_file(&events, "003.json", &user_event(3, "u1", "Ada Lovelace"));
    let next = src2.poll(256).unwrap();
    assert_eq!(next.len(), 1);
    assert_eq!(next[0].position, SourcePosition("003.json".to_string()));
}
