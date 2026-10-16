//! Phase 17.6: NDJSON on stdin produces the same projection as the equivalent
//! `directory` source over the same events.

mod common;

use common::*;
use kulyrax_core::source::EventSource;
use kulyrax_core::source::directory::DirectorySource;
use kulyrax_core::source::stdin::StdinSource;

use std::io::Cursor;
use tempfile::tempdir;

fn events() -> Vec<kulyrax_core::event::Event> {
    vec![
        user_event(1, "u1", "Ada"),
        user_event(1, "u2", "Grace"),
        user_event(2, "u1", "Ada L."),
        user_event(3, "u1", "Ada Lovelace"),
    ]
}

#[test]
fn stdin_and_directory_converge() {
    // stdin
    let stdin_dir = tempdir().unwrap();
    let stdin_db = stdin_dir.path().join("s.db");
    init_db(&stdin_db);
    let ndjson: String = events()
        .iter()
        .map(|e| serde_json::to_string(e).unwrap())
        .collect::<Vec<_>>()
        .join("\n");
    let src: Box<dyn EventSource> = Box::new(
        StdinSource::new(
            "in",
            stdin_dir.path().join("state"),
            Cursor::new(ndjson.into_bytes()),
        )
        .unwrap(),
    );
    run_once(&stdin_db, vec![src], None).unwrap();

    // directory
    let dir_dir = tempdir().unwrap();
    let dir_db = dir_dir.path().join("d.db");
    init_db(&dir_db);
    let ev = dir_dir.path().join("events");
    for (i, e) in events().iter().enumerate() {
        write_event_file(&ev, &format!("{i:03}.json"), e);
    }
    let src: Box<dyn EventSource> =
        Box::new(DirectorySource::new("dir", &ev, dir_dir.path().join("state")).unwrap());
    run_once(&dir_db, vec![src], None).unwrap();

    assert_eq!(read_user(&stdin_db, "u1"), read_user(&dir_db, "u1"));
    assert_eq!(read_user(&stdin_db, "u2"), read_user(&dir_db, "u2"));
    assert_eq!(
        read_user(&stdin_db, "u1"),
        Some(("Ada Lovelace".to_string(), 3))
    );
}
