//! Phase 2: the directory source splits a file larger than the batch. Its
//! events' positions are `<file>#<index>`, so a restart resumes inside the
//! file, and once the file is done the next file follows.

mod common;

use common::user_event;
use kulyrax_core::source::directory::DirectorySource;
use kulyrax_core::source::{EventSource, SourcePosition};

use tempfile::tempdir;

fn ids(batch: &[kulyrax_core::SourcedEvent]) -> Vec<String> {
    batch.iter().map(|se| se.event.event_id.clone()).collect()
}

fn max_position(batch: &[kulyrax_core::SourcedEvent]) -> SourcePosition {
    batch.iter().map(|se| se.position.clone()).max().unwrap()
}

#[test]
fn large_file_is_split_and_resumed_across_restarts() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path().join("events");
    std::fs::create_dir_all(&dir).unwrap();
    let a: String = (1..=5)
        .map(|i| serde_json::to_string(&user_event(i, &format!("a{i}"), "x")).unwrap() + "\n")
        .collect();
    std::fs::write(dir.join("a.ndjson"), a).unwrap();
    std::fs::write(
        dir.join("b.ndjson"),
        serde_json::to_string(&user_event(1, "b1", "x")).unwrap() + "\n",
    )
    .unwrap();
    let state = tmp.path().join("state");
    let open = || DirectorySource::new("dir", &dir, &state).unwrap();

    let mut src = open();
    let first = src.poll(2).unwrap();
    assert_eq!(ids(&first), ["evt-a1-1", "evt-a2-2"]);
    assert_eq!(max_position(&first).0, "a.ndjson#000000000001");
    src.commit(max_position(&first)).unwrap();

    // A restart resumes inside the file.
    let mut src = open();
    let second = src.poll(2).unwrap();
    assert_eq!(ids(&second), ["evt-a3-3", "evt-a4-4"]);
    src.commit(max_position(&second)).unwrap();

    // The file's last part, then the next file whole.
    let third = src.poll(2).unwrap();
    assert_eq!(ids(&third), ["evt-a5-5"]);
    src.commit(max_position(&third)).unwrap();
    let fourth = src.poll(2).unwrap();
    assert_eq!(ids(&fourth), ["evt-b1-1"]);
    assert_eq!(max_position(&fourth).0, "b.ndjson");
    src.commit(max_position(&fourth)).unwrap();

    assert!(open().poll(2).unwrap().is_empty(), "everything committed");
}

#[test]
fn whole_files_that_fit_keep_their_plain_position() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path().join("events");
    std::fs::create_dir_all(&dir).unwrap();
    for (name, id) in [("a.json", "a"), ("b.json", "b")] {
        std::fs::write(
            dir.join(name),
            serde_json::to_string(&user_event(1, id, "x")).unwrap(),
        )
        .unwrap();
    }
    let mut src = DirectorySource::new("dir", &dir, tmp.path().join("state")).unwrap();
    let batch = src.poll(10).unwrap();
    assert_eq!(ids(&batch), ["evt-a-1", "evt-b-1"]);
    assert_eq!(max_position(&batch).0, "b.json");
}
