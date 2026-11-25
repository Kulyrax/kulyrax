//! F-05: a source's committed position must never fall behind the events it
//! has delivered. The directory source compares positions as text. A split
//! file's positions are `<file>#<index>`, so a later file whose name continues
//! the split file's name with a character below `#` (a space, `!`, `"`) sorts
//! *before* the committed position, although it is delivered after it.

mod common;

use common::user_event;
use kulyrax_core::source::EventSource;
use kulyrax_core::source::directory::DirectorySource;

use tempfile::tempdir;

fn ids(batch: &[kulyrax_core::SourcedEvent]) -> Vec<String> {
    batch.iter().map(|se| se.event.event_id.clone()).collect()
}

fn ndjson(ids: &[&str]) -> String {
    ids.iter()
        .map(|id| serde_json::to_string(&user_event(1, id, "x")).unwrap() + "\n")
        .collect()
}

/// `a.ndjson` (taken in parts), then `a.ndjson<c>b.ndjson`, listed after it.
fn a_file_after_a_split_file_is_committed(separator: char) {
    let tmp = tempdir().unwrap();
    let dir = tmp.path().join("events");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("a.ndjson"), ndjson(&["a1", "a2", "a3"])).unwrap();
    let next = format!("a.ndjson{separator}b.ndjson");
    std::fs::write(dir.join(&next), ndjson(&["b1"])).unwrap();
    let state = tmp.path().join("state");
    let mut src = DirectorySource::new("dir", &dir, &state).unwrap();

    let mut delivered = Vec::new();
    // Each batch is committed at its last event, as the run loop does.
    for _ in 0..6 {
        let batch = src.poll(2).unwrap();
        let Some(last) = batch.last() else { break };
        src.commit(last.position.clone()).unwrap();
        delivered.extend(ids(&batch));
    }
    assert_eq!(
        delivered,
        ["evt-a1-1", "evt-a2-1", "evt-a3-1", "evt-b1-1"],
        "{next:?} is delivered once, then the source is caught up (committed: {:?})",
        src.committed_position()
    );
    let reopened = DirectorySource::new("dir", &dir, &state)
        .unwrap()
        .poll(10)
        .unwrap();
    assert!(
        reopened.is_empty(),
        "after a restart nothing is delivered again: {:?}",
        ids(&reopened)
    );
}

#[test]
fn a_file_named_with_a_space_after_a_split_file_is_committed() {
    a_file_after_a_split_file_is_committed(' ');
}

#[test]
fn a_file_named_with_an_exclamation_mark_after_a_split_file_is_committed() {
    a_file_after_a_split_file_is_committed('!');
}

/// The control: a separator above `#` works today.
#[test]
fn a_file_named_with_a_dash_after_a_split_file_is_committed() {
    a_file_after_a_split_file_is_committed('-');
}
