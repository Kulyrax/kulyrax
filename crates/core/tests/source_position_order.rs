//! F-05: a source's committed position never falls behind the events it has
//! delivered, and each source orders its positions by its own rule, not as
//! text (decision 2026-11-25-source-position-contract).
//!
//! Directory: a split file's positions are `<file>#<index>`. A later file whose
//! name continues the split file's name with a character below `#` (a space,
//! `!`, `"`) sorts *before* them as text, although it is delivered after them.
//! Up to 0.12.1 its commit was ignored and it was delivered on every poll.

mod common;

use common::user_event;
use kulyrax_core::source::directory::DirectorySource;
use kulyrax_core::source::stdin::StdinSource;
use kulyrax_core::source::{EventSource, SourcePosition};
use std::io::Cursor;

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

/// `"` is not allowed in Windows file names.
#[cfg(not(windows))]
#[test]
fn a_file_named_with_a_quote_after_a_split_file_is_committed() {
    a_file_after_a_split_file_is_committed('"');
}

/// The control: a separator above `#` worked in 0.12.1 too.
#[test]
fn a_file_named_with_a_dash_after_a_split_file_is_committed() {
    a_file_after_a_split_file_is_committed('-');
}

/// A directory position committed earlier, by the source's own order, is
/// ignored: a whole file, then an earlier part of a file, then an earlier file.
#[test]
fn a_directory_commit_never_moves_backward() {
    let tmp = tempdir().unwrap();
    let dir = tmp.path().join("events");
    std::fs::create_dir_all(&dir).unwrap();
    let state = tmp.path().join("state");
    let mut src = DirectorySource::new("dir", &dir, &state).unwrap();
    let at = |p: &str| SourcePosition(p.into());

    src.commit(at("b.ndjson#000000000004")).unwrap();
    for earlier in ["b.ndjson#000000000003", "a.ndjson", "a.ndjson b.ndjson"] {
        src.commit(at(earlier)).unwrap();
        assert_eq!(
            src.committed_position(),
            Some(&at("b.ndjson#000000000004")),
            "{earlier:?} is before b.ndjson#4"
        );
    }
    // Later: a later part, the whole file, then a later file.
    for later in ["b.ndjson#000000000005", "b.ndjson", "b.ndjson c.ndjson"] {
        src.commit(at(later)).unwrap();
        assert_eq!(src.committed_position(), Some(&at(later)));
    }
}

fn stdin_lines(n: u64) -> String {
    (0..n)
        .map(|i| serde_json::to_string(&user_event(1, &format!("s{i}"), "x")).unwrap() + "\n")
        .collect()
}

/// Stdin orders positions by line number: committing an earlier line is
/// ignored, and the source resumes after the latest.
#[test]
fn a_stdin_commit_never_moves_backward_and_resumes_after_it() {
    let tmp = tempdir().unwrap();
    let state = tmp.path().join("state");
    let mut src = StdinSource::new("in", &state, Cursor::new(stdin_lines(4))).unwrap();
    let batch = src.poll(3).unwrap();
    let last = batch.last().unwrap().position.clone();
    src.commit(last.clone()).unwrap();
    src.commit(batch[0].position.clone()).unwrap();
    assert_eq!(src.committed_position(), Some(&last));

    let mut src = StdinSource::new("in", &state, Cursor::new(stdin_lines(4))).unwrap();
    assert_eq!(ids(&src.poll(10).unwrap()), ["evt-s3-1"]);
}

/// A stdin checkpoint that names no line is an error, not line 0.
#[test]
fn a_stdin_checkpoint_that_is_not_a_line_number_is_refused() {
    let tmp = tempdir().unwrap();
    let state = tmp.path().join("state");
    kulyrax_core::source::checkpoint::write_checkpoint(&state, "in", &SourcePosition("x".into()))
        .unwrap();
    let err = StdinSource::new("in", &state, Cursor::new(String::new()))
        .err()
        .expect("refused");
    assert!(err.to_string().contains("not a stdin line number"), "{err}");
}

/// Hands out one batch with positions `8`, `9`, `10` (numeric offsets, not
/// text-ordered), then nothing; records every commit.
struct Offsets {
    polled: bool,
    commits: std::sync::Arc<std::sync::Mutex<Vec<String>>>,
    committed: Option<SourcePosition>,
}

impl EventSource for Offsets {
    fn id(&self) -> &str {
        "offsets"
    }

    fn poll(
        &mut self,
        _max_batch: usize,
    ) -> Result<Vec<kulyrax_core::SourcedEvent>, kulyrax_core::SourceError> {
        if std::mem::replace(&mut self.polled, true) {
            return Ok(Vec::new());
        }
        Ok([8u64, 9, 10]
            .into_iter()
            .map(|i| {
                kulyrax_core::SourcedEvent::new(
                    user_event(1, &format!("o{i}"), "x"),
                    SourcePosition(i.to_string()),
                )
            })
            .collect())
    }

    fn commit(&mut self, position: SourcePosition) -> Result<(), kulyrax_core::SourceError> {
        self.commits.lock().unwrap().push(position.0.clone());
        self.committed = Some(position);
        Ok(())
    }

    fn committed_position(&self) -> Option<&SourcePosition> {
        self.committed.as_ref()
    }
}

/// The run loop commits a batch at its last event in delivery order (`10`),
/// not at the text-wise maximum (`9`), and reports the batch as `8..10`.
#[test]
fn the_run_loop_commits_at_the_last_event_in_delivery_order() {
    let tmp = tempdir().unwrap();
    let db = tmp.path().join("app.db");
    common::init_db(&db);
    let commits = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    let source = Offsets {
        polled: false,
        commits: std::sync::Arc::clone(&commits),
        committed: None,
    };
    let report = common::run_once(&db, vec![Box::new(source)], None).unwrap();
    assert_eq!(*commits.lock().unwrap(), ["10"]);
    let batch = &report.batches[0];
    assert_eq!(batch.position_from.as_deref(), Some("8"));
    assert_eq!(batch.position_to, "10");
}
