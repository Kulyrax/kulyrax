//! Step 5 (decision 2026-11-28-format-versions): persisted formats carry a
//! version. A newer version is refused, an unversioned (0.12.x) one is read
//! as format 1, and what this version writes reads back unchanged.

use kulyrax_core::format::{FormatError, FormatProblem};
use kulyrax_core::source::checkpoint::{
    CHECKPOINT_FORMAT, checkpoint_path, read_checkpoint, write_checkpoint,
};
use kulyrax_core::source::{SourceError, SourcePosition};

/// What Kulyrax 0.12.1 wrote (`source/checkpoint.rs` at `v0.12.1`): no
/// `format` field.
const CHECKPOINT_0_12_1: &str = r#"{
  "position": "events/0003.json#1",
  "committed_at": "1764000000"
}"#;

#[test]
fn a_checkpoint_round_trips_with_its_format() {
    let dir = tempfile::tempdir().expect("tempdir");
    let position = SourcePosition("events/0003.json#1".to_string());
    write_checkpoint(dir.path(), "in", &position).expect("write");
    let text = std::fs::read_to_string(checkpoint_path(dir.path(), "in")).expect("read");
    let value: serde_json::Value = serde_json::from_str(&text).expect("json");
    assert_eq!(value["format"], CHECKPOINT_FORMAT, "{text}");
    let back = read_checkpoint(dir.path(), "in")
        .expect("read")
        .expect("some");
    assert_eq!(back.format, CHECKPOINT_FORMAT);
    assert_eq!(back.position, position);
}

#[test]
fn a_0_12_1_checkpoint_reads_as_format_1_and_is_stamped_on_commit() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(checkpoint_path(dir.path(), "in"), CHECKPOINT_0_12_1).expect("write");
    let cp = read_checkpoint(dir.path(), "in")
        .expect("read")
        .expect("some");
    assert_eq!(cp.format, 1);
    assert_eq!(cp.position.as_str(), "events/0003.json#1");

    write_checkpoint(dir.path(), "in", &cp.position).expect("commit");
    let text = std::fs::read_to_string(checkpoint_path(dir.path(), "in")).expect("read");
    assert!(text.contains("\"format\": 1"), "{text}");
}

fn refused(content: &str) -> FormatError {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = checkpoint_path(dir.path(), "in");
    std::fs::write(&path, content).expect("write");
    let err = read_checkpoint(dir.path(), "in").expect_err("refused");
    assert_eq!(
        std::fs::read_to_string(&path).expect("kept"),
        content,
        "the checkpoint is left as it is"
    );
    match err {
        SourceError::Format(e) => {
            assert!(e.store.ends_with("in.json"), "{}", e.store);
            e
        }
        other => panic!("not a format refusal: {other}"),
    }
}

#[test]
fn a_newer_checkpoint_is_refused() {
    let e = refused(r#"{"format": 2, "position": "p", "committed_at": "0", "more": 1}"#);
    assert_eq!(
        e.problem,
        FormatProblem::Newer {
            found: 2,
            supported: CHECKPOINT_FORMAT
        }
    );
    assert!(e.to_string().contains("newer Kulyrax"), "{e}");
}

#[test]
fn a_checkpoint_with_an_unknown_format_is_refused() {
    for content in [
        r#"{"format": "one", "position": "p", "committed_at": "0"}"#,
        r#"{"format": 0, "position": "p", "committed_at": "0"}"#,
        r#"{"format": -1, "position": "p", "committed_at": "0"}"#,
    ] {
        let e = refused(content);
        assert!(
            matches!(e.problem, FormatProblem::Unrecognized(_)),
            "{content}: {e}"
        );
    }
}
