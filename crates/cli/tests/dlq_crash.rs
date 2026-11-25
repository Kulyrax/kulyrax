//! A dead-lettered event is never lost by a crash: its DLQ file is durable
//! before the batch's checkpoint is committed. Killed anywhere between the
//! two, the next run reads the batch again and dead-letters it again (a second
//! file, `~2`, never an overwrite). After that run, every event that the
//! checkpoint has passed has a DLQ file.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kulyrax"))
}

const IDS: [&str; 3] = ["a.b", "a/b", "a_b"];

fn project() -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path();
    let write = |path: PathBuf, s: &str| {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, s).unwrap();
    };
    write(
        p.join("kulyrax.yaml"),
        "version: 1\nrouting:\n  file: routing.yaml\nadapters:\n  - type: sqlite\n    id: sql\n    priority: 10\n    config:\n      path: ./app.db\nsources:\n  - type: directory\n    id: events\n    path: ./events\n    state_dir: ./state\n",
    );
    write(p.join("routing.yaml"), "U: [sql]\n");
    // A table that does not exist: every event fails.
    write(
        p.join("mappings/sql/u.yaml"),
        "event: U\ntable: missing\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n",
    );
    std::fs::create_dir_all(p.join("mappings/document")).unwrap();
    let lines: Vec<String> = IDS
        .iter()
        .enumerate()
        .map(|(i, id)| {
            format!(
                r#"{{"event_id":"{id}","event_type":"U","metadata":{{}},"payload":"{{\"id\":\"x{i}\"}}","sequence":{},"version":1}}"#,
                i + 1
            )
        })
        .collect();
    write(p.join("events/0001.ndjson"), &(lines.join("\n") + "\n"));
    dir
}

fn run(p: &Path, crash_at: Option<&str>) -> Output {
    let mut cmd = Command::new(bin());
    cmd.current_dir(p)
        .args([
            "run",
            "--config",
            "kulyrax.yaml",
            "--mappings",
            "mappings",
            "--once",
        ])
        .args(["--dlq", "dlq"])
        .env_remove("KULYRAX_CRASH_AT")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(point) = crash_at {
        cmd.env("KULYRAX_CRASH_AT", point);
    }
    cmd.output().unwrap()
}

fn text(out: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// The event ids held by the DLQ's complete files, and how many files there
/// are (an empty file is a name claimed by a run that crashed before writing).
fn dlq(p: &Path) -> (BTreeSet<String>, usize, usize) {
    let mut ids = BTreeSet::new();
    let (mut full, mut empty) = (0, 0);
    for entry in std::fs::read_dir(p.join("dlq"))
        .into_iter()
        .flatten()
        .flatten()
    {
        let path = entry.path();
        if path.extension().is_none_or(|x| x != "json") {
            continue;
        }
        let raw = std::fs::read_to_string(&path).unwrap();
        if raw.is_empty() {
            empty += 1;
            continue;
        }
        let body: serde_json::Value = serde_json::from_str(&raw).unwrap();
        ids.insert(body["event"]["event_id"].as_str().unwrap().to_string());
        full += 1;
    }
    (ids, full, empty)
}

#[test]
fn a_crash_between_the_dlq_write_and_the_checkpoint_loses_no_event() {
    let all: BTreeSet<String> = IDS.iter().map(|s| s.to_string()).collect();
    for point in [
        "dlq-before-rename:1",
        "dlq-before-rename:2",
        "dlq-written:1",
        "dlq-written:3",
    ] {
        let dir = project();
        let p = dir.path();
        let crashed = run(p, Some(point));
        let name = point.split(':').next().unwrap();
        assert!(
            !crashed.status.success()
                && text(&crashed).contains(&format!("crash point {name} reached")),
            "{point} did not crash the run: {}",
            text(&crashed)
        );
        // Nothing was committed, so no event has been passed by the checkpoint.
        assert!(
            !p.join("state/events.json").exists(),
            "{point}: the checkpoint moved before the batch resolved"
        );

        let out = run(p, None);
        let (ids, full, empty) = dlq(p);
        assert!(
            p.join("state/events.json").exists(),
            "{point}: {}",
            text(&out)
        );
        assert_eq!(
            ids,
            all,
            "{point}: every event has a DLQ file\n{}",
            text(&out)
        );
        assert!(
            full >= IDS.len() && empty <= 1,
            "{point}: {full} files, {empty} empty"
        );

        // Caught up: a third run reads nothing and parks nothing new.
        let again = run(p, None);
        assert_eq!(dlq(p).1, full, "{point}: {}", text(&again));
    }
}
