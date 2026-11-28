//! The CLI's exit codes (`docs/stability.md` §3), each through the binary:
//! 0 ok, 1 internal, 2 invalid input, 3 events failed or dead-lettered,
//! 4 refused by a safety check. SQLite and file stores only; no Docker.

use std::path::{Path, PathBuf};
use std::process::Command;

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kulyrax"))
}

fn write(path: PathBuf, s: &str) {
    std::fs::create_dir_all(path.parent().expect("parent")).expect("mkdir");
    std::fs::write(path, s).expect("write");
}

const GOOD: &str = r#"{"event_id":"e1","event_type":"U","payload":"{\"id\":\"u1\",\"name\":\"Ada\"}","metadata":{},"version":1,"sequence":1}"#;
/// The payload is not JSON, so the SQL adapter fails the event.
const BAD: &str = r#"{"event_id":"e2","event_type":"U","payload":"not json","metadata":{},"version":1,"sequence":2}"#;

/// A SQLite project projecting `U` into `table`, with a directory source
/// holding `events`.
fn project(table: &str, events: &[&str]) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let p = dir.path();
    write(
        p.join("config.yaml"),
        "version: 1\nrouting:\n  file: routing.yaml\nadapters:\n  - type: sqlite\n    id: sql\n    priority: 10\n    config:\n      path: app.db\nsources:\n  - type: directory\n    id: in\n    path: events\n    state_dir: state\n",
    );
    write(p.join("routing.yaml"), "U: [sql]\n");
    write(
        p.join("mappings/sql/u.yaml"),
        &format!(
            "event: U\ntable: {table}\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n  name: payload.name\n"
        ),
    );
    std::fs::create_dir_all(p.join("mappings/document")).expect("mkdir");
    std::fs::create_dir_all(p.join("events")).expect("mkdir");
    for (i, e) in events.iter().enumerate() {
        write(p.join(format!("events/{i:03}.json")), e);
    }
    // Kulyrax projects into tables the user owns; it does not create them.
    rusqlite::Connection::open(p.join("app.db"))
        .and_then(|c| {
            c.execute_batch(&format!(
                "CREATE TABLE \"{table}\" (id TEXT PRIMARY KEY, name TEXT)"
            ))
        })
        .expect("create the table");
    dir
}

/// Run `kulyrax <args>` in `dir`; its exit code and its output.
fn kulyrax(dir: &Path, args: &[&str]) -> (Option<i32>, String) {
    let out = Command::new(bin())
        .args(args)
        .current_dir(dir)
        .output()
        .expect("run kulyrax");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    (out.status.code(), text)
}

const RUN_ONCE: &[&str] = &[
    "run",
    "--config",
    "config.yaml",
    "--mappings",
    "mappings",
    "--once",
];

fn assert_code(got: (Option<i32>, String), want: i32) {
    assert_eq!(got.0, Some(want), "{}", got.1);
}

#[test]
fn exit_0_when_every_event_projects() {
    let p = project("users", &[GOOD]);
    assert_code(kulyrax(p.path(), RUN_ONCE), 0);
}

#[test]
fn exit_1_on_an_internal_error() {
    // `--event` names a directory: reading it is an I/O error.
    let p = project("users", &[GOOD]);
    let args = [
        "run",
        "--config",
        "config.yaml",
        "--mappings",
        "mappings",
        "--event",
        "events",
    ];
    assert_code(kulyrax(p.path(), &args), 1);
}

#[test]
fn exit_2_on_invalid_input() {
    let p = project("users", &[GOOD]);
    // Bad arguments (clap).
    assert_code(kulyrax(p.path(), &["run", "--no-such-flag"]), 2);

    // A misspelt config key.
    let config = std::fs::read_to_string(p.path().join("config.yaml")).expect("read");
    write(
        p.path().join("bad.yaml"),
        &config.replace("priority: 10", "priority: 10\n    depends_om: []"),
    );
    let args = [
        "run",
        "--config",
        "bad.yaml",
        "--mappings",
        "mappings",
        "--once",
    ];
    assert_code(kulyrax(p.path(), &args), 2);

    // An unknown source type.
    write(
        p.path().join("bad.yaml"),
        &config.replace("type: directory", "type: no_such_source"),
    );
    assert_code(kulyrax(p.path(), &args), 2);

    // A rebuild of an adapter that is not in the config.
    let args = [
        "rebuild",
        "--config",
        "config.yaml",
        "--mappings",
        "mappings",
        "--events",
        "events",
        "--adapter",
        "nope",
    ];
    assert_code(kulyrax(p.path(), &args), 2);
}

#[test]
fn exit_3_when_events_fail_or_are_dead_lettered() {
    let p = project("users", &[GOOD, BAD]);
    // Failed, with no DLQ.
    assert_code(kulyrax(p.path(), RUN_ONCE), 3);

    // Dead-lettered: handled, but `--once` still reports it.
    let p = project("users", &[GOOD, BAD]);
    let mut args = RUN_ONCE.to_vec();
    args.extend(["--dlq", "dlq"]);
    assert_code(kulyrax(p.path(), &args), 3);

    // One event (`run --event`), a replay, a dry run, a rebuild.
    let p = project("users", &[GOOD, BAD]);
    write(p.path().join("bad.json"), BAD);
    let base = ["--config", "config.yaml", "--mappings", "mappings"];
    let with = |cmd: &str, extra: &[&str]| {
        let mut a = vec![cmd];
        a.extend(base);
        a.extend(extra);
        kulyrax(p.path(), &a)
    };
    assert_code(with("run", &["--event", "bad.json"]), 3);
    assert_code(with("replay", &["--events", "events"]), 3);
    assert_code(with("dry-run", &["--event", "bad.json"]), 3);
    assert_code(
        with("rebuild", &["--events", "events", "--adapter", "sql"]),
        3,
    );
}

#[test]
fn exit_4_when_a_safety_check_refuses() {
    // The source's lock is held by another process (here, this test). The
    // outbox source takes its lock before it connects, so no database is
    // needed: the URL is never used.
    let p = project("users", &[GOOD]);
    let config = std::fs::read_to_string(p.path().join("config.yaml")).expect("read");
    let outbox = "  - type: postgres_outbox\n    id: o\n    state_dir: state\n    config:\n      url: postgres://app@127.0.0.1:1/app\n      table: outbox\n      sequence_column: id\n";
    let (head, _) = config.split_once("  - type: directory").expect("sources");
    write(p.path().join("config.yaml"), &format!("{head}{outbox}"));
    std::fs::create_dir_all(p.path().join("state")).expect("mkdir");
    let lock = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(p.path().join("state/o.lock"))
        .expect("lock file");
    lock.try_lock().expect("take the lock");
    let held = kulyrax(p.path(), RUN_ONCE);
    assert!(
        held.1.contains("is in use by another process"),
        "{}",
        held.1
    );
    assert_code(held, 4);
    drop(lock);

    // A rebuild whose mapped table is named like a shadow table.
    let p = project("users__kulyrax_rebuild", &[GOOD]);
    let args = [
        "rebuild",
        "--config",
        "config.yaml",
        "--mappings",
        "mappings",
        "--events",
        "events",
        "--adapter",
        "sql",
    ];
    assert_code(kulyrax(p.path(), &args), 4);

    // `init` would overwrite a file.
    let p = project("users", &[GOOD]);
    assert_code(kulyrax(p.path(), &["init", "."]), 4);
}

/// A source checkpoint a newer Kulyrax wrote: the start is refused before
/// any event is read, and the checkpoint is kept.
#[test]
fn exit_4_when_a_checkpoint_is_newer() {
    let p = project("users", &[GOOD]);
    let cp = p.path().join("state/in.json");
    let newer = r#"{"format": 2, "position": "000.json#0", "committed_at": "0"}"#;
    write(cp.clone(), newer);
    let got = kulyrax(p.path(), RUN_ONCE);
    assert!(got.1.contains("written by a newer Kulyrax"), "{}", got.1);
    assert_code(got, 4);
    assert_eq!(std::fs::read_to_string(&cp).expect("kept"), newer);
    let rows: i64 = rusqlite::Connection::open(p.path().join("app.db"))
        .and_then(|c| c.query_row("SELECT count(*) FROM users", [], |r| r.get(0)))
        .expect("count");
    assert_eq!(rows, 0, "nothing was projected");
}

/// A file store whose batch log a newer Kulyrax wrote (magic `KLXWAL02`), or
/// that is not a Kulyrax batch log at all: the start is refused and the log
/// is kept. Before step 5 the newest such log was deleted as torn.
#[test]
fn exit_4_when_a_batch_log_is_not_this_versions() {
    for (magic, says) in [
        (&b"KLXWAL02"[..], "written by a newer Kulyrax"),
        (
            &b"NOTALOG!"[..],
            "not in a format this version of Kulyrax knows",
        ),
    ] {
        let p = project("users", &[GOOD]);
        let config = std::fs::read_to_string(p.path().join("config.yaml")).expect("read");
        let doc = "  - type: file\n    id: doc\n    priority: 20\n    config:\n      root: store\nsources:";
        write(
            p.path().join("config.yaml"),
            &config.replacen("sources:", doc, 1),
        );
        let log = p.path().join("store/.kulyrax/wal/0000000000000001.log");
        let mut bytes = magic.to_vec();
        bytes.extend_from_slice(&[0; 12]);
        std::fs::create_dir_all(log.parent().expect("parent")).expect("mkdir");
        std::fs::write(&log, &bytes).expect("write the log");

        let got = kulyrax(p.path(), RUN_ONCE);
        assert!(got.1.contains(says), "{}", got.1);
        assert_code(got, 4);
        assert_eq!(std::fs::read(&log).expect("the log is kept"), bytes);
    }
}
