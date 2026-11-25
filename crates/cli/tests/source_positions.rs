//! F-05 through the binary: `kulyrax run --once` finishes when a directory
//! holds a file taken in parts and then a file whose name continues it with a
//! space. Up to 0.12.1 the second file's commit was ignored (text order put it
//! before the split file's positions), so it was delivered on every poll and
//! `--once` never returned.

use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

fn write(path: &Path, content: &str) {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).unwrap();
    }
    std::fs::write(path, content).unwrap();
}

fn event(id: &str) -> String {
    serde_json::json!({
        "event_id": format!("e-{id}"),
        "event_type": "UserCreated",
        "payload": serde_json::json!({ "id": id, "name": id }).to_string(),
        "metadata": {},
        "version": 1,
        "sequence": 1,
    })
    .to_string()
        + "\n"
}

#[test]
fn run_once_finishes_after_a_split_file_and_a_file_named_after_it() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path();
    write(
        &p.join("kulyrax.yaml"),
        "version: 1
routing:
  file: routing.yaml
adapters:
  - type: keyvalue
    id: kv
    priority: 10
    config:
      root: ./store
    capabilities: [write, upsert]
sources:
  - type: directory
    id: events
    path: ./events
    state_dir: ./state
",
    );
    write(&p.join("routing.yaml"), "UserCreated: [kv]\n");
    write(
        &p.join("mappings/keyvalue/created.yaml"),
        "event: UserCreated\nnamespace: users\nkey: payload.id\nversion: 1\nvalue:\n  name: payload.name\n",
    );
    for kind in ["sql", "document", "graph"] {
        std::fs::create_dir_all(p.join("mappings").join(kind)).unwrap();
    }
    write(
        &p.join("events/events.ndjson"),
        &(event("u1") + &event("u2") + &event("u3")),
    );
    write(&p.join("events/events.ndjson 2.ndjson"), &event("u4"));

    let mut child = Command::new(env!("CARGO_BIN_EXE_kulyrax"))
        .current_dir(p)
        .args(["run", "--config", "kulyrax.yaml", "--mappings", "mappings"])
        .args(["--once", "--max-batch", "2"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(60);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() > deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("`kulyrax run --once` did not finish within 60 s");
        }
        std::thread::sleep(Duration::from_millis(50));
    };
    assert!(status.success(), "{status}");

    let checkpoint: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(p.join("state/events.json")).unwrap())
            .unwrap();
    assert_eq!(checkpoint["position"], "events.ndjson 2.ndjson");
}
