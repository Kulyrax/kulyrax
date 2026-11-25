//! One `.kulyrax/` shared by its three owners: a file store whose root is the
//! config folder, `kulyrax reload`'s control files, and the source checkpoints
//! in `state_dir: <config folder>/.kulyrax/state`. They run together, then the
//! loop restarts: every owner finds its own entries and nothing else.

use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kulyrax_core::kulyrax_dir;
use kulyrax_core::pipeline::{request_reload, run_source_loop};
use kulyrax_core::runtime::ReloadOutcome;
use kulyrax_core::{RunMode, SourceRunOptions};

const TIMEOUT: Duration = Duration::from_secs(20);

fn slash(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

fn write_mapping(root: &Path, value: &str) {
    fs::write(
        root.join("mappings/keyvalue/user.yaml"),
        format!(
            "event: UserRegistered\nnamespace: users\nkey: payload.id\nversion: 1\n\
             on_existing: replace\nvalue:\n{value}"
        ),
    )
    .expect("mapping");
}

fn add_event(root: &Path, file: &str, id: &str, seq: u64) {
    let line = format!(
        r#"{{"event_id":"{file}","event_type":"UserRegistered","payload":"{{\"id\":\"{id}\",\"name\":\"N-{id}-{seq}\",\"email\":\"{id}@x\"}}","metadata":{{}},"version":1,"sequence":{seq}}}"#
    );
    fs::write(root.join("events").join(file), format!("{line}\n")).expect("event");
}

fn value(root: &Path, id: &str) -> Option<serde_json::Value> {
    fs::read_to_string(root.join("users").join(format!("{id}.json")))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
}

fn wait_for(root: &Path, id: &str, want: serde_json::Value) {
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        if value(root, id).as_ref() == Some(&want) {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("{id} never became {want}: {:?}", value(root, id));
}

fn entries(root: &Path) -> BTreeSet<String> {
    fs::read_dir(root.join(kulyrax_dir::DIR))
        .expect(".kulyrax/")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect()
}

fn run(
    root: &Path,
    mode: RunMode,
    stop: &Arc<AtomicBool>,
) -> thread::JoinHandle<kulyrax_core::SourceRunReport> {
    let (root, stop) = (root.to_path_buf(), Arc::clone(stop));
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    thread::spawn(move || {
        let opts = {
            let mut o = SourceRunOptions::default();
            o.mode = mode;
            o
        };
        run_source_loop(
            &root.join("config.yaml"),
            &root.join("mappings"),
            &opts,
            &stop,
            |_runtime| {},
            move |outcome| outcomes.lock().expect("lock").push(outcome.clone()),
        )
        .expect("loop ran cleanly")
    })
}

#[test]
fn a_store_reload_and_checkpoints_share_one_kulyrax_dir() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    for d in [
        "mappings/keyvalue",
        "mappings/sql",
        "mappings/document",
        "events",
    ] {
        fs::create_dir_all(root.join(d)).expect("dir");
    }
    // Store root = config folder = the parent of `state_dir`.
    fs::write(
        root.join("config.yaml"),
        format!(
            "version: 1\nrouting:\n  file: routing.yaml\nadapters:\n  - type: keyvalue\n    \
             id: kv\n    priority: 10\n    capabilities: [write, upsert, delete]\n    config:\n      \
             root: {root}\nsources:\n  - type: \
             directory\n    id: events\n    path: {root}/events\n    state_dir: {root}/.kulyrax/state\n",
            root = slash(root)
        ),
    )
    .expect("config");
    fs::write(root.join("routing.yaml"), "UserRegistered: [kv]\n").expect("routing");
    write_mapping(root, "  name: payload.name\n");

    let stop = Arc::new(AtomicBool::new(false));
    let running = run(
        root,
        RunMode::Continuous {
            poll_interval: Duration::from_millis(50),
        },
        &stop,
    );
    add_event(root, "0001.ndjson", "u1", 5);
    wait_for(root, "u1", serde_json::json!({"name": "N-u1-5"}));

    write_mapping(root, "  name: payload.name\n  email: payload.email\n");
    let applied = request_reload(&root.join("config.yaml"), TIMEOUT).expect("request");
    assert!(
        matches!(applied, ReloadOutcome::Applied { .. }),
        "{applied:?}"
    );
    add_event(root, "0002.ndjson", "u2", 3);
    wait_for(
        root,
        "u2",
        serde_json::json!({"name": "N-u2-3", "email": "u2@x"}),
    );
    stop.store(true, Ordering::Relaxed);
    let report = running.join().expect("loop thread");
    assert_eq!(report.events_failed, 0);

    // Every owner's entries, side by side; no store wrote into another's.
    let found = entries(root);
    for e in [kulyrax_dir::CONTROL, kulyrax_dir::STATE, kulyrax_dir::KV] {
        assert!(found.contains(e), "{e} missing: {found:?}");
    }
    for e in &found {
        assert!(
            kulyrax_dir::STORE_ENTRIES.contains(&e.as_str())
                || kulyrax_dir::RESERVED_ENTRIES.contains(&e.as_str()),
            "unowned entry {e}: {found:?}"
        );
    }
    let checkpoints: Vec<_> = fs::read_dir(root.join(".kulyrax/state"))
        .expect("state")
        .map(|e| e.expect("entry").file_name().to_string_lossy().into_owned())
        .collect();
    assert!(
        checkpoints.iter().any(|n| n.starts_with("events")),
        "{checkpoints:?}"
    );

    // Restart: the checkpoint resumes after 0002, the guards are found (an
    // older u1 is stale), and a newer event applies.
    add_event(root, "0003.ndjson", "u1", 2);
    add_event(root, "0004.ndjson", "u2", 9);
    let stop = Arc::new(AtomicBool::new(false));
    let report = run(root, RunMode::Once, &stop).join().expect("loop thread");
    assert_eq!(report.events_processed, 2, "{report:?}");
    assert_eq!(report.events_failed, 0);
    assert_eq!(
        value(root, "u1"),
        Some(serde_json::json!({"name": "N-u1-5"}))
    );
    assert_eq!(
        value(root, "u2"),
        Some(serde_json::json!({"name": "N-u2-9", "email": "u2@x"}))
    );
    assert_eq!(entries(root), found);
}
