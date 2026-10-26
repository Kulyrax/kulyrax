//! `kulyrax reload` against a real continuous source loop: an applied reload
//! changes how the next events project, and a rejected one leaves the loop
//! running on its previous configuration.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use kulyrax_core::pipeline::{request_reload, run_source_loop};
use kulyrax_core::runtime::ReloadOutcome;
use kulyrax_core::{RunMode, SourceRunOptions};

const TIMEOUT: Duration = Duration::from_secs(20);

fn slash(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

fn write_config(root: &Path, extra_source: &str) {
    let config = format!(
        "version: 1\nrouting:\n  file: routing.yaml\nadapters:\n  - type: file\n    id: docs\n    priority: 10\n    config:\n      root: {out}\nsources:\n  - type: directory\n    id: events\n    path: ./events\n    state_dir: ./.kulyrax/state\n{extra_source}",
        out = slash(&root.join("out")),
    );
    fs::write(root.join("config.yaml"), config).expect("config");
}

fn write_mapping(root: &Path, document: &str) {
    fs::write(
        root.join("mappings/document/user.yaml"),
        format!("event: UserRegistered\ncollection: users\nid: payload.id\nversion: 1\ndocument:\n{document}"),
    )
    .expect("mapping");
}

fn add_event(root: &Path, file: &str, id: &str) {
    let line = format!(
        r#"{{"event_id":"{id}","event_type":"UserRegistered","payload":"{{\"id\":\"{id}\",\"name\":\"N-{id}\",\"email\":\"{id}@x\"}}","metadata":{{}},"version":1,"sequence":1}}"#
    );
    fs::write(root.join("events").join(file), format!("{line}\n")).expect("event");
}

fn wait_for_doc(root: &Path, id: &str) -> serde_json::Value {
    let path: PathBuf = root.join("out").join("users").join(format!("{id}.json"));
    let deadline = Instant::now() + TIMEOUT;
    while Instant::now() < deadline {
        let doc = fs::read_to_string(&path)
            .ok()
            .and_then(|raw| serde_json::from_str(&raw).ok());
        if let Some(doc) = doc {
            return doc;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("{} was never projected", path.display());
}

#[test]
fn reload_applies_valid_changes_and_rejects_invalid_ones() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_path_buf();
    fs::create_dir_all(root.join("mappings/document")).expect("mappings");
    fs::create_dir_all(root.join("mappings/sql")).expect("sql dir");
    fs::create_dir_all(root.join("events")).expect("events");
    write_config(&root, "");
    fs::write(root.join("routing.yaml"), "UserRegistered: [docs]\n").expect("routing");
    write_mapping(&root, "  name: payload.name\n");

    let stop = Arc::new(AtomicBool::new(false));
    let outcomes = Arc::new(Mutex::new(Vec::new()));
    let loop_thread = {
        let (root, stop, outcomes) = (root.clone(), Arc::clone(&stop), Arc::clone(&outcomes));
        thread::spawn(move || {
            let opts = SourceRunOptions {
                mode: RunMode::Continuous {
                    poll_interval: Duration::from_millis(50),
                },
                ..SourceRunOptions::default()
            };
            run_source_loop(
                &root.join("config.yaml"),
                &root.join("mappings"),
                &opts,
                &stop,
                |_runtime| {},
                move |outcome| outcomes.lock().expect("lock").push(outcome.clone()),
            )
        })
    };
    let config = root.join("config.yaml");

    // 1. The starting mapping projects `name` only.
    add_event(&root, "0001.ndjson", "u1");
    assert_eq!(
        wait_for_doc(&root, "u1"),
        serde_json::json!({"name": "N-u1"})
    );

    // 2. A valid change: the next event projects `email` too.
    write_mapping(&root, "  name: payload.name\n  email: payload.email\n");
    let applied = request_reload(&config, TIMEOUT).expect("request");
    assert!(
        matches!(applied, ReloadOutcome::Applied { .. }),
        "{applied:?}"
    );
    add_event(&root, "0002.ndjson", "u2");
    assert_eq!(
        wait_for_doc(&root, "u2"),
        serde_json::json!({"name": "N-u2", "email": "u2@x"})
    );

    // 3. An invalid change is rejected and the loop keeps the last good setup.
    fs::write(root.join("routing.yaml"), "UserRegistered: docs\n").expect("broken routing");
    let rejected = request_reload(&config, TIMEOUT).expect("request");
    assert!(
        matches!(&rejected, ReloadOutcome::Rejected { reason } if reason.contains("routing")),
        "{rejected:?}"
    );
    add_event(&root, "0003.ndjson", "u3");
    assert_eq!(
        wait_for_doc(&root, "u3"),
        serde_json::json!({"name": "N-u3", "email": "u3@x"})
    );

    // 4. Changing `sources:` needs a restart.
    fs::write(root.join("routing.yaml"), "UserRegistered: [docs]\n").expect("routing");
    write_config(
        &root,
        "  - type: directory\n    id: more\n    path: ./more\n    state_dir: ./.kulyrax/more\n",
    );
    let sources_changed = request_reload(&config, TIMEOUT).expect("request");
    assert!(
        matches!(&sources_changed, ReloadOutcome::Rejected { reason } if reason.contains("sources")),
        "{sources_changed:?}"
    );

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let report = loop_thread
        .join()
        .expect("loop thread")
        .expect("loop ran cleanly");
    assert_eq!(report.events_failed, 0);
    assert_eq!(outcomes.lock().expect("lock").len(), 3);
}
