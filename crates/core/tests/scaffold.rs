//! `init_project` writes a starter that loads, validates, and resolves its own
//! sample events; and it never overwrites an existing project.

use std::fs;

use kulyrax_core::event::Event;
use kulyrax_core::pipeline::{InitError, init_project};
use kulyrax_core::runtime::{DryRunOutcome, KulyraxRuntime};

#[test]
fn starter_project_loads_and_its_sample_events_resolve() {
    let dir = tempfile::tempdir().expect("tempdir");
    let project = dir.path().join("new-project");

    let written = init_project(&project).expect("init");
    assert!(written.iter().all(|p| p.exists()));
    assert!(
        project.join("mappings/sql").is_dir(),
        "sql/ must exist for loading"
    );

    let runtime = KulyraxRuntime::load(&project.join("config.yaml"), &project.join("mappings"))
        .expect("starter config, routing and mappings validate");

    let events = fs::read_to_string(project.join("events/0001-users.ndjson")).expect("events");
    let mut ids = Vec::new();
    for line in events.lines() {
        let event: Event = serde_json::from_str(line).expect("sample event parses");
        let report = runtime.dry_run_once(&event).expect("dry run");
        assert_eq!(report.adapters.len(), 1);
        match &report.adapters[0].outcome {
            DryRunOutcome::Planned(w) => {
                assert_eq!(w.target, "users");
                ids.push(w.entity_key.clone());
            }
            other => panic!("expected a planned write, got {other:?}"),
        }
    }
    assert_eq!(ids, ["u1", "u2"]);
}

#[test]
fn init_refuses_to_overwrite_and_writes_nothing() {
    let dir = tempfile::tempdir().expect("tempdir");
    fs::write(dir.path().join("config.yaml"), "mine").expect("existing config");

    let err = init_project(dir.path()).expect_err("must refuse");
    match err {
        InitError::WouldOverwrite(paths) => {
            assert_eq!(paths, [dir.path().join("config.yaml")]);
        }
        other => panic!("expected WouldOverwrite, got {other}"),
    }
    assert_eq!(
        fs::read_to_string(dir.path().join("config.yaml")).expect("read"),
        "mine"
    );
    assert!(!dir.path().join("routing.yaml").exists());
    assert!(!dir.path().join("mappings").exists());
}

#[test]
fn init_works_in_an_existing_empty_folder() {
    let dir = tempfile::tempdir().expect("tempdir");
    init_project(dir.path()).expect("init into an existing folder");
    assert!(dir.path().join("config.yaml").exists());
}
