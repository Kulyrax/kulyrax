//! Phase 17.6: two directory sources feeding disjoint entity sets → both
//! project, and each source's committed position is tracked independently.

mod common;

use common::*;
use kulyrax_core::source::directory::DirectorySource;
use kulyrax_core::source::{EventSource, SourcePosition};
use kulyrax_core::{RunMode, SourceRunOptions, run_sources};

use std::collections::HashMap;
use std::sync::atomic::AtomicBool;
use tempfile::tempdir;

#[test]
fn two_sources_project_and_checkpoint_independently() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("test.db");
    init_db(&db);

    let a_events = dir.path().join("a");
    write_event_file(&a_events, "01.json", &user_event(1, "u1", "Ada"));
    write_event_file(&a_events, "02.json", &user_event(2, "u1", "Ada L."));

    let b_events = dir.path().join("b");
    write_event_file(&b_events, "01.json", &user_event(1, "u2", "Grace"));

    let state = dir.path().join("state");
    let sources: Vec<Box<dyn EventSource>> = vec![
        Box::new(DirectorySource::new("src-a", &a_events, &state).unwrap()),
        Box::new(DirectorySource::new("src-b", &b_events, &state).unwrap()),
    ];

    let stop = AtomicBool::new(false);
    let opts = SourceRunOptions {
        mode: RunMode::Once,
        max_batch: 256,
        retry_budget: 0,
        dlq_dir: None,
    };
    run_sources(
        &config(&db),
        routing(),
        sql_mappings(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        sources,
        &opts,
        &stop,
    )
    .unwrap();

    assert_eq!(read_user(&db, "u1"), Some(("Ada L.".to_string(), 2)));
    assert_eq!(read_user(&db, "u2"), Some(("Grace".to_string(), 1)));

    let a = DirectorySource::new("src-a", &a_events, &state).unwrap();
    let b = DirectorySource::new("src-b", &b_events, &state).unwrap();
    assert_eq!(
        a.committed_position(),
        Some(&SourcePosition("02.json".to_string()))
    );
    assert_eq!(
        b.committed_position(),
        Some(&SourcePosition("01.json".to_string()))
    );
}
