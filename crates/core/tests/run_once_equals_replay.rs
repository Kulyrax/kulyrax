//! Phase 17.6 (17.3): `run --once` over a directory produces the same
//! projected state as `replay` over the same events — the source loop subsumes
//! replay.

mod common;

use common::*;
use kulyrax_core::replay::{ReplayContext, events_from_path};
use kulyrax_core::source::EventSource;
use kulyrax_core::source::directory::DirectorySource;

use std::collections::HashMap;
use tempfile::tempdir;

fn fixture_events(dir: &std::path::Path) {
    for (i, (seq, id, name)) in [
        (1u64, "u1", "Ada"),
        (1, "u2", "Grace"),
        (2, "u1", "Ada L."),
        (3, "u1", "Ada Lovelace"),
        (2, "u2", "Grace Hopper"),
    ]
    .into_iter()
    .enumerate()
    {
        write_event_file(dir, &format!("{i:03}.json"), &user_event(seq, id, name));
    }
}

#[test]
fn run_once_and_replay_project_identically() {
    // run --once
    let a = tempdir().unwrap();
    let a_db = a.path().join("a.db");
    init_db(&a_db);
    let a_events = a.path().join("events");
    fixture_events(&a_events);
    let src: Box<dyn EventSource> =
        Box::new(DirectorySource::new("dir", &a_events, a.path().join("state")).unwrap());
    run_once(&a_db, vec![src], None).unwrap();

    // replay
    let b = tempdir().unwrap();
    let b_db = b.path().join("b.db");
    init_db(&b_db);
    let b_events = b.path().join("events");
    fixture_events(&b_events);
    let mut ctx = ReplayContext::new(
        &config(&b_db),
        routing(),
        sql_mappings(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
    );
    ctx.run_stream(events_from_path(&b_events).unwrap())
        .unwrap();

    assert_eq!(read_user(&a_db, "u1"), read_user(&b_db, "u1"));
    assert_eq!(read_user(&a_db, "u2"), read_user(&b_db, "u2"));
    assert_eq!(
        read_user(&a_db, "u1"),
        Some(("Ada Lovelace".to_string(), 3))
    );
    assert_eq!(
        read_user(&a_db, "u2"),
        Some(("Grace Hopper".to_string(), 2))
    );
}
