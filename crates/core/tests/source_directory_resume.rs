//! Phase 17.6: project a directory source, "crash" before the position is
//! committed, restart from the (uncommitted) checkpoint → the final projected
//! state is byte-identical to a single straight run. This is the
//! effectively-once guarantee the whole 11–16 arc was building toward.

mod common;

use common::*;
use kulyrax_core::source::directory::DirectorySource;
use kulyrax_core::source::{EventSource, SourceError, SourcePosition, SourcedEvent};
use kulyrax_core::{RunMode, SourceRunOptions, run_sources};

use std::sync::atomic::AtomicBool;
use tempfile::tempdir;

/// Wraps a real source but makes the first `commit` fail — as if the process
/// died between "batch dispatched" and "position written".
struct CrashOnFirstCommit {
    inner: DirectorySource,
    crashed: bool,
}

impl EventSource for CrashOnFirstCommit {
    fn id(&self) -> &str {
        self.inner.id()
    }
    fn poll(&mut self, max_batch: usize) -> Result<Vec<SourcedEvent>, SourceError> {
        self.inner.poll(max_batch)
    }
    fn commit(&mut self, position: SourcePosition) -> Result<(), SourceError> {
        if !self.crashed {
            self.crashed = true;
            return Err(SourceError::Checkpoint(
                "simulated crash before commit".into(),
            ));
        }
        self.inner.commit(position)
    }
    fn committed_position(&self) -> Option<&SourcePosition> {
        self.inner.committed_position()
    }
}

fn opts() -> SourceRunOptions {
    SourceRunOptions {
        mode: RunMode::Once,
        max_batch: 3,
        retry_budget: 0,
        max_batch_delay: std::time::Duration::ZERO,
        dlq_dir: None,
    }
}

fn events_dir(base: &std::path::Path) -> std::path::PathBuf {
    let dir = base.join("events");
    for (i, (id, name, seq)) in [
        ("u1", "Ada", 1u64),
        ("u1", "Ada L.", 2),
        ("u2", "Grace", 1),
        ("u1", "Ada Lovelace", 3),
        ("u2", "Grace Hopper", 2),
    ]
    .into_iter()
    .enumerate()
    {
        write_event_file(&dir, &format!("{i:03}.json"), &user_event(seq, id, name));
    }
    dir
}

#[test]
fn resume_after_precommit_crash_matches_clean_run() {
    // Clean run.
    let clean = tempdir().unwrap();
    let clean_db = clean.path().join("clean.db");
    init_db(&clean_db);
    let dir = events_dir(clean.path());
    let state = clean.path().join("state");
    let stop = AtomicBool::new(false);
    let src: Box<dyn EventSource> = Box::new(DirectorySource::new("dir", &dir, &state).unwrap());
    run_sources(
        &config(&clean_db),
        routing(),
        sql_mappings(),
        Default::default(),
        Default::default(),
        Default::default(),
        vec![src],
        &opts(),
        &stop,
    )
    .unwrap();
    let clean_u1 = read_user(&clean_db, "u1");
    let clean_u2 = read_user(&clean_db, "u2");
    assert_eq!(clean_u1, Some(("Ada Lovelace".to_string(), 3)));
    assert_eq!(clean_u2, Some(("Grace Hopper".to_string(), 2)));

    // Crashy run: same DB + dir + state, first commit fails mid-way.
    let crashy = tempdir().unwrap();
    let db = crashy.path().join("test.db");
    init_db(&db);
    let dir = events_dir(crashy.path());
    let state = crashy.path().join("state");

    let crashy_src: Box<dyn EventSource> = Box::new(CrashOnFirstCommit {
        inner: DirectorySource::new("dir", &dir, &state).unwrap(),
        crashed: false,
    });
    let err = run_sources(
        &config(&db),
        routing(),
        sql_mappings(),
        Default::default(),
        Default::default(),
        Default::default(),
        vec![crashy_src],
        &opts(),
        &stop,
    );
    assert!(
        err.is_err(),
        "the simulated crash should surface as an error"
    );

    // Restart: fresh source reads the (never-written) checkpoint = start over.
    let restart_src: Box<dyn EventSource> =
        Box::new(DirectorySource::new("dir", &dir, &state).unwrap());
    assert!(
        restart_src.committed_position().is_none(),
        "nothing was committed"
    );
    run_sources(
        &config(&db),
        routing(),
        sql_mappings(),
        Default::default(),
        Default::default(),
        Default::default(),
        vec![restart_src],
        &opts(),
        &stop,
    )
    .unwrap();

    assert_eq!(
        read_user(&db, "u1"),
        clean_u1,
        "u1 must match the clean run"
    );
    assert_eq!(
        read_user(&db, "u2"),
        clean_u2,
        "u2 must match the clean run"
    );
}
