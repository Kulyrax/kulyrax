//! The run report counts every stage of the source loop: one source read per
//! poll, one dispatch and checkpoint per batch, one adapter call and write
//! per projected SQL event, one guard read and one commit per batch (each
//! batch is one transaction, its guard lanes loaded in one query), plus one
//! schema setup.

mod common;

use common::*;
use kulyrax_core::source::EventSource;
use kulyrax_core::source::directory::DirectorySource;
use kulyrax_core::{RunMode, SourceRunOptions, run_sources};

use std::sync::atomic::AtomicBool;
use tempfile::tempdir;

#[test]
fn run_report_counts_each_stage() {
    let tmp = tempdir().unwrap();
    let db = tmp.path().join("test.db");
    init_db(&db);
    let dir = tmp.path().join("events");
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
    let src: Box<dyn EventSource> =
        Box::new(DirectorySource::new("dir", &dir, tmp.path().join("state")).unwrap());
    let opts = SourceRunOptions {
        mode: RunMode::Once,
        max_batch: 3,
        retry_budget: 0,
        max_batch_delay: std::time::Duration::ZERO,
        dlq_dir: None,
    };

    let report = run_sources(
        &config(&db),
        routing(),
        sql_mappings(),
        Default::default(),
        Default::default(),
        Default::default(),
        vec![src],
        &opts,
        &AtomicBool::new(false),
    )
    .unwrap();

    let t = report.timings;
    // Two batches (files 000–002, then 003–004), then the empty poll that ends the run.
    assert_eq!(t.source_read.calls, 3);
    assert_eq!(t.dispatch.calls, 2);
    assert_eq!(t.checkpoint.calls, 2);
    // The guard table is created once, before the first event.
    assert_eq!(t.schema.calls, 1);
    for (stage, s) in [("adapter", t.adapter), ("write", t.write)] {
        assert_eq!(s.calls, 5, "{stage}: one call per event");
    }
    for (stage, s) in [("read", t.read), ("commit", t.commit)] {
        assert_eq!(s.calls, 2, "{stage}: one call per batch");
    }
    assert_eq!(t.batch.calls, 4, "begin and commit per batch");
    assert!(report.batches.iter().all(|b| !b.fell_back));
    assert!(
        t.dispatch.total_us >= t.adapter.total_us,
        "dispatch contains adapter"
    );
}
