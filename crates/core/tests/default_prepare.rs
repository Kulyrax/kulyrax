//! An adapter that never overrides `StorageAdapter::prepare` (as a
//! third-party adapter written before `prepare` existed) works unchanged: the
//! default is a no-op, nothing checks it, and the run loop projects through it.

mod common;

use common::*;
use kulyrax_core::adapter::{AdapterResult, StorageAdapter, prepare_adapters};
use kulyrax_core::event::Event;
use kulyrax_core::routing::StorageKind;
use kulyrax_core::source::EventSource;
use kulyrax_core::source::directory::DirectorySource;
use kulyrax_core::{RunMode, SourceRunOptions, StoppedReason, run_sources_with_adapters};

use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Mutex};
use tempfile::tempdir;

/// Records every event it is handed. Implements only the methods that
/// existed before `prepare`.
struct Recorder {
    seen: Arc<Mutex<Vec<String>>>,
}

impl StorageAdapter for Recorder {
    fn kind(&self) -> StorageKind {
        StorageKind::Sql
    }
    // The id the shared test config and routing use.
    fn id(&self) -> &str {
        "sql-primary"
    }
    fn priority(&self) -> u32 {
        10
    }
    fn handle(&self, event: &Event) -> AdapterResult {
        self.seen.lock().unwrap().push(event.event_id.clone());
        AdapterResult::created(self.id().to_string(), self.kind())
    }
}

#[test]
fn default_prepare_is_a_no_op_that_never_fails() {
    let adapters: Vec<Box<dyn StorageAdapter>> = vec![Box::new(Recorder {
        seen: Arc::default(),
    })];
    assert!(prepare_adapters(&adapters).is_empty());
}

#[test]
fn run_loop_projects_through_an_adapter_without_prepare() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("test.db");
    let events = dir.path().join("events");
    write_event_file(&events, "01.json", &user_event(1, "u1", "Ada"));
    write_event_file(&events, "02.json", &user_event(1, "u2", "Grace"));

    let seen = Arc::new(Mutex::new(Vec::new()));
    let src: Box<dyn EventSource> =
        Box::new(DirectorySource::new("dir", &events, dir.path().join("state")).unwrap());
    let report = run_sources_with_adapters(
        &config(&db),
        routing(),
        vec![Box::new(Recorder {
            seen: Arc::clone(&seen),
        })],
        vec![src],
        &SourceRunOptions {
            mode: RunMode::Once,
            ..SourceRunOptions::default()
        },
        &AtomicBool::new(false),
    )
    .unwrap();

    assert!(matches!(report.stopped_reason, StoppedReason::CaughtUp));
    assert_eq!(report.events_succeeded, 2);
    assert_eq!(*seen.lock().unwrap(), ["evt-u1-1", "evt-u2-1"]);
}
