//! Phase 17.6 (17.5): an adapter that fails transiently then succeeds → the
//! batch is retried and committed exactly once, with no double application.

mod common;

use common::*;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::adapter::{AdapterError, AdapterResult, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::source::EventSource;
use kulyrax_core::source::directory::DirectorySource;
use kulyrax_core::upcast::UpcasterRegistry;
use kulyrax_core::{RunMode, SourceRunOptions, StoppedReason, run_sources_with_adapters};

use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tempfile::tempdir;

/// Fails the first `fail_first` `handle()` calls, then delegates to a real
/// SQLite adapter.
struct FlakyAdapter {
    inner: SqliteAdapter,
    calls: Cell<u32>,
    fail_first: u32,
}

impl StorageAdapter for FlakyAdapter {
    fn id(&self) -> &str {
        self.inner.id()
    }
    fn kind(&self) -> StorageKind {
        StorageKind::Sql
    }
    fn priority(&self) -> u32 {
        self.inner.priority()
    }
    fn prepare(&self) -> Result<(), AdapterError> {
        self.inner.prepare()
    }
    fn handle(&self, event: &Event) -> AdapterResult {
        let n = self.calls.get();
        self.calls.set(n + 1);
        if n < self.fail_first {
            return AdapterResult::failure(
                self.id().to_string(),
                StorageKind::Sql,
                AdapterError::WriteFailed("transient lock contention".to_string()),
            );
        }
        self.inner.handle(event)
    }
}

#[test]
fn transient_failure_is_retried_and_committed_once() {
    let dir = tempdir().unwrap();
    let db = dir.path().join("test.db");
    init_db(&db);
    let events = dir.path().join("events");
    write_event_file(&events, "01.json", &user_event(1, "u1", "Ada"));
    write_event_file(&events, "02.json", &user_event(2, "u1", "Ada L."));

    let adapter = FlakyAdapter {
        inner: SqliteAdapter::new(
            "sql-primary".to_string(),
            db.to_string_lossy().to_string(),
            10,
            SqlRuntimeBuilder::new(sql_mappings()),
            Arc::new(UpcasterRegistry::new()),
            MigrationPolicy::default(),
        ),
        calls: Cell::new(0),
        fail_first: 1,
    };

    let src: Box<dyn EventSource> =
        Box::new(DirectorySource::new("dir", &events, dir.path().join("state")).unwrap());
    let opts = SourceRunOptions {
        mode: RunMode::Once,
        max_batch: 256,
        retry_budget: 3,
        dlq_dir: None,
    };
    let stop = AtomicBool::new(false);
    let report = run_sources_with_adapters(
        &config(&db),
        routing(),
        vec![Box::new(adapter)],
        vec![src],
        &opts,
        &stop,
    )
    .unwrap();

    assert!(
        matches!(report.stopped_reason, StoppedReason::CaughtUp),
        "{report:?}"
    );
    assert_eq!(report.batches_committed, 1);
    assert_eq!(report.events_dlq, 0);
    assert_eq!(report.batches.len(), 1);
    assert_eq!(
        report.batches[0].retries, 1,
        "one retry after the transient fail"
    );
    assert_eq!(report.events_failed, 0, "the final attempt had no failures");

    assert_eq!(read_user(&db, "u1"), Some(("Ada L.".to_string(), 2)));

    // Committed → a restart re-polls nothing.
    let src2: Box<dyn EventSource> =
        Box::new(DirectorySource::new("dir", &events, dir.path().join("state")).unwrap());
    let r2 = run_once(&db, vec![src2], None).unwrap();
    assert_eq!(r2.events_processed, 0);
    assert_eq!(
        read_user(&db, "u1"),
        Some(("Ada L.".to_string(), 2)),
        "no double apply"
    );
}
