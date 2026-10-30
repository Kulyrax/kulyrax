//! Phase 2: `max_batch_delay`. In continuous mode a batch closes when
//! `max_batch` events are waiting or when the delay has passed, whichever
//! comes first: after a short poll the loop waits out the delay before the
//! next poll; after a full poll (a backlog) it doesn't wait; `RunMode::Once`
//! never waits.

mod common;

use common::*;
use kulyrax_core::adapter::{AdapterResult, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::routing::StorageKind;
use kulyrax_core::source::{EventSource, SourceError, SourcePosition, SourcedEvent};
use kulyrax_core::{RunMode, SourceRunOptions, run_sources_with_adapters};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tempfile::tempdir;

/// Hands out `per_poll` events per poll until `remaining` runs out, then
/// sets `stop`; records when each non-empty poll happened.
struct Trickle {
    remaining: usize,
    per_poll: usize,
    next: usize,
    polls: Arc<Mutex<Vec<Instant>>>,
    stop: Arc<AtomicBool>,
    committed: Option<SourcePosition>,
}

impl EventSource for Trickle {
    fn id(&self) -> &str {
        "trickle"
    }

    fn poll(&mut self, max_batch: usize) -> Result<Vec<SourcedEvent>, SourceError> {
        let n = self.per_poll.min(max_batch).min(self.remaining);
        if n == 0 {
            self.stop.store(true, Ordering::Relaxed);
            return Ok(Vec::new());
        }
        self.polls.lock().unwrap().push(Instant::now());
        self.remaining -= n;
        let start = self.next;
        self.next += n;
        Ok((start..start + n)
            .map(|i| SourcedEvent {
                event: user_event(1, &format!("u{i}"), "x"),
                position: SourcePosition(format!("{i:06}")),
            })
            .collect())
    }

    fn commit(&mut self, position: SourcePosition) -> Result<(), SourceError> {
        self.committed = Some(position);
        Ok(())
    }

    fn committed_position(&self) -> Option<&SourcePosition> {
        self.committed.as_ref()
    }
}

struct Accept;

impl StorageAdapter for Accept {
    fn kind(&self) -> StorageKind {
        StorageKind::Sql
    }
    fn id(&self) -> &str {
        "sql-primary"
    }
    fn priority(&self) -> u32 {
        10
    }
    fn handle(&self, _event: &Event) -> AdapterResult {
        AdapterResult::created(self.id().to_string(), self.kind())
    }
}

/// Gaps between consecutive non-empty polls.
fn poll_gaps(mode: RunMode, per_poll: usize, max_batch: usize, delay: Duration) -> Vec<Duration> {
    let tmp = tempdir().unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let polls = Arc::new(Mutex::new(Vec::new()));
    let source = Trickle {
        remaining: per_poll * 5,
        per_poll,
        next: 0,
        polls: Arc::clone(&polls),
        stop: Arc::clone(&stop),
        committed: None,
    };
    let report = run_sources_with_adapters(
        &config(&tmp.path().join("unused.db")),
        routing(),
        vec![Box::new(Accept)],
        vec![Box::new(source)],
        &SourceRunOptions {
            mode,
            max_batch,
            max_batch_delay: delay,
            retry_budget: 0,
            dlq_dir: None,
        },
        &stop,
    )
    .unwrap();
    assert_eq!(report.events_processed, per_poll as u64 * 5);
    let polls = polls.lock().unwrap();
    polls.windows(2).map(|w| w[1] - w[0]).collect()
}

const DELAY: Duration = Duration::from_millis(80);

fn continuous() -> RunMode {
    RunMode::Continuous {
        poll_interval: Duration::from_millis(10),
    }
}

#[test]
fn short_polls_wait_out_the_delay() {
    let gaps = poll_gaps(continuous(), 2, 10, DELAY);
    assert_eq!(gaps.len(), 4);
    for gap in gaps {
        assert!(gap >= DELAY - Duration::from_millis(5), "{gap:?}");
    }
}

#[test]
fn full_polls_do_not_wait() {
    let gaps = poll_gaps(continuous(), 10, 10, DELAY);
    assert_eq!(gaps.len(), 4);
    for gap in gaps {
        assert!(gap < DELAY / 2, "{gap:?}");
    }
}

#[test]
fn once_mode_never_waits() {
    let gaps = poll_gaps(RunMode::Once, 2, 10, DELAY);
    for gap in gaps {
        assert!(gap < DELAY / 2, "{gap:?}");
    }
}

#[test]
fn zero_delay_disables_it() {
    let gaps = poll_gaps(continuous(), 2, 10, Duration::ZERO);
    for gap in gaps {
        assert!(gap < DELAY / 2, "{gap:?}");
    }
}
