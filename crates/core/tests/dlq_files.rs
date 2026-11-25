//! Dead-lettered events each keep a DLQ file of their own: names come from
//! the injective file-name stems of the event id and the position, and a name
//! already taken gets a `~n` suffix instead of being overwritten.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::atomic::AtomicBool;

use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::runtime::config::KulyraxConfig;
use kulyrax_core::source::directory::DirectorySource;
use kulyrax_core::source::{EventSource, SourcePosition, SourcedEvent};
use kulyrax_core::{RunMode, SourceError, SourceRunOptions};

fn line(id: &str, seq: u64) -> String {
    format!(
        r#"{{"event_id":"{id}","event_type":"U","metadata":{{}},"payload":"{{\"id\":\"x{seq}\"}}","sequence":{seq},"version":1}}"#
    )
}

/// Run the given sources into a SQLite table that does not exist: every event
/// fails and, with no retries, is dead-lettered into `dlq`.
fn run(root: &Path, dlq: &Path, sources: Vec<Box<dyn EventSource>>) -> u64 {
    let mapping: SqlMapping = kulyrax_core::yaml::from_str(
        "event: U\ntable: missing\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n",
    )
    .expect("mapping");
    let config: KulyraxConfig = kulyrax_core::yaml::from_str(&format!(
        "version: 1\nrouting:\n  file: r.yaml\nadapters:\n  - type: sqlite\n    id: sql\n    priority: 10\n    config:\n      path: {}\n",
        root.join("app.db").display().to_string().replace('\\', "/")
    ))
    .expect("config");
    let report = kulyrax_core::run_sources(
        &config,
        HashMap::from([("U".to_string(), vec!["sql".to_string()])]),
        HashMap::from([("U".to_string(), mapping)]),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        sources,
        &{
            let mut o = SourceRunOptions::default();
            o.dlq_dir = Some(dlq.to_path_buf());
            o.max_batch = 100;
            o.max_batch_delay = std::time::Duration::ZERO;
            o.mode = RunMode::Once;
            o.retry_budget = 0;
            o
        },
        &AtomicBool::new(false),
    )
    .expect("run");
    report.events_dlq
}

/// DLQ file name → the event id it holds.
fn parked(dlq: &Path) -> BTreeMap<String, String> {
    std::fs::read_dir(dlq)
        .expect("dlq dir")
        .map(|e| e.expect("entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .map(|p| {
            let body: serde_json::Value =
                serde_json::from_str(&std::fs::read_to_string(&p).expect("read")).expect("json");
            (
                p.file_name().expect("name").to_string_lossy().into_owned(),
                body["event"]["event_id"]
                    .as_str()
                    .expect("event_id")
                    .to_string(),
            )
        })
        .collect()
}

/// One directory file: its events share one position (the file name). Their
/// ids differ only by punctuation, which the old name mapping folded to one
/// name (`a_b`), so each overwrote the last.
#[test]
fn events_of_one_file_whose_ids_differ_in_punctuation_get_a_file_each() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    std::fs::create_dir_all(root.join("events")).expect("events");
    let ids = ["a.b", "a/b", "a_b", "A_B"];
    let body: Vec<String> = ids
        .iter()
        .enumerate()
        .map(|(i, id)| line(id, i as u64 + 1))
        .collect();
    std::fs::write(root.join("events/0001.ndjson"), body.join("\n") + "\n").expect("write");
    let source =
        DirectorySource::new("events", root.join("events"), root.join("state")).expect("source");
    let dlq = root.join("dlq");
    assert_eq!(run(root, &dlq, vec![Box::new(source)]), 4);
    let files = parked(&dlq);
    let mut held: Vec<&str> = files.values().map(String::as_str).collect();
    held.sort_unstable();
    let mut want = ids.to_vec();
    want.sort_unstable();
    assert_eq!(held, want, "{files:?}");
}

/// A source that yields the same event at the same position every run.
struct Same;

impl EventSource for Same {
    fn id(&self) -> &str {
        "same"
    }
    fn poll(&mut self, _max: usize) -> Result<Vec<SourcedEvent>, SourceError> {
        Ok(Vec::new())
    }
    fn commit(&mut self, _position: SourcePosition) -> Result<(), SourceError> {
        Ok(())
    }
    fn committed_position(&self) -> Option<&SourcePosition> {
        None
    }
}

struct Once(Option<SourcedEvent>);

impl EventSource for Once {
    fn id(&self) -> &str {
        "once"
    }
    fn poll(&mut self, _max: usize) -> Result<Vec<SourcedEvent>, SourceError> {
        Ok(self.0.take().into_iter().collect())
    }
    fn commit(&mut self, _position: SourcePosition) -> Result<(), SourceError> {
        Ok(())
    }
    fn committed_position(&self) -> Option<&SourcePosition> {
        None
    }
}

fn event(id: &str, position: &str) -> SourcedEvent {
    SourcedEvent::new(
        serde_json::from_str(&line(id, 7)).expect("event"),
        SourcePosition(position.into()),
    )
}

/// The same event dead-lettered twice (redelivered after a crash, or replayed)
/// keeps both files.
#[test]
fn the_same_event_dead_lettered_twice_keeps_both_files() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let dlq = root.join("dlq");
    for _ in 0..2 {
        let n = run(
            root,
            &dlq,
            vec![Box::new(Once(Some(event("e1", "p1")))), Box::new(Same)],
        );
        assert_eq!(n, 1);
    }
    let files = parked(&dlq);
    assert_eq!(files.len(), 2, "{files:?}");
    assert!(files.values().all(|id| id == "e1"), "{files:?}");
}

/// Ids and positions of any length still make a file name the file system
/// accepts, and the pair stays distinguishable from a similar one.
#[test]
fn long_ids_and_positions_get_short_names() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path();
    let dlq = root.join("dlq");
    let long_id = "i".repeat(300);
    let long_pos = "p".repeat(300);
    assert_eq!(
        run(
            root,
            &dlq,
            vec![
                Box::new(Once(Some(event(&long_id, &long_pos)))),
                Box::new(Once(Some(event(&long_id, &format!("{long_pos}2"))))),
            ],
        ),
        2
    );
    let files = parked(&dlq);
    assert_eq!(files.len(), 2, "{files:?}");
    assert!(files.keys().all(|name| name.len() <= 255), "{files:?}");
}
