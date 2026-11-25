//! File names in the file-backed stores: values from events (keys, document
//! ids, node ids) never leave the store root, never meet each other on a
//! case-insensitive file system, survive any length; names from mappings are
//! checked; a store written by 0.12.0 opens with every guard lane, or is
//! refused when it holds files this version would name differently.

use std::collections::{BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use kulyrax_core::adapter::document::file::FileDocumentAdapter;
use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::document::runtime::DocumentRuntimeBuilder;
use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::runtime::GraphRuntimeBuilder;
use kulyrax_core::adapter::graph::store::GraphStore;
use kulyrax_core::adapter::keyvalue::mapping::KvMapping;
use kulyrax_core::adapter::keyvalue::runtime::KvRuntimeBuilder;
use kulyrax_core::adapter::keyvalue::store::FileKvStore;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

fn ev(event_type: &str, id: &str, seq: u64) -> Event {
    Event {
        event_id: format!("e-{seq}-{}", id.len()),
        event_type: event_type.into(),
        payload: serde_json::json!({ "id": id, "v": seq }).to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

fn kv(root: &Path, namespaces: &[&str]) -> FileKvStore {
    let mappings = namespaces
        .iter()
        .enumerate()
        .map(|(i, ns)| {
            let event = if i == 0 { "U".to_string() } else { format!("U{i}") };
            let m: KvMapping = serde_yaml::from_str(&format!(
                "event: {event}\nnamespace: {ns}\nversion: 1\nkey: payload.id\non_existing: replace\nvalue:\n  v: payload.v\n"
            ))
            .unwrap();
            (event, m)
        })
        .collect();
    FileKvStore::new(
        "kv".into(),
        root.to_path_buf(),
        10,
        KvRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn doc(root: &Path, collection: &str) -> FileDocumentAdapter {
    let m: DocumentMapping = serde_yaml::from_str(&format!(
        "event: U\ncollection: {collection}\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  v: payload.v\n"
    ))
    .unwrap();
    FileDocumentAdapter::new(
        "doc".into(),
        root.to_path_buf(),
        10,
        DocumentRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn graph(root: &Path, label: &str) -> GraphStore {
    let node: GraphMapping = serde_yaml::from_str(&format!(
        "kind: node\nevent: U\nlabel: {label}\nversion: 1\nkey: payload.id\non_existing: replace\nproperties:\n  v: payload.v\n"
    ))
    .unwrap();
    let edge: GraphMapping = serde_yaml::from_str(
        "kind: edge\nevent: E\nedge_type: LINKS\nversion: 1\nfrom: payload.id\nto: payload.to\non_existing: replace\nproperties:\n  v: payload.v\n",
    )
    .unwrap();
    GraphStore::new(
        "graph".into(),
        root.to_path_buf(),
        10,
        GraphRuntimeBuilder::new(HashMap::from([
            ("U".to_string(), node),
            ("E".to_string(), edge),
        ])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn files(dir: &Path) -> BTreeSet<String> {
    fn walk(base: &Path, dir: &Path, out: &mut BTreeSet<String>) {
        for e in std::fs::read_dir(dir).into_iter().flatten().flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(base, &p, out);
            } else {
                out.insert(
                    p.strip_prefix(base)
                        .unwrap()
                        .to_string_lossy()
                        .replace('\\', "/"),
                );
            }
        }
    }
    let mut out = BTreeSet::new();
    walk(dir, dir, &mut out);
    out
}

fn outcome(a: &dyn StorageAdapter, e: &Event) -> AdapterOutcome {
    a.handle(e).outcome
}

fn stale(o: &AdapterOutcome) -> bool {
    matches!(o, AdapterOutcome::Skipped(SkipReason::StaleSequence))
}

const NASTY: &[&str] = &[
    "../../escaped",
    "..",
    ".",
    "a/b",
    "a\\b",
    "C:\\x",
    "/abs",
    "x\0y",
    "nul",
    "CON.txt",
    "^5e",
];

/// Every hostile id is written inside the store root, and is projected
/// again idempotently after a restart (its guard is found under its name).
#[test]
fn event_ids_never_leave_the_store_root() {
    let outer = tempfile::tempdir().unwrap();
    let root = outer.path().join("store");
    {
        let (k, d, g) = (
            kv(&root, &["users"]),
            doc(&root, "users_doc"),
            graph(&root, "User"),
        );
        for (i, id) in NASTY.iter().enumerate() {
            let seq = 10 + i as u64;
            for a in [&k as &dyn StorageAdapter, &d, &g] {
                let o = outcome(a, &ev("U", id, seq));
                assert!(
                    matches!(o, AdapterOutcome::Created),
                    "{} {id:?}: {o:?}",
                    a.id()
                );
            }
        }
    }
    let outside: Vec<String> = files(outer.path())
        .into_iter()
        .filter(|f| !f.starts_with("store/"))
        .collect();
    assert!(outside.is_empty(), "written outside the root: {outside:?}");
    let (k, d, g) = (
        kv(&root, &["users"]),
        doc(&root, "users_doc"),
        graph(&root, "User"),
    );
    for (i, id) in NASTY.iter().enumerate() {
        for a in [&k as &dyn StorageAdapter, &d, &g] {
            let o = outcome(a, &ev("U", id, i as u64));
            assert!(
                stale(&o),
                "{} {id:?} redelivered after a restart: {o:?}",
                a.id()
            );
        }
    }
}

/// `K` and `k` are two entities, on every file system: after a restart an
/// older `K` is still stale, which on NTFS / macOS needs `K`'s and `k`'s
/// files to be distinct.
#[test]
fn case_variants_are_distinct_entities() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (k, d, g) = (
            kv(dir.path(), &["users"]),
            doc(dir.path(), "docs"),
            graph(dir.path(), "User"),
        );
        for a in [&k as &dyn StorageAdapter, &d, &g] {
            assert!(matches!(
                outcome(a, &ev("U", "K", 5)),
                AdapterOutcome::Created
            ));
            assert!(matches!(
                outcome(a, &ev("U", "k", 1)),
                AdapterOutcome::Created
            ));
        }
    }
    let (k, d, g) = (
        kv(dir.path(), &["users"]),
        doc(dir.path(), "docs"),
        graph(dir.path(), "User"),
    );
    for a in [&k as &dyn StorageAdapter, &d, &g] {
        let o = outcome(a, &ev("U", "K", 3));
        assert!(stale(&o), "{}: an older K after a restart: {o:?}", a.id());
        let o = outcome(a, &ev("U", "k", 2));
        assert!(
            matches!(o, AdapterOutcome::Updated),
            "{}: k is its own lane: {o:?}",
            a.id()
        );
    }
    let names = files(dir.path());
    assert!(
        names.contains("users/^4b.json") && names.contains("users/k.json"),
        "{names:?}"
    );
}

#[test]
fn long_ids_get_hashed_names_with_the_id_inside() {
    let dir = tempfile::tempdir().unwrap();
    let long = format!("{}-{}", "x".repeat(300), "end");
    let longer = format!("{}-other", "y".repeat(300));
    {
        let k = kv(dir.path(), &["users"]);
        assert!(matches!(
            outcome(&k, &ev("U", &long, 5)),
            AdapterOutcome::Created
        ));
        assert!(matches!(
            outcome(&k, &ev("U", &longer, 1)),
            AdapterOutcome::Created
        ));
        let g = graph(dir.path(), "User");
        assert!(matches!(
            outcome(&g, &ev("U", &long, 5)),
            AdapterOutcome::Created
        ));
        let edge = Event {
            payload: serde_json::json!({ "id": long, "to": longer, "v": 1 }).to_string(),
            ..ev("E", "", 6)
        };
        assert!(matches!(outcome(&g, &edge), AdapterOutcome::Created));
    }
    for f in files(dir.path()) {
        let name = f.rsplit('/').next().unwrap();
        assert!(name.len() <= 255, "{f}");
    }
    let k = kv(dir.path(), &["users"]);
    assert!(stale(&outcome(&k, &ev("U", &long, 3))));

    // A guard whose stored id is another id's (as a hash collision would
    // make it) fails the event instead of being taken for its own.
    let guard = files(dir.path())
        .into_iter()
        .find(|f| {
            f.starts_with(".conduit/kv/users/^h")
                && std::fs::read_to_string(dir.path().join(f))
                    .unwrap()
                    .contains(&long)
        })
        .unwrap();
    let path = dir.path().join(&guard);
    let text = std::fs::read_to_string(&path).unwrap();
    drop(k);
    std::fs::write(&path, text.replace(&long, "someone else")).unwrap();
    let k = kv(dir.path(), &["users"]);
    let o = outcome(&k, &ev("U", &long, 9));
    assert!(
        matches!(&o, AdapterOutcome::Failed(e) if format!("{e:?}").contains("share a hashed file name")),
        "{o:?}"
    );
}

/// Namespaces named like the store's own files work and keep their guards
/// across a restart.
#[test]
fn namespaces_cannot_reach_the_store_s_own_files() {
    for ns in [
        "wal", "lock", "layout", "kv", "entities", "guard", "incident",
    ] {
        let dir = tempfile::tempdir().unwrap();
        {
            let k = kv(dir.path(), &[ns]);
            let o = outcome(&k, &ev("U", "u1", 5));
            assert!(matches!(o, AdapterOutcome::Created), "{ns}: {o:?}");
        }
        let k = kv(dir.path(), &[ns]);
        let o = outcome(&k, &ev("U", "u1", 3));
        assert!(stale(&o), "{ns}: an older event after a restart: {o:?}");
    }
}

// ---------------------------------------------------------------------------
// Stores written by 0.12.0
// ---------------------------------------------------------------------------

fn write(path: PathBuf, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

const OLD_GUARD: &str =
    r#"{"last_sequence":5,"last_event_id":"e5","deleted":false,"permanent":false}"#;

/// A 0.12.0 key-value store (guards at `.conduit/<namespace>/`, including a
/// namespace `wal`) opens with every lane: an older redelivery is stale.
#[test]
fn a_0_12_key_value_store_keeps_its_guards() {
    let dir = tempfile::tempdir().unwrap();
    for ns in ["users", "wal"] {
        write(dir.path().join(ns).join("u1.json"), r#"{"v":5}"#);
        write(
            dir.path().join(".conduit").join(ns).join("u1.guard.json"),
            OLD_GUARD,
        );
    }
    std::fs::create_dir_all(dir.path().join(".conduit/wal")).unwrap();
    let k = kv(dir.path(), &["users", "wal"]);
    k.prepare().unwrap();
    let names = files(dir.path());
    for ns in ["users", "wal"] {
        assert!(
            names.contains(&format!(".conduit/kv/{ns}/u1.guard.json")),
            "{names:?}"
        );
        assert!(
            !names.contains(&format!(".conduit/{ns}/u1.guard.json")),
            "{names:?}"
        );
    }
    assert!(stale(&outcome(&k, &ev("U", "u1", 3))));
    assert!(stale(&outcome(&k, &ev("U1", "u1", 3))));
    assert!(names.contains(".conduit/layout/kv"), "{names:?}");
}

fn refused(a: &dyn StorageAdapter) -> String {
    a.prepare().err().map(|e| e.to_string()).unwrap_or_default()
}

/// Files 0.12.0 named from ids this version encodes (here `K`, and `a/b`,
/// which made a directory) are refused, named, never re-keyed.
#[test]
fn a_0_12_store_with_ids_named_differently_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path().join("users/K.json"), "{}");
    write(dir.path().join(".conduit/users/K.guard.json"), OLD_GUARD);
    write(dir.path().join("users/a/b.json"), "{}");
    let err = refused(&kv(dir.path(), &["users"]));
    assert!(err.contains("Rebuild the store"), "{err}");
    assert!(
        err.contains("K.json") && err.contains("K.guard.json"),
        "{err}"
    );
    assert!(
        err.contains(&format!("users{}a", std::path::MAIN_SEPARATOR)),
        "{err}"
    );
    // Nothing was re-keyed: the files are where they were.
    assert!(dir.path().join("users/K.json").exists());

    let dir = tempfile::tempdir().unwrap();
    write(dir.path().join(".conduit/entities/docs/K.done"), OLD_GUARD);
    assert!(refused(&doc(dir.path(), "docs")).contains("K.done"));

    let dir = tempfile::tempdir().unwrap();
    write(dir.path().join("nodes/User/K.json"), "{}");
    assert!(refused(&graph(dir.path(), "User")).contains("K.json"));
}

#[test]
fn an_old_and_a_new_guard_for_one_key_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path().join(".conduit/users/u1.guard.json"), OLD_GUARD);
    write(
        dir.path().join(".conduit/kv/users/u1.guard.json"),
        OLD_GUARD,
    );
    let err = refused(&kv(dir.path(), &["users"]));
    let old = Path::new(".conduit").join("users").join("u1.guard.json");
    let new = Path::new(".conduit")
        .join("kv")
        .join("users")
        .join("u1.guard.json");
    assert!(err.contains(&old.display().to_string()), "{err}");
    assert!(err.contains(&new.display().to_string()), "{err}");
}

// ---------------------------------------------------------------------------
// Names from mappings
// ---------------------------------------------------------------------------

#[test]
fn mapping_names_must_be_plain_directory_names() {
    for bad in ["../x", ".conduit", "a/b", "con", "a:b", "a.b"] {
        let dir = tempfile::tempdir().unwrap();
        let err = refused(&kv(dir.path(), &[bad]));
        assert!(err.contains("cannot name a directory"), "{bad}: {err}");
        let err = refused(&doc(dir.path(), bad));
        assert!(err.contains("cannot name a directory"), "{bad}: {err}");
        let err = refused(&graph(dir.path(), bad));
        assert!(err.contains("cannot name a directory"), "{bad}: {err}");
        assert!(
            !dir.path().join(".conduit").exists(),
            "{bad}: refused before writing"
        );
    }
}

/// Two names that are one directory on a case-insensitive file system, or
/// two adapters on one root writing the same directory, are refused.
#[test]
fn directories_are_not_shared() {
    let dir = tempfile::tempdir().unwrap();
    let err = refused(&kv(dir.path(), &["Users", "users"]));
    assert!(err.contains("would share the directory"), "{err}");

    let dir = tempfile::tempdir().unwrap();
    let k = kv(dir.path(), &["users"]);
    k.prepare().unwrap();
    let err = refused(&doc(dir.path(), "users"));
    assert!(err.contains("would share the directory"), "{err}");
    // Different names share a root as before.
    doc(dir.path(), "orders").prepare().unwrap();
}

#[test]
fn sql_names_must_be_plain_identifiers() {
    let dir = tempfile::tempdir().unwrap();
    let sqlite = |table: &str, column: &str| {
        let m: SqlMapping = serde_yaml::from_str(&format!(
            "event: U\ntable: {table}\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n  '{column}': payload.v\n"
        ))
        .unwrap();
        SqliteAdapter::new(
            "sql".into(),
            dir.path().join("app.db").to_string_lossy().into(),
            10,
            SqlRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
            Arc::new(UpcasterRegistry::new()),
            MigrationPolicy::default(),
        )
    };
    let err = refused(&sqlite("users; DROP TABLE x", "name"));
    assert!(
        err.contains("not plain identifiers") && err.contains("users; DROP"),
        "{err}"
    );
    let err = refused(&sqlite("users", "a\"b"));
    assert!(err.contains("column"), "{err}");
    sqlite("main.users", "name").prepare().unwrap();
}

// ---------------------------------------------------------------------------
// The DLQ
// ---------------------------------------------------------------------------

/// A source whose position is chosen by a producer (as over gRPC).
struct Hostile {
    sent: bool,
}

impl kulyrax_core::source::EventSource for Hostile {
    fn id(&self) -> &str {
        "grpc"
    }
    fn poll(
        &mut self,
        _max: usize,
    ) -> Result<Vec<kulyrax_core::source::SourcedEvent>, kulyrax_core::SourceError> {
        if self.sent {
            return Ok(Vec::new());
        }
        self.sent = true;
        Ok(vec![kulyrax_core::source::SourcedEvent {
            event: ev("U", "u1", 1),
            position: kulyrax_core::source::SourcePosition("../../outside".into()),
        }])
    }
    fn commit(
        &mut self,
        _position: kulyrax_core::source::SourcePosition,
    ) -> Result<(), kulyrax_core::SourceError> {
        Ok(())
    }
    fn committed_position(&self) -> Option<&kulyrax_core::source::SourcePosition> {
        None
    }
}

#[test]
fn a_dlq_file_stays_in_the_dlq_directory() {
    let outer = tempfile::tempdir().unwrap();
    let dlq = outer.path().join("a").join("b").join("dlq");
    // A mapping to a table that does not exist: the event fails, and is
    // dead-lettered once its retries are spent.
    let m: SqlMapping = serde_yaml::from_str(
        "event: U\ntable: missing\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n",
    )
    .unwrap();
    let config: kulyrax_core::runtime::config::KulyraxConfig = serde_yaml::from_str(&format!(
        "version: 1\nrouting:\n  file: r.yaml\nadapters:\n  - type: sqlite\n    id: sql\n    priority: 10\n    config:\n      path: {}\n",
        outer.path().join("app.db").display().to_string().replace('\\', "/")
    ))
    .unwrap();
    let report = kulyrax_core::run_sources(
        &config,
        HashMap::from([("U".to_string(), vec!["sql".to_string()])]),
        HashMap::from([("U".to_string(), m)]),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        vec![Box::new(Hostile { sent: false })],
        &kulyrax_core::SourceRunOptions {
            mode: kulyrax_core::RunMode::Once,
            max_batch: 10,
            retry_budget: 0,
            max_batch_delay: std::time::Duration::ZERO,
            dlq_dir: Some(dlq.clone()),
        },
        &std::sync::atomic::AtomicBool::new(false),
    )
    .unwrap();
    assert_eq!(report.events_dlq, 1);
    let all = files(outer.path());
    let parked: Vec<&String> = all.iter().filter(|f| f.ends_with(".json")).collect();
    assert_eq!(parked.len(), 1, "{all:?}");
    assert!(parked[0].starts_with("a/b/dlq/"), "{all:?}");
}
