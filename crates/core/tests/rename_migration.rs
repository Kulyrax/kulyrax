//! Stores written by Kulyrax 0.12 (internal files under `.conduit/`, the SQL
//! guard table `conduit_projection_state`) open under this version: renamed
//! in place once, every guard lane kept, a tombstone left under the old name.
//! Built here by writing a store with this version and putting it back in
//! the 0.12.1 layout; `kulyrax-cli`'s `rename_migration` tests use a real
//! 0.12.1 binary.

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
            let m: KvMapping = kulyrax_core::yaml::from_str(&format!(
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
    let m: DocumentMapping = kulyrax_core::yaml::from_str(&format!(
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
    let node: GraphMapping = kulyrax_core::yaml::from_str(&format!(
        "kind: node\nevent: U\nlabel: {label}\nversion: 1\nkey: payload.id\non_existing: replace\nproperties:\n  v: payload.v\n"
    ))
    .unwrap();
    let edge: GraphMapping = kulyrax_core::yaml::from_str(
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

fn write(path: PathBuf, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

fn refused(a: &dyn StorageAdapter) -> String {
    a.prepare().err().map(|e| e.to_string()).unwrap_or_default()
}

/// A key-value store with an upsert (`U`) and a delete (`D`) mapping on
/// `users`.
fn kv_with_delete(root: &Path) -> FileKvStore {
    let upsert: KvMapping = kulyrax_core::yaml::from_str(
        "event: U\nnamespace: users\nversion: 1\nkey: payload.id\non_existing: replace\nvalue:\n  v: payload.v\n",
    )
    .unwrap();
    let delete: KvMapping = kulyrax_core::yaml::from_str(
        "event: D\nnamespace: users\nversion: 1\nkey: payload.id\noperation: delete\nvalue: {}\n",
    )
    .unwrap();
    FileKvStore::new(
        "kv".into(),
        root.to_path_buf(),
        10,
        KvRuntimeBuilder::new(HashMap::from([
            ("U".to_string(), upsert),
            ("D".to_string(), delete),
        ])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

/// Put a store this version wrote back into the 0.12.1 layout: the same
/// entries under `.conduit/`, layout markers at version 1.
fn as_0_12_1(root: &Path) {
    std::fs::rename(root.join(".kulyrax"), root.join(".conduit")).unwrap();
    let markers = root.join(".conduit/layout");
    for e in std::fs::read_dir(&markers).into_iter().flatten().flatten() {
        std::fs::write(e.path(), "1\n").unwrap();
    }
}

fn assert_migrated(root: &Path) {
    let tombstone = root.join(".conduit");
    assert!(tombstone.is_file(), "the tombstone file replaces .conduit/");
    assert!(
        std::fs::read_to_string(&tombstone)
            .unwrap()
            .contains("Kulyrax 0.12 cannot open this store")
    );
    assert!(!root.join(".kulyrax-tmp-conduit").exists());
    for e in std::fs::read_dir(root.join(".kulyrax/layout")).unwrap() {
        let path = e.unwrap().path();
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "2\n", "{path:?}");
    }
}

/// Lanes in each file store: created, updated, deleted. After the move every
/// one is intact: older redeliveries are stale, the deleted lane stays
/// deleted, and a newer event still applies.
#[test]
fn a_0_12_1_file_store_opens_with_every_lane() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    {
        let k = kv_with_delete(root);
        let d = doc(root, "docs");
        let g = graph(root, "User");
        for a in [&k as &dyn StorageAdapter, &d, &g] {
            assert!(matches!(
                outcome(a, &ev("U", "u1", 5)),
                AdapterOutcome::Created
            ));
            assert!(matches!(
                outcome(a, &ev("U", "u2", 3)),
                AdapterOutcome::Created
            ));
        }
        assert!(matches!(
            outcome(&k, &ev("D", "u2", 4)),
            AdapterOutcome::Deleted
        ));
    }
    as_0_12_1(root);
    let before = files(root);
    assert!(
        before.contains(".conduit/kv/users/u1.guard.json"),
        "{before:?}"
    );

    let k = kv_with_delete(root);
    let d = doc(root, "docs");
    let g = graph(root, "User");
    for a in [&k as &dyn StorageAdapter, &d, &g] {
        a.prepare().unwrap();
        let o = outcome(a, &ev("U", "u1", 2));
        assert!(stale(&o), "{}: an older u1 after the move: {o:?}", a.id());
    }
    assert!(stale(&outcome(&k, &ev("U", "u2", 2))), "deleted lane kept");
    assert!(matches!(
        outcome(&k, &ev("U", "u1", 9)),
        AdapterOutcome::Updated
    ));
    assert_migrated(root);
    let after = files(root);
    assert!(
        after.contains(".kulyrax/kv/users/u1.guard.json"),
        "{after:?}"
    );
    assert!(
        !after.iter().any(|f| f.starts_with(".conduit/")),
        "{after:?}"
    );
    drop((k, d, g));

    // A second start is a no-op.
    let k = kv_with_delete(root);
    k.prepare().unwrap();
    assert!(stale(&outcome(&k, &ev("U", "u1", 3))));
    assert_migrated(root);
}

#[test]
fn an_entry_under_both_names_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    {
        let k = kv(root, &["users"]);
        assert!(matches!(
            outcome(&k, &ev("U", "u1", 5)),
            AdapterOutcome::Created
        ));
    }
    as_0_12_1(root);
    write(root.join(".kulyrax/kv/users/u1.guard.json"), "{}");
    let err = refused(&kv(root, &["users"]));
    let old = Path::new(".conduit").join("kv");
    let new = Path::new(".kulyrax").join("kv");
    assert!(err.contains(&old.display().to_string()), "{err}");
    assert!(err.contains(&new.display().to_string()), "{err}");
    assert!(
        root.join(".conduit/kv/users/u1.guard.json").exists(),
        "nothing moved"
    );
}

/// Kulyrax 0.12.0 kept key-value guards at `.conduit/<namespace>/` (even for
/// a namespace named `wal`). They are refused, not moved, with the way out.
#[test]
fn a_0_12_0_key_value_store_needs_one_start_under_0_12_1() {
    for ns in ["users", "wal", "state"] {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        write(root.join(ns).join("u1.json"), r#"{"v":5}"#);
        write(
            root.join(".conduit").join(ns).join("u1.guard.json"),
            r#"{"last_sequence":5,"last_event_id":"e5","deleted":false,"permanent":false}"#,
        );
        let err = refused(&kv(root, &[ns]));
        assert!(
            err.contains("Open the store once with Kulyrax 0.12.1"),
            "{ns}: {err}"
        );
        assert!(
            root.join(".conduit")
                .join(ns)
                .join("u1.guard.json")
                .exists(),
            "{ns}: nothing moved"
        );
        assert!(
            !root
                .join(".kulyrax")
                .join(ns)
                .join("u1.guard.json")
                .exists(),
            "{ns}"
        );
    }
}

/// Ids 0.12.0 named differently (`K`) are still refused after the move.
#[test]
fn a_0_12_store_with_ids_named_differently_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    write(dir.path().join("users/K.json"), "{}");
    write(dir.path().join("users/a/b.json"), "{}");
    std::fs::create_dir_all(dir.path().join(".conduit/wal")).unwrap();
    let err = refused(&kv(dir.path(), &["users"]));
    assert!(err.contains("Rebuild the store"), "{err}");
    assert!(err.contains("K.json"), "{err}");
    assert!(
        err.contains(&format!("users{}a", std::path::MAIN_SEPARATOR)),
        "{err}"
    );
    assert!(dir.path().join("users/K.json").exists(), "nothing re-keyed");

    let dir = tempfile::tempdir().unwrap();
    write(
        dir.path().join(".conduit/entities/docs/K.done"),
        r#"{"last_sequence":5}"#,
    );
    assert!(refused(&doc(dir.path(), "docs")).contains("K.done"));
}

/// Where a killed migration can stop, and what the next start does.
#[test]
fn an_interrupted_move_finishes_on_the_next_start() {
    // Some entries moved, others not.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    {
        let k = kv(root, &["users"]);
        let d = doc(root, "docs");
        assert!(matches!(
            outcome(&k, &ev("U", "u1", 5)),
            AdapterOutcome::Created
        ));
        assert!(matches!(
            outcome(&d, &ev("U", "u1", 5)),
            AdapterOutcome::Created
        ));
    }
    as_0_12_1(root);
    std::fs::create_dir_all(root.join(".kulyrax")).unwrap();
    std::fs::rename(root.join(".conduit/kv"), root.join(".kulyrax/kv")).unwrap();
    let k = kv(root, &["users"]);
    let d = doc(root, "docs");
    k.prepare().unwrap();
    assert!(stale(&outcome(&k, &ev("U", "u1", 2))));
    assert!(stale(&outcome(&d, &ev("U", "u1", 2))));
    assert_migrated(root);
    drop((k, d));

    // Parked, the tombstone not yet written.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    {
        let k = kv(root, &["users"]);
        assert!(matches!(
            outcome(&k, &ev("U", "u1", 5)),
            AdapterOutcome::Created
        ));
    }
    std::fs::create_dir_all(root.join(".kulyrax-tmp-conduit")).unwrap();
    let k = kv(root, &["users"]);
    k.prepare().unwrap();
    assert!(stale(&outcome(&k, &ev("U", "u1", 2))));
    assert!(root.join(".conduit").is_file());
    assert!(!root.join(".kulyrax-tmp-conduit").exists());
    drop(k);

    // Parked, and a 0.12 process made a new `.conduit/` meanwhile: refused.
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join(".kulyrax-tmp-conduit")).unwrap();
    std::fs::create_dir_all(root.join(".conduit/kv")).unwrap();
    let err = refused(&kv(root, &["users"]));
    assert!(
        err.contains(".kulyrax-tmp-conduit") && err.contains(".conduit"),
        "{err}"
    );
}

// ---------------------------------------------------------------------------
// SQLite
// ---------------------------------------------------------------------------

fn sqlite(path: &Path) -> SqliteAdapter {
    let m: SqlMapping = kulyrax_core::yaml::from_str(
        "event: U\ntable: users\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  v: payload.v\n",
    )
    .unwrap();
    SqliteAdapter::new(
        "sql".into(),
        path.to_string_lossy().into(),
        10,
        SqlRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn sqlite_exec(path: &Path, sql: &str) {
    rusqlite::Connection::open(path)
        .unwrap()
        .execute_batch(sql)
        .unwrap();
}

fn sqlite_objects(path: &Path) -> Vec<(String, String)> {
    let c = rusqlite::Connection::open(path).unwrap();
    let mut s = c
        .prepare(
            "SELECT name, type FROM sqlite_master WHERE name LIKE '%projection_state%' \
             ORDER BY name",
        )
        .unwrap();
    s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

fn sqlite_0_12_1(path: &Path) {
    sqlite_exec(path, "CREATE TABLE users (id TEXT PRIMARY KEY, v INTEGER)");
    {
        let a = sqlite(path);
        a.prepare().unwrap();
        assert!(matches!(
            outcome(&a, &ev("U", "u1", 5)),
            AdapterOutcome::Created
        ));
        assert!(matches!(
            outcome(&a, &ev("U", "u2", 3)),
            AdapterOutcome::Created
        ));
    }
    sqlite_exec(
        path,
        "ALTER TABLE kulyrax_projection_state RENAME TO conduit_projection_state",
    );
}

#[test]
fn a_0_12_1_sqlite_database_opens_with_every_lane() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.db");
    sqlite_0_12_1(&db);
    let a = sqlite(&db);
    a.prepare().unwrap();
    assert!(stale(&outcome(&a, &ev("U", "u1", 2))));
    assert!(stale(&outcome(&a, &ev("U", "u2", 1))));
    assert!(matches!(
        outcome(&a, &ev("U", "u1", 9)),
        AdapterOutcome::Updated
    ));
    let objects = sqlite_objects(&db);
    assert!(
        objects.contains(&("conduit_projection_state".into(), "view".into()))
            && objects.contains(&("kulyrax_projection_state".into(), "table".into())),
        "{objects:?}"
    );
    // The tombstone refuses a 0.12-shaped guard write.
    let c = rusqlite::Connection::open(&db).unwrap();
    let write = c.execute(
        "INSERT INTO conduit_projection_state (target_table, entity_key, facet, last_sequence, \
         last_event_id, deleted, permanent, processed_at) VALUES ('users', '[\"u9\"]', '', 1, \
         'e', 0, 0, 'now')",
        [],
    );
    assert!(write.is_err(), "{write:?}");
    drop((c, a));

    // A second start is a no-op.
    let a = sqlite(&db);
    a.prepare().unwrap();
    assert!(stale(&outcome(&a, &ev("U", "u1", 3))));
    assert_eq!(sqlite_objects(&db), objects);
}

#[test]
fn both_sqlite_guard_tables_are_refused() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.db");
    sqlite_0_12_1(&db);
    sqlite_exec(&db, "CREATE TABLE kulyrax_projection_state (x TEXT)");
    let err = refused(&sqlite(&db));
    assert!(
        err.contains("conduit_projection_state") && err.contains("kulyrax_projection_state"),
        "{err}"
    );
}
