//! `pipeline::rebuild` on SQLite: the full history is projected into shadow
//! tables and swapped in with one transaction, replacing each table and its
//! guard lanes; the swap is refused if another writer projected into the
//! tables meanwhile.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use kulyrax_core::adapter::sql::exec::ShadowTable;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::adapter::{AdapterOutcome, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::pipeline;
use kulyrax_core::replay::ReplayRunOptions;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

const MAPPING_V1: &str = "event: UserUpserted\ntable: users\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  name: payload.name\n";
const MAPPING_V2: &str = "event: UserUpserted\ntable: users\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  name: payload.name\n  email: payload.email\n";

struct Project {
    _dir: tempfile::TempDir,
    root: PathBuf,
    db: PathBuf,
    out: PathBuf,
}

impl Project {
    fn config(&self) -> PathBuf {
        self.root.join("config.yaml")
    }
    fn events(&self) -> PathBuf {
        self.root.join("events.ndjson")
    }
    fn mappings(&self, version: &str) -> PathBuf {
        self.root.join(format!("mappings-{version}"))
    }
}

fn user(seq: u64, id: &str, name: &str) -> Event {
    Event::new(
        format!("evt-{id}-{seq}"),
        "UserUpserted",
        seq,
        serde_json::json!({ "id": id, "name": name, "email": format!("{name}@x") }).to_string(),
    )
}

fn history() -> Vec<Event> {
    vec![
        user(1, "u1", "ada"),
        user(1, "u2", "grace"),
        user(2, "u1", "ada2"),
        user(1, "u3", "edsger"),
    ]
}

fn write_events(path: &Path, events: &[Event]) {
    let lines: Vec<String> = events
        .iter()
        .map(|e| serde_json::to_string(e).unwrap())
        .collect();
    std::fs::write(path, lines.join("\n")).unwrap();
}

/// A project with a SQLite adapter `sql` and a file adapter `docs`, both
/// routed `UserUpserted`, and mapping sets `v1` / `v2` for `users`.
fn project() -> Project {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let db = root.join("app.db");
    let out = root.join("out");
    std::fs::write(
        root.join("config.yaml"),
        format!(
            "version: 1\nrouting:\n  file: routing.yaml\nadapters:\n  - type: sqlite\n    id: sql\n    priority: 10\n    capabilities: [write, upsert]\n    config:\n      path: {db}\n  - type: file\n    id: docs\n    priority: 20\n    capabilities: [write, upsert]\n    config:\n      root: {out}\n",
            db = db.display().to_string().replace('\\', "/"),
            out = out.display().to_string().replace('\\', "/"),
        ),
    )
    .unwrap();
    std::fs::write(root.join("routing.yaml"), "UserUpserted: [sql, docs]\n").unwrap();
    for (version, sql) in [("v1", MAPPING_V1), ("v2", MAPPING_V2)] {
        let m = root.join(format!("mappings-{version}"));
        std::fs::create_dir_all(m.join("sql")).unwrap();
        std::fs::create_dir_all(m.join("document")).unwrap();
        std::fs::write(m.join("sql/users.yaml"), sql).unwrap();
        std::fs::write(
            m.join("document/users.yaml"),
            "event: UserUpserted\ncollection: users\nversion: 1\nid: payload.id\non_existing: replace\ndocument:\n  name: payload.name\n",
        )
        .unwrap();
    }
    let conn = rusqlite::Connection::open(&db).unwrap();
    conn.execute_batch(
        "CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT, email TEXT);
         CREATE INDEX users_name ON users (name);",
    )
    .unwrap();
    write_events(&root.join("events.ndjson"), &history());
    Project {
        _dir: dir,
        root,
        db,
        out,
    }
}

fn opts() -> ReplayRunOptions {
    ReplayRunOptions::default()
}

fn rows(db: &Path) -> Vec<(String, Option<String>, Option<String>)> {
    let conn = rusqlite::Connection::open(db).unwrap();
    let mut stmt = conn
        .prepare("SELECT id, name, email FROM users ORDER BY id")
        .unwrap();
    stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn query_strings(db: &Path, sql: &str) -> Vec<String> {
    let conn = rusqlite::Connection::open(db).unwrap();
    let mut stmt = conn.prepare(sql).unwrap();
    stmt.query_map([], |r| r.get(0))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap()
}

fn guard_lanes(db: &Path) -> Vec<String> {
    query_strings(
        db,
        "SELECT target_table || ' ' || entity_key || ' ' || last_sequence
         FROM kulyrax_projection_state ORDER BY 1",
    )
}

fn doc_files(out: &Path) -> usize {
    walk(out)
}

fn walk(dir: &Path) -> usize {
    std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .flatten()
                .map(|e| {
                    if e.path().is_dir() {
                        walk(&e.path())
                    } else {
                        1
                    }
                })
                .sum()
        })
        .unwrap_or(0)
}

#[test]
fn rebuild_replaces_a_table_after_a_mapping_change() {
    let p = project();
    pipeline::replay(&p.config(), &p.mappings("v1"), &p.events(), &opts(), |_| {}).unwrap();
    // A row no event projected, as a damaged projection would hold.
    rusqlite::Connection::open(&p.db)
        .unwrap()
        .execute("INSERT INTO users (id, name) VALUES ('ghost', 'boo')", [])
        .unwrap();
    let docs_before = doc_files(&p.out);
    assert_eq!(
        rows(&p.db)[0],
        ("ghost".into(), Some("boo".into()), None),
        "v1 has no email; the ghost sorts first"
    );

    let report = pipeline::rebuild(
        &p.config(),
        &p.mappings("v2"),
        &p.events(),
        "sql",
        &opts(),
        |_| {},
    )
    .unwrap();
    assert_eq!(report.tables, vec!["users"]);
    assert_eq!(report.replay.events_processed, 4);

    assert_eq!(
        rows(&p.db),
        vec![
            ("u1".into(), Some("ada2".into()), Some("ada2@x".into())),
            ("u2".into(), Some("grace".into()), Some("grace@x".into())),
            ("u3".into(), Some("edsger".into()), Some("edsger@x".into())),
        ],
        "the v2 projection, without the ghost"
    );
    assert_eq!(
        guard_lanes(&p.db),
        vec![
            r#"users ["u1"] 2"#,
            r#"users ["u2"] 1"#,
            r#"users ["u3"] 1"#,
        ],
        "the guard lanes are the rebuild's, under the table's own name"
    );
    assert_eq!(
        query_strings(
            &p.db,
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'users%' ORDER BY 1"
        ),
        vec!["users"],
        "no shadow table is left"
    );
    assert_eq!(
        query_strings(
            &p.db,
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'users' AND sql IS NOT NULL"
        ),
        vec!["users_name__kulyrax_rebuild"],
        "the index came along"
    );
    assert_eq!(
        doc_files(&p.out),
        docs_before,
        "only the rebuilt adapter is written"
    );

    // A second rebuild works too, and the index name alternates back.
    pipeline::rebuild(
        &p.config(),
        &p.mappings("v2"),
        &p.events(),
        "sql",
        &opts(),
        |_| {},
    )
    .unwrap();
    assert_eq!(rows(&p.db).len(), 3);
    assert_eq!(
        query_strings(
            &p.db,
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'users' AND sql IS NOT NULL"
        ),
        vec!["users_name"]
    );

    // Live projection carries on against the rebuilt table and its guards.
    write_events(
        &p.events(),
        &[history(), vec![user(3, "u1", "ada3")]].concat(),
    );
    let live =
        pipeline::replay(&p.config(), &p.mappings("v2"), &p.events(), &opts(), |_| {}).unwrap();
    assert_eq!(live.events_failed, 0);
    assert_eq!(
        rows(&p.db)[0],
        ("u1".into(), Some("ada3".into()), Some("ada3@x".into()))
    );
}

fn live_adapter(db: &Path) -> SqliteAdapter {
    let mapping: SqlMapping = kulyrax_core::yaml::from_str(MAPPING_V1).unwrap();
    let adapter = SqliteAdapter::new(
        "sql".into(),
        db.to_string_lossy().into(),
        10,
        SqlRuntimeBuilder::new(HashMap::from([("UserUpserted".to_string(), mapping)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );
    adapter.prepare().unwrap();
    adapter
}

/// A writer projects into the table between the rebuild's start and its
/// swap: the swap is refused, and the table is left as that writer left it.
#[test]
fn rebuild_refuses_to_swap_when_another_writer_projected() {
    let p = project();
    let live = live_adapter(&p.db);
    for e in history() {
        assert!(live.handle(&e).is_success());
    }
    let tables = [ShadowTable::new("users", "users__kulyrax_rebuild")];
    let rebuilder = live.shadow_rebuild().expect("SQLite supports rebuilds");
    let before = rebuilder.start_rebuild(&tables).unwrap();

    let r = live.handle(&user(5, "u1", "during"));
    assert!(
        matches!(r.outcome, AdapterOutcome::Updated),
        "{:?}",
        r.outcome
    );

    let err = rebuilder
        .finish_rebuild(&tables, before)
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("another writer projected into users"), "{err}");
    assert_eq!(
        rows(&p.db)[0],
        ("u1".into(), Some("during".into()), None),
        "the table is untouched"
    );
    assert!(guard_lanes(&p.db).contains(&r#"users ["u1"] 5"#.to_string()));

    // A redelivery skips: the guard is not newer, so nothing changes.
    let before = rebuilder.start_rebuild(&tables).unwrap();
    let r = live.handle(&user(5, "u1", "during"));
    assert!(
        matches!(r.outcome, AdapterOutcome::Skipped(_)),
        "{:?}",
        r.outcome
    );
    rebuilder.finish_rebuild(&tables, before).unwrap();
    assert_eq!(rows(&p.db), vec![], "the (empty) shadow was swapped in");
}

/// An event the new mappings cannot project fails the rebuild before the
/// swap: the old table stays, and the shadow is left for inspection.
#[test]
fn rebuild_with_a_failing_event_swaps_nothing() {
    let p = project();
    pipeline::replay(&p.config(), &p.mappings("v1"), &p.events(), &opts(), |_| {}).unwrap();
    let before = rows(&p.db);
    // v2 maps an email column the table lacks in this copy of the project.
    let broken = p.root.join("mappings-broken");
    std::fs::create_dir_all(broken.join("sql")).unwrap();
    std::fs::create_dir_all(broken.join("document")).unwrap();
    std::fs::write(
        broken.join("sql/users.yaml"),
        MAPPING_V2.replace("email: payload.email", "nickname: payload.email"),
    )
    .unwrap();
    std::fs::copy(
        p.mappings("v1").join("document/users.yaml"),
        broken.join("document/users.yaml"),
    )
    .unwrap();

    let err = pipeline::rebuild(&p.config(), &broken, &p.events(), "sql", &opts(), |_| {})
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default();
    assert!(err.contains("were not swapped in"), "{err}");
    assert!(err.contains("users__kulyrax_rebuild"), "{err}");
    assert_eq!(rows(&p.db), before, "the old table is untouched");
}

#[test]
fn rebuild_needs_an_adapter_with_sql_tables() {
    let p = project();
    let err = |adapter: &str| {
        pipeline::rebuild(
            &p.config(),
            &p.mappings("v2"),
            &p.events(),
            adapter,
            &opts(),
            |_| {},
        )
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default()
    };
    assert!(
        err("docs").contains("cannot rebuild into shadow tables"),
        "{}",
        err("docs")
    );
    assert!(
        err("nope").contains("no adapter \"nope\""),
        "{}",
        err("nope")
    );
}

// ---------------------------------------------------------------------------
// Preflight: refused before anything is created
// ---------------------------------------------------------------------------

/// Add `ddl` to a projected project, run a rebuild, and return its error,
/// after checking that it created no shadow table and no guard lane.
fn refused_with(ddl: &str) -> String {
    let p = project();
    pipeline::replay(&p.config(), &p.mappings("v1"), &p.events(), &opts(), |_| {}).unwrap();
    rusqlite::Connection::open(&p.db)
        .unwrap()
        .execute_batch(ddl)
        .unwrap();
    let before = (rows(&p.db), guard_lanes(&p.db));

    let err = pipeline::rebuild(
        &p.config(),
        &p.mappings("v2"),
        &p.events(),
        "sql",
        &opts(),
        |_| {},
    )
    .err()
    .map(|e| e.to_string())
    .unwrap_or_default();
    assert!(
        query_strings(
            &p.db,
            "SELECT name FROM sqlite_master WHERE name LIKE '%__kulyrax_rebuild%'"
        )
        .is_empty(),
        "no shadow table or index"
    );
    assert_eq!(
        query_strings(
            &p.db,
            "SELECT target_table FROM kulyrax_projection_state WHERE target_table <> 'users'"
        ),
        Vec::<String>::new(),
        "no shadow guard lanes"
    );
    assert_eq!((rows(&p.db), guard_lanes(&p.db)), before, "nothing changed");
    err
}

#[test]
fn rebuild_refuses_a_table_a_view_reads() {
    let err = refused_with("CREATE VIEW users_active AS SELECT id FROM users WHERE name <> ''");
    assert!(err.contains("users: view users_active"), "{err}");
}

#[test]
fn rebuild_refuses_a_table_with_a_trigger() {
    let err = refused_with(
        "CREATE TABLE audit (id TEXT);
         CREATE TRIGGER users_audit AFTER INSERT ON users BEGIN INSERT INTO audit VALUES (new.id); END;",
    );
    assert!(err.contains("users: trigger users_audit"), "{err}");
}

#[test]
fn rebuild_refuses_a_table_a_foreign_key_points_at() {
    let err = refused_with(
        "CREATE TABLE orders (id TEXT PRIMARY KEY, user_id TEXT REFERENCES users (id))",
    );
    assert!(err.contains("users: foreign key from orders"), "{err}");
}

#[test]
fn preflight_ignores_names_that_only_contain_the_table_name() {
    let p = project();
    rusqlite::Connection::open(&p.db)
        .unwrap()
        .execute_batch(
            "CREATE TABLE users_archive (id TEXT);
             CREATE VIEW old_users AS SELECT id FROM users_archive;",
        )
        .unwrap();
    pipeline::rebuild(
        &p.config(),
        &p.mappings("v2"),
        &p.events(),
        "sql",
        &opts(),
        |_| {},
    )
    .unwrap();
}
