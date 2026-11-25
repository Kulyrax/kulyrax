//! Upgrading from a real Kulyrax 0.12.1 binary, per backend:
//!
//! 1. 0.12.1 projects three events (two creates, a delete) into a store.
//! 2. This version opens the store, migrates it, and gets older redeliveries:
//!    both are skipped as stale (one against the deleted lane) and a new
//!    entity is created. Every guard lane survived the rename.
//! 3. 0.12.1, started again on the migrated store, fails instead of starting
//!    on an empty one (the tombstone). Not for Neo4j (no tombstone possible),
//!    Redis or Elasticsearch (nothing renamed).
//!
//! The kill tests stop this version at each step of a migration
//! (`KULYRAX_CRASH_AT`) and check that the next start completes it. One more
//! leaves a 0.12.1 batch log behind (0.12.1 killed after writing it), which the
//! migration replays under the old paths before moving anything.
//!
//! **Gated on `KULYRAX_V0121_BIN`**, the path to a 0.12.1 `kulyrax` built with
//! `--features kulyrax-core/crash-points` (CI builds it from the v0.12.1 tag).
//! The networked backends are also gated on their usual URLs.

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};

fn new_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kulyrax"))
}

fn old_bin() -> Option<PathBuf> {
    std::env::var("KULYRAX_V0121_BIN")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(PathBuf::from)
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.trim().is_empty())
}

static SEQ: AtomicU32 = AtomicU32::new(0);

fn unique(prefix: &str) -> String {
    format!(
        "{prefix}{}x{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Sql,
    Document,
    KeyValue,
    Graph,
}

struct Backend {
    name: &'static str,
    kind: Kind,
    /// The adapter's `type:` and `config:` lines.
    adapter: String,
    /// The table, collection, namespace or label the mappings use.
    target: String,
    /// Whether 0.12.1 must fail against the migrated store.
    tombstone: bool,
    /// Drops what the test created on a shared server.
    cleanup: Option<Box<dyn Fn()>>,
}

impl Drop for Backend {
    fn drop(&mut self) {
        if let Some(cleanup) = &self.cleanup {
            cleanup();
        }
    }
}

fn write(path: &Path, content: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, content).unwrap();
}

fn project(b: &Backend) -> tempfile::TempDir {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path();
    write(
        &p.join("kulyrax.yaml"),
        &format!(
            "version: 1\nrouting:\n  file: routing.yaml\nadapters:\n  - id: store\n    \
             priority: 10\n    capabilities: [write, upsert, delete]\n{}\nsources:\n  - type: \
             directory\n    id: events\n    path: ./events\n    state_dir: ./state\n",
            b.adapter
        ),
    );
    write(&p.join("routing.yaml"), "U: [store]\nD: [store]\n");
    let m = p.join("mappings");
    std::fs::create_dir_all(m.join("sql")).unwrap();
    std::fs::create_dir_all(m.join("document")).unwrap();
    let t = &b.target;
    let (kind_dir, upsert, delete) = match b.kind {
        Kind::Sql => (
            "sql",
            format!(
                "event: U\ntable: {t}\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  v: payload.v\n"
            ),
            format!(
                "event: D\ntable: {t}\nprimary_key: id\nversion: 1\noperation: delete\ncolumns:\n  id: payload.id\n"
            ),
        ),
        Kind::Document => (
            "document",
            format!(
                "event: U\ncollection: {t}\nid: payload.id\nversion: 1\non_existing: replace\ndocument:\n  v: payload.v\n"
            ),
            format!(
                "event: D\ncollection: {t}\nid: payload.id\nversion: 1\noperation: delete\ndocument: {{}}\n"
            ),
        ),
        Kind::KeyValue => (
            "keyvalue",
            format!(
                "event: U\nnamespace: {t}\nkey: payload.id\nversion: 1\non_existing: replace\nvalue:\n  v: payload.v\n"
            ),
            format!(
                "event: D\nnamespace: {t}\nkey: payload.id\nversion: 1\noperation: delete\nvalue: {{}}\n"
            ),
        ),
        Kind::Graph => (
            "graph",
            format!(
                "kind: node\nevent: U\nlabel: {t}\nkey: payload.id\nversion: 1\non_existing: replace\nproperties:\n  v: payload.v\n"
            ),
            format!(
                "kind: node\nevent: D\nlabel: {t}\nkey: payload.id\nversion: 1\noperation: delete\n"
            ),
        ),
    };
    write(&m.join(kind_dir).join("u.yaml"), &upsert);
    write(&m.join(kind_dir).join("d.yaml"), &delete);
    dir
}

fn events(p: &Path, file: &str, events: &[(&str, &str, u64)]) {
    let lines: Vec<String> = events
        .iter()
        .map(|(t, id, seq)| {
            format!(
                r#"{{"event_id":"{file}-{id}-{seq}","event_type":"{t}","metadata":{{}},"payload":"{{\"id\":\"{id}\",\"v\":{seq}}}","sequence":{seq},"version":1}}"#
            )
        })
        .collect();
    write(&p.join("events").join(file), &(lines.join("\n") + "\n"));
}

fn run(bin: &Path, p: &Path, crash_at: Option<&str>) -> Output {
    let mut cmd = Command::new(bin);
    cmd.current_dir(p)
        .args([
            "run",
            "--config",
            "kulyrax.yaml",
            "--mappings",
            "mappings",
            "--once",
        ])
        .env_remove("KULYRAX_CRASH_AT")
        .env_remove("KULYRAX_TEST_RELABEL_BATCH")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(point) = crash_at {
        cmd.env("KULYRAX_CRASH_AT", point);
        if point.starts_with("guard-relabel-batch") {
            cmd.env("KULYRAX_TEST_RELABEL_BATCH", "1");
        }
    }
    cmd.output().unwrap()
}

fn text(out: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// (created, updated, deleted, skipped, failed), summed over the batches.
fn counts(out: &Output) -> [u64; 5] {
    let stdout = String::from_utf8_lossy(&out.stdout);
    let mut sum = [0; 5];
    for line in stdout.lines() {
        let Some(start) = line.find("(created ") else {
            continue;
        };
        let rest = &line[start + 1..];
        for (i, key) in ["created", "updated", "deleted", "skipped", "failed"]
            .iter()
            .enumerate()
        {
            let n = rest
                .split(", ")
                .find_map(|part| part.strip_prefix(&format!("{key} ")))
                .and_then(|n| n.split(|c: char| !c.is_ascii_digit()).next())
                .and_then(|n| n.parse::<u64>().ok())
                .unwrap_or(0);
            sum[i] += n;
        }
    }
    sum
}

fn ok(out: &Output, what: &str) {
    assert!(out.status.success(), "{what} failed:\n{}", text(out));
}

/// Step 1: 0.12.1 projects u1@5, u2@3, then deletes u2@4.
fn with_0_12_1(old: &Path, b: &Backend) -> tempfile::TempDir {
    let dir = project(b);
    events(
        dir.path(),
        "000001.ndjson",
        &[("U", "u1", 5), ("U", "u2", 3), ("D", "u2", 4)],
    );
    let out = run(old, dir.path(), None);
    ok(&out, &format!("{}: 0.12.1", b.name));
    assert_eq!(
        counts(&out)[..3],
        [2, 0, 1],
        "{}: 0.12.1\n{}",
        b.name,
        text(&out)
    );
    dir
}

/// Step 2: this version, with older redeliveries and one new entity.
fn upgraded(b: &Backend, p: &Path) {
    events(
        p,
        "000002.ndjson",
        &[("U", "u1", 2), ("U", "u2", 1), ("U", "u3", 1)],
    );
    let out = run(&new_bin(), p, None);
    ok(&out, &format!("{}: this version", b.name));
    assert_eq!(
        counts(&out),
        [1, 0, 0, 2, 0],
        "{}: (created, updated, deleted, skipped, failed)\n{}",
        b.name,
        text(&out)
    );
}

/// Step 3: 0.12.1 again, on the migrated store, fails.
fn old_fails(old: &Path, b: &Backend, p: &Path) {
    events(p, "000003.ndjson", &[("U", "u4", 1)]);
    let out = run(old, p, None);
    let [created, ..] = counts(&out);
    eprintln!(
        "{}: 0.12.1 on the migrated store says:
{}",
        b.name,
        text(&out)
    );
    assert!(
        !out.status.success() && created == 0,
        "{}: 0.12.1 started on a migrated store:\n{}",
        b.name,
        text(&out)
    );
}

fn upgrade(b: Backend) {
    let Some(old) = old_bin() else {
        eprintln!("SKIP: KULYRAX_V0121_BIN not set");
        return;
    };
    let dir = with_0_12_1(&old, &b);
    upgraded(&b, dir.path());
    if b.tombstone {
        old_fails(&old, &b, dir.path());
    }
}

/// Kill this version at `point` during the migration, then start it again.
fn killed_at(b: Backend, point: &str) {
    let Some(old) = old_bin() else {
        eprintln!("SKIP: KULYRAX_V0121_BIN not set");
        return;
    };
    let dir = with_0_12_1(&old, &b);
    events(
        dir.path(),
        "000002.ndjson",
        &[("U", "u1", 2), ("U", "u2", 1), ("U", "u3", 1)],
    );
    let out = run(&new_bin(), dir.path(), Some(point));
    let name = point.split(':').next().unwrap_or(point);
    assert!(
        !out.status.success() && text(&out).contains(&format!("crash point {name} reached")),
        "{}: {point} did not crash the run:\n{}",
        b.name,
        text(&out)
    );
    let out = run(&new_bin(), dir.path(), None);
    ok(&out, &format!("{}: after a kill at {point}", b.name));
    assert_eq!(
        counts(&out),
        [1, 0, 0, 2, 0],
        "{}: {point}\n{}",
        b.name,
        text(&out)
    );
    if b.tombstone {
        old_fails(&old, &b, dir.path());
    }
}

// ---------------------------------------------------------------------------
// Backends
// ---------------------------------------------------------------------------

fn file_store(kind: Kind) -> Backend {
    let (ty, name) = match kind {
        Kind::Document => ("file", "file document store"),
        Kind::KeyValue => ("keyvalue", "file key-value store"),
        _ => ("graph", "file graph store"),
    };
    Backend {
        name,
        kind,
        adapter: format!("    type: {ty}\n    config:\n      root: ./store"),
        target: if kind == Kind::Graph { "User" } else { "users" }.to_string(),
        tombstone: true,
        cleanup: None,
    }
}

fn sqlite(dir_for_db: &Path) -> Backend {
    let db = dir_for_db.join("app.db");
    rusqlite::Connection::open(&db)
        .unwrap()
        .execute_batch("CREATE TABLE users (id TEXT PRIMARY KEY, v INTEGER)")
        .unwrap();
    Backend {
        name: "sqlite",
        kind: Kind::Sql,
        adapter: format!(
            "    type: sqlite\n    config:\n      path: {}",
            db.display().to_string().replace('\\', "/")
        ),
        target: "users".into(),
        tombstone: true,
        cleanup: None,
    }
}

fn postgres() -> Option<Backend> {
    let base = env("KULYRAX_PG_URL")?;
    let schema = unique("kx_upgrade_");
    let mut c = ::postgres::Client::connect(&base, ::postgres::NoTls).unwrap();
    c.batch_execute(&format!(
        "CREATE SCHEMA {schema}; CREATE TABLE {schema}.users (id text PRIMARY KEY, v bigint)"
    ))
    .unwrap();
    let sep = if base.contains('?') { '&' } else { '?' };
    Some(Backend {
        name: "postgres",
        kind: Kind::Sql,
        adapter: format!(
            "    type: postgres\n    config:\n      url: \"{base}{sep}options=-c%20search_path%3D{schema}\""
        ),
        target: "users".into(),
        tombstone: true,
        cleanup: Some(Box::new(move || {
            if let Ok(mut c) = ::postgres::Client::connect(&base, ::postgres::NoTls) {
                let _ = c.batch_execute(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"));
            }
        })),
    })
}

fn mysql() -> Option<Backend> {
    use ::mysql::prelude::Queryable;
    let base = env("KULYRAX_MYSQL_URL")?;
    let db = unique("kx_upgrade_");
    let mut conn = ::mysql::Pool::new(base.as_str())
        .unwrap()
        .get_conn()
        .unwrap();
    conn.query_drop(format!("CREATE DATABASE {db}")).unwrap();
    conn.query_drop(format!(
        "CREATE TABLE {db}.users (id VARCHAR(64) PRIMARY KEY, v BIGINT)"
    ))
    .unwrap();
    let (server, _) = base.rsplit_once('/').unwrap();
    let server = server.to_string();
    Some(Backend {
        name: "mysql",
        kind: Kind::Sql,
        adapter: format!("    type: mysql\n    config:\n      url: {server}/{db}"),
        target: "users".into(),
        tombstone: true,
        cleanup: Some(Box::new(move || {
            if let Ok(mut conn) = ::mysql::Pool::new(base.as_str()).and_then(|p| p.get_conn()) {
                let _ = conn.query_drop(format!("DROP DATABASE IF EXISTS {db}"));
            }
        })),
    })
}

fn mongodb() -> Option<Backend> {
    let url = env("KULYRAX_MONGO_URL")?;
    Some(Backend {
        name: "mongodb",
        kind: Kind::Document,
        adapter: format!(
            "    type: mongodb\n    config:\n      url: \"{url}\"\n      database: {}",
            unique("kx_upgrade_")
        ),
        target: "users".into(),
        tombstone: true,
        cleanup: None,
    })
}

fn neo4j() -> Option<Backend> {
    let uri = env("KULYRAX_NEO4J_URI")?;
    let user = env("KULYRAX_NEO4J_USER").unwrap_or_else(|| "neo4j".into());
    let password = env("KULYRAX_NEO4J_PASSWORD").unwrap_or_else(|| "kulyrax-test".into());
    Some(Backend {
        name: "neo4j",
        kind: Kind::Graph,
        adapter: format!(
            "    type: neo4j\n    config:\n      uri: {uri}\n      user: {user}\n      password: {password}"
        ),
        target: unique("Upgrade"),
        tombstone: false,
        cleanup: None,
    })
}

fn redis() -> Option<Backend> {
    let url = env("KULYRAX_REDIS_URL")?;
    Some(Backend {
        name: "redis",
        kind: Kind::KeyValue,
        adapter: format!("    type: redis\n    config:\n      url: {url}"),
        target: unique("upgrade"),
        tombstone: false,
        cleanup: None,
    })
}

fn elasticsearch() -> Option<Backend> {
    let url = env("KULYRAX_ELASTICSEARCH_URL")?;
    Some(Backend {
        name: "elasticsearch",
        kind: Kind::Document,
        // Creating an index can wait on a cluster-state write; on a busy
        // single-node server that took over 30 s, past the 10 s default.
        adapter: format!(
            "    type: elasticsearch\n    config:\n      url: {url}\n      timeout_ms: 60000"
        ),
        target: unique("upgrade"),
        tombstone: false,
        cleanup: None,
    })
}

fn skip(name: &str) {
    eprintln!("SKIP: {name} URL not set");
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn upgrade_file_stores() {
    for kind in [Kind::KeyValue, Kind::Document, Kind::Graph] {
        upgrade(file_store(kind));
    }
}

#[test]
fn upgrade_sqlite() {
    let db = tempfile::tempdir().unwrap();
    upgrade(sqlite(db.path()));
}

#[test]
fn upgrade_postgres() {
    postgres().map_or_else(|| skip("postgres"), upgrade);
}

#[test]
fn upgrade_mysql() {
    mysql().map_or_else(|| skip("mysql"), upgrade);
}

#[test]
fn upgrade_mongodb() {
    mongodb().map_or_else(|| skip("mongodb"), upgrade);
}

#[test]
fn upgrade_neo4j() {
    neo4j().map_or_else(|| skip("neo4j"), upgrade);
}

#[test]
fn upgrade_redis() {
    redis().map_or_else(|| skip("redis"), upgrade);
}

#[test]
fn upgrade_elasticsearch() {
    elasticsearch().map_or_else(|| skip("elasticsearch"), upgrade);
}

#[test]
fn file_store_migration_killed_at_each_step() {
    for point in [
        "legacy-move:1",
        "legacy-move:2",
        "legacy-park:1",
        "legacy-tombstone:1",
    ] {
        for kind in [Kind::KeyValue, Kind::Document, Kind::Graph] {
            killed_at(file_store(kind), point);
        }
    }
}

#[test]
fn sqlite_migration_killed_before_commit() {
    let db = tempfile::tempdir().unwrap();
    killed_at(sqlite(db.path()), "guard-table-renamed:1");
}

#[test]
fn postgres_migration_killed_before_commit() {
    postgres().map_or_else(
        || skip("postgres"),
        |b| killed_at(b, "guard-table-renamed:1"),
    );
}

#[test]
fn mysql_migration_killed_at_each_step() {
    for point in ["guard-tombstone-created:1", "guard-table-renamed:1"] {
        mysql().map_or_else(|| skip("mysql"), |b| killed_at(b, point));
    }
}

#[test]
fn mongodb_migration_killed_at_each_step() {
    for point in ["guard-tombstone-created:1", "guard-table-renamed:1"] {
        mongodb().map_or_else(|| skip("mongodb"), |b| killed_at(b, point));
    }
}

#[test]
fn neo4j_migration_killed_between_batches() {
    neo4j().map_or_else(|| skip("neo4j"), |b| killed_at(b, "guard-relabel-batch:1"));
}

/// 0.12.1 killed after writing a batch log: the log names `.conduit/` paths.
/// This version replays it under those paths, then moves the store, then
/// reads the batch again from the source: all of it is stale.
#[test]
fn a_0_12_1_batch_log_is_replayed_before_the_move() {
    let Some(old) = old_bin() else {
        eprintln!("SKIP: KULYRAX_V0121_BIN not set");
        return;
    };
    for kind in [Kind::KeyValue, Kind::Document, Kind::Graph] {
        let b = file_store(kind);
        let dir = project(&b);
        let p = dir.path();
        events(
            p,
            "000001.ndjson",
            &[("U", "u1", 5), ("U", "u2", 3), ("D", "u2", 4)],
        );
        let out = run(&old, p, Some("after-log:1"));
        assert!(!out.status.success(), "{}: 0.12.1 did not crash", b.name);
        let logs = std::fs::read_dir(p.join("store/.conduit/wal"))
            .map(|d| d.count())
            .unwrap_or(0);
        assert!(logs > 0, "{}: no 0.12.1 batch log left", b.name);

        events(
            p,
            "000002.ndjson",
            &[("U", "u1", 2), ("U", "u2", 1), ("U", "u3", 1)],
        );
        let out = run(&new_bin(), p, None);
        ok(&out, &format!("{}: replay", b.name));
        assert_eq!(counts(&out), [1, 0, 0, 5, 0], "{}\n{}", b.name, text(&out));
        assert!(p.join("store/.conduit").is_file(), "{}: tombstone", b.name);
    }
}
