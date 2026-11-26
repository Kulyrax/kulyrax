//! Shared fixtures for the Phase 17 source-loop tests.

#![allow(dead_code)]

use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::{
    AdapterConfig, KulyraxConfig, SqliteAdapterConfig, SqliteConfig,
};

use std::collections::HashMap;
use std::path::Path;

pub fn config(db_path: &Path) -> KulyraxConfig {
    KulyraxConfig::new(
        "routing.yaml",
        vec![AdapterConfig::Sqlite(SqliteAdapterConfig::new(
            "sql-primary",
            10,
            SqliteConfig::new(db_path.to_string_lossy()),
        ))],
    )
}

pub fn routing() -> HashMap<String, Vec<String>> {
    let mut r = HashMap::new();
    r.insert("UserUpserted".to_string(), vec!["sql-primary".into()]);
    r
}

pub fn sql_mappings() -> HashMap<String, SqlMapping> {
    let mut m = HashMap::new();
    m.insert(
        "UserUpserted".to_string(),
        kulyrax_core::yaml::from_str::<SqlMapping>(
            "event: UserUpserted\ntable: users\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n  id: payload.id\n  name: payload.name\n",
        )
        .unwrap(),
    );
    m
}

pub fn init_db(db_path: &Path) {
    let conn = rusqlite::Connection::open(db_path).unwrap();
    conn.execute("CREATE TABLE users (id TEXT PRIMARY KEY, name TEXT)", [])
        .unwrap();
}

pub fn user_event(sequence: u64, id: &str, name: &str) -> Event {
    Event::new(
        format!("evt-{id}-{sequence}"),
        "UserUpserted".to_string(),
        sequence,
        format!(r#"{{ "id": "{id}", "name": "{name}" }}"#),
    )
}

pub fn write_event_file(dir: &Path, name: &str, event: &Event) {
    std::fs::create_dir_all(dir).unwrap();
    std::fs::write(dir.join(name), serde_json::to_string(event).unwrap()).unwrap();
}

/// Run `sources` once against a fresh DB, default options unless `opts` given.
pub fn run_once(
    db_path: &Path,
    sources: Vec<Box<dyn kulyrax_core::source::EventSource>>,
    opts: Option<kulyrax_core::SourceRunOptions>,
) -> Result<kulyrax_core::SourceRunReport, kulyrax_core::SourceError> {
    let opts = opts.unwrap_or({
        let mut o = kulyrax_core::SourceRunOptions::default();
        o.dlq_dir = None;
        o.max_batch = 256;
        o.max_batch_delay = std::time::Duration::ZERO;
        o.mode = kulyrax_core::RunMode::Once;
        o.retry_budget = 2;
        o
    });
    let stop = std::sync::atomic::AtomicBool::new(false);
    kulyrax_core::run_sources(
        &config(db_path),
        routing(),
        sql_mappings(),
        HashMap::new(),
        HashMap::new(),
        HashMap::new(),
        sources,
        &opts,
        &stop,
    )
}

/// `(name, last_sequence)` for a `users` row, or `None` if absent.
pub fn read_user(db_path: &Path, id: &str) -> Option<(String, i64)> {
    let conn = rusqlite::Connection::open(db_path).ok()?;
    conn.query_row(
        "SELECT u.name, s.last_sequence
         FROM users u
         JOIN kulyrax_projection_state s
           ON s.target_table = 'users' AND s.entity_key = json_array(u.id)
         WHERE u.id = ?1",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .ok()
}

/// Prepare an adapter the way every write path does before projecting.
pub fn prepared<A: kulyrax_core::adapter::StorageAdapter>(adapter: A) -> A {
    adapter.prepare().expect("prepare adapter");
    adapter
}
