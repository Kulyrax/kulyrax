//! Shared fixtures for the Phase 17 source-loop tests.

#![allow(dead_code)]

use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::{
    AdapterConfig, KulyraxConfig, RoutingConfig, SqliteAdapterConfig, SqliteConfig,
};

use std::collections::HashMap;
use std::path::Path;

pub fn config(db_path: &Path) -> KulyraxConfig {
    KulyraxConfig {
        version: 1,
        routing: RoutingConfig {
            file: "routing.json".into(),
        },
        adapters: vec![AdapterConfig::Sqlite(SqliteAdapterConfig {
            id: "sql-primary".into(),
            priority: 10,
            config: SqliteConfig {
                path: db_path.to_string_lossy().into(),
            },
            capabilities: None,
            depends_on: vec![],
        })],
        failure_policy: Default::default(),
        migration_policy: Default::default(),
        sources: Vec::new(),
    }
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
        serde_yaml::from_str::<SqlMapping>(
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
    Event {
        event_id: format!("evt-{id}-{sequence}"),
        event_type: "UserUpserted".to_string(),
        payload: format!(r#"{{ "id": "{id}", "name": "{name}" }}"#),
        metadata: HashMap::new(),
        version: 1,
        sequence,
    }
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
    let opts = opts.unwrap_or(kulyrax_core::SourceRunOptions {
        mode: kulyrax_core::RunMode::Once,
        max_batch: 256,
        retry_budget: 2,
        dlq_dir: None,
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
         JOIN conduit_projection_state s
           ON s.target_table = 'users' AND s.entity_key = json_array(u.id)
         WHERE u.id = ?1",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .ok()
}
