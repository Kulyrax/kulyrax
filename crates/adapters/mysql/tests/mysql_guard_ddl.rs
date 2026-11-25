//! The guard read, target write and guard write of one event are a single
//! transaction, checked by making the guard write fail.
//!
//! **Gated on `KULYRAX_MYSQL_URL`**, like `mysql_backend.rs`. Its own test
//! file, so its own process: it adds a `CHECK` constraint to the shared
//! `kulyrax_projection_state`, and that `ALTER TABLE` rebuilds the table
//! while waiting on every open transaction on it. Run alongside the batch
//! tests, whose transactions span many events, the rebuild made them fail
//! with ERROR 1213.

use kulyrax_adapter_mysql::MySqlAdapter;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::{AdapterOutcome, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use mysql::prelude::Queryable;
use std::collections::HashMap;
use std::sync::Arc;

mod common;

type R = Result<(), Box<dyn std::error::Error>>;

macro_rules! require_mysql {
    ($url:ident) => {
        let Some($url) = common::mysql_url() else {
            eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
            return Ok(());
        };
    };
}

fn fresh_table() -> String {
    "t_guard_ddl".to_string()
}

fn my_exec(url: &str, sql: &str) -> R {
    let pool = mysql::Pool::new(url)?;
    let mut conn = pool.get_conn()?;
    conn.query_drop(sql)?;
    Ok(())
}

fn my_adapter(url: &str, mappings: HashMap<String, SqlMapping>) -> MySqlAdapter {
    let adapter = MySqlAdapter::new(
        "mysql".into(),
        url,
        2,
        10,
        SqlRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );
    adapter.prepare().expect("prepare adapter");
    adapter
}

fn upsert_mapping(event: &str, table: &str, cols: &str) -> SqlMapping {
    kulyrax_core::yaml::from_str(&format!(
        "event: {event}\ntable: {table}\nprimary_key: id\nversion: 1\non_existing: replace\ncolumns:\n{cols}"
    ))
    .unwrap()
}

fn ev(event_type: &str, seq: u64, payload: serde_json::Value) -> Event {
    Event::new(
        format!("evt-{event_type}-{seq}"),
        event_type,
        seq,
        payload.to_string(),
    )
}

#[test]
fn mysql_failed_guard_write_leaves_neither_row() -> R {
    require_mysql!(url);
    let t = fresh_table();
    my_exec(
        &url,
        &format!(
            "DROP TABLE IF EXISTS {t}; CREATE TABLE {t} (id VARCHAR(64) PRIMARY KEY, name VARCHAR(64))"
        ),
    )?;
    my_exec(
        &url,
        &format!("DELETE FROM kulyrax_projection_state WHERE target_table = '{t}'"),
    )
    .ok();

    let mut m = HashMap::new();
    m.insert(
        "Atomic".into(),
        upsert_mapping("Atomic", &t, "  id: payload.id\n  name: payload.name\n"),
    );
    let a = my_adapter(&url, m);

    // An ordinary write first, for contrast with the forced failure.
    let ok = a.handle(&ev(
        "Atomic",
        1,
        serde_json::json!({ "id": "u1", "name": "a" }),
    ));
    assert!(
        matches!(ok.outcome, AdapterOutcome::Created),
        "{:?}",
        ok.outcome
    );

    let fail = ev("Atomic", 2, serde_json::json!({ "id": "u2", "name": "b" }));
    let check = format!("chk_{}_{t}", std::process::id());
    my_exec(
        &url,
        &format!(
            "ALTER TABLE kulyrax_projection_state ADD CONSTRAINT {check} CHECK (last_event_id <> '{}')",
            fail.event_id
        ),
    )?;
    let r = a.handle(&fail);
    let dropped = my_exec(
        &url,
        &format!("ALTER TABLE kulyrax_projection_state DROP CHECK {check}"),
    );

    assert!(
        matches!(r.outcome, AdapterOutcome::Failed(_)),
        "{:?}",
        r.outcome
    );
    let pool = mysql::Pool::new(url.as_str())?;
    let mut conn = pool.get_conn()?;
    let target: Option<String> =
        conn.exec_first(format!("SELECT name FROM {t} WHERE id = 'u2'"), ())?;
    let guards: Option<i64> = conn.exec_first(
        format!(
            "SELECT count(*) FROM kulyrax_projection_state WHERE target_table = '{t}' AND entity_key = '[\"u2\"]'"
        ),
        (),
    )?;
    dropped?;
    assert_eq!(
        target, None,
        "the target row must roll back with the guard write"
    );
    assert_eq!(guards, Some(0), "no guard row");
    Ok(())
}
