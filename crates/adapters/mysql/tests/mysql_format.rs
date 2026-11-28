//! Step 5's format marker on MySQL (decision 2026-11-28-format-versions):
//! `kulyrax_format` sits beside the guard table in the connection's
//! database; it is created insert-if-absent, so concurrent first starts leave
//! one row; a newer one is refused before the guard table is touched; a user
//! who may not create it is refused with the grant it needs.
//!
//! **Gated on `KULYRAX_MYSQL_URL`.** Each test runs in a database of its own.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use kulyrax_adapter_mysql::MySqlAdapter;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::{AdapterError, StorageAdapter};
use kulyrax_core::format::FormatProblem;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;
use mysql::prelude::Queryable;

type R = Result<(), Box<dyn std::error::Error>>;

static SEQ: AtomicU32 = AtomicU32::new(0);

/// A fresh database holding a `users` table: its name and a URL using it.
fn database(base: &str) -> Result<(String, String), Box<dyn std::error::Error>> {
    let name = format!(
        "kx_format_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    exec(
        base,
        &format!("DROP DATABASE IF EXISTS {name}; CREATE DATABASE {name}"),
    )?;
    let (server, _) = base
        .rsplit_once('/')
        .ok_or("KULYRAX_MYSQL_URL has no database")?;
    let url = format!("{server}/{name}");
    exec(
        &url,
        "CREATE TABLE users (id VARCHAR(64) PRIMARY KEY, v BIGINT)",
    )?;
    Ok((name, url))
}

/// `url` with its user and password replaced.
fn as_user(url: &str, user: &str, password: &str) -> Result<String, Box<dyn std::error::Error>> {
    let (scheme, rest) = url.split_once("://").ok_or("no scheme")?;
    let host = rest.split_once('@').map_or(rest, |(_, h)| h);
    Ok(format!("{scheme}://{user}:{password}@{host}"))
}

fn exec(url: &str, sql: &str) -> R {
    mysql::Pool::new(url)?.get_conn()?.query_drop(sql)?;
    Ok(())
}

fn adapter(url: &str) -> MySqlAdapter {
    let m: SqlMapping = kulyrax_core::yaml::from_str(
        "event: U\ntable: users\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n  v: payload.v\n",
    )
    .expect("mapping");
    MySqlAdapter::new(
        "mysql".into(),
        url,
        2,
        10,
        SqlRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn markers(url: &str) -> Result<Vec<(String, i64)>, Box<dyn std::error::Error>> {
    Ok(mysql::Pool::new(url)?
        .get_conn()?
        .query("SELECT component, version FROM kulyrax_format ORDER BY component")?)
}

fn has_table(url: &str, table: &str) -> Result<bool, Box<dyn std::error::Error>> {
    let n: Option<i64> = mysql::Pool::new(url)?.get_conn()?.exec_first(
        "SELECT COUNT(*) FROM information_schema.tables
         WHERE table_schema = DATABASE() AND table_name = ?",
        (table,),
    )?;
    Ok(n == Some(1))
}

/// Change 2: the marker is in the guard table's database.
#[test]
fn the_marker_sits_beside_the_guard_table() -> R {
    let Ok(base) = std::env::var("KULYRAX_MYSQL_URL") else {
        eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
        return Ok(());
    };
    let (name, url) = database(&base)?;
    adapter(&url).prepare()?;
    adapter(&url).prepare()?;
    assert!(has_table(&url, "kulyrax_format")?);
    assert!(has_table(&url, "kulyrax_projection_state")?);
    assert_eq!(markers(&url)?, [("guards".to_string(), 1)]);
    exec(&base, &format!("DROP DATABASE {name}"))
}

/// A database with guard rows and no marker (0.12.x): format 1, stamped.
#[test]
fn a_0_12_database_is_stamped_as_format_1() -> R {
    let Ok(base) = std::env::var("KULYRAX_MYSQL_URL") else {
        eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
        return Ok(());
    };
    let (name, url) = database(&base)?;
    adapter(&url).prepare()?;
    exec(
        &url,
        "DROP TABLE kulyrax_format;
         INSERT INTO kulyrax_projection_state (target_table, entity_key, last_sequence, last_event_id)
         VALUES ('users', '[\"u1\"]', 7, 'e7')",
    )?;
    adapter(&url).prepare()?;
    assert_eq!(markers(&url)?, [("guards".to_string(), 1)]);
    let seq: Option<i64> = mysql::Pool::new(url.as_str())?
        .get_conn()?
        .query_first("SELECT last_sequence FROM kulyrax_projection_state")?;
    assert_eq!(seq, Some(7));
    exec(&base, &format!("DROP DATABASE {name}"))
}

#[test]
fn a_newer_database_is_refused_before_the_guard_table() -> R {
    let Ok(base) = std::env::var("KULYRAX_MYSQL_URL") else {
        eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
        return Ok(());
    };
    let (name, url) = database(&base)?;
    exec(
        &url,
        "CREATE TABLE kulyrax_format (component VARCHAR(64) PRIMARY KEY, version INT NOT NULL);
         INSERT INTO kulyrax_format VALUES ('guards', 2)",
    )?;
    let err = adapter(&url).prepare().expect_err("refused");
    let AdapterError::Format(e) = err else {
        panic!("not a format refusal: {err}");
    };
    assert_eq!(
        e.problem,
        FormatProblem::Newer {
            found: 2,
            supported: 1
        }
    );
    assert!(e.store.contains(&name), "{}", e.store);
    assert!(!has_table(&url, "kulyrax_projection_state")?);
    exec(&base, &format!("DROP DATABASE {name}"))
}

/// Change 3: concurrent first starts leave one marker row, one version.
#[test]
fn concurrent_first_starts_leave_one_marker() -> R {
    let Ok(base) = std::env::var("KULYRAX_MYSQL_URL") else {
        eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
        return Ok(());
    };
    let (name, url) = database(&base)?;
    let results: Vec<Result<(), String>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|_| s.spawn(|| adapter(&url).prepare().map_err(|e| e.to_string())))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err("panicked".into())))
            .collect()
    });
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(markers(&url)?, [("guards".to_string(), 1)]);
    exec(&base, &format!("DROP DATABASE {name}"))
}

/// Change 4: a user who may not create the marker is refused, naming the
/// database and the grant; with that grant the same user starts.
#[test]
fn a_user_without_create_names_the_grant() -> R {
    let Ok(base) = std::env::var("KULYRAX_MYSQL_URL") else {
        eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
        return Ok(());
    };
    let (name, url) = database(&base)?;
    let user = format!("{name}_app");
    adapter(&url).prepare()?;
    exec(
        &url,
        &format!(
            "DROP TABLE kulyrax_format;
             DROP USER IF EXISTS '{user}'@'%';
             CREATE USER '{user}'@'%' IDENTIFIED BY 'app-pw';
             GRANT SELECT, INSERT, UPDATE, DELETE ON {name}.* TO '{user}'@'%'"
        ),
    )?;
    let app = as_user(&url, &user, "app-pw")?;
    let err = adapter(&app).prepare().expect_err("refused");
    let AdapterError::Format(e) = &err else {
        panic!("not a format refusal: {err}");
    };
    let FormatProblem::MarkerNotCreated { grant, .. } = &e.problem else {
        panic!("not MarkerNotCreated: {e}");
    };
    assert_eq!(grant, &format!("CREATE on `{name}`.*"));
    assert!(e.store.contains(&name), "{}", e.store);
    assert!(!e.to_string().contains("app-pw"), "no password: {e}");

    exec(&url, &format!("GRANT CREATE ON {name}.* TO '{user}'@'%'"))?;
    adapter(&app).prepare()?;
    assert_eq!(markers(&url)?, [("guards".to_string(), 1)]);
    exec(
        &base,
        &format!("DROP DATABASE {name}; DROP USER '{user}'@'%'"),
    )
}

/// Mappings built in code (not loaded from files) that target Kulyrax's own
/// tables are refused at `prepare`, before any connection: no server needed.
#[test]
fn prepare_refuses_a_mapping_on_kulyrax_tables() {
    for table in ["kulyrax_format", "Kulyrax_Tombstone_Tmp"] {
        let m: SqlMapping = kulyrax_core::yaml::from_str(&format!(
            "event: U\ntable: {table}\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n"
        ))
        .expect("mapping");
        let a = MySqlAdapter::new(
            "mysql".into(),
            "mysql://app@127.0.0.1:1/app",
            1,
            10,
            SqlRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
            Arc::new(UpcasterRegistry::new()),
            MigrationPolicy::default(),
        );
        let err = a.prepare().expect_err("refused").to_string();
        assert!(
            err.contains("Kulyrax keeps for itself") && err.contains(table),
            "{err}"
        );
    }
}
