//! Step 5's format marker on Postgres (decision 2026-11-28-format-versions):
//! `kulyrax_format` sits beside the guard table, in the schema the
//! connection's `search_path` selects (never `public` here); it is created
//! insert-if-absent, so concurrent first starts leave one row; a newer one is
//! refused before the guard table is touched; a user who may not create it is
//! refused with the grant it needs.
//!
//! **Gated on `KULYRAX_PG_URL`.** Each test runs in a schema of its own.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use kulyrax_adapter_postgres::PostgresAdapter;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::{AdapterError, StorageAdapter};
use kulyrax_core::format::FormatProblem;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

type R = Result<(), Box<dyn std::error::Error>>;

static SEQ: AtomicU32 = AtomicU32::new(0);

/// A fresh, non-default schema holding a `users` table, and its name.
fn schema(base: &str) -> Result<String, Box<dyn std::error::Error>> {
    let name = format!(
        "kx_format_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    );
    let mut c = postgres::Client::connect(base, postgres::NoTls)?;
    c.batch_execute(&format!(
        "DROP SCHEMA IF EXISTS {name} CASCADE; CREATE SCHEMA {name};
         CREATE TABLE {name}.users (id text PRIMARY KEY, v bigint)"
    ))?;
    Ok(name)
}

/// A URL whose connections use `schema`, and act as `role` when given.
fn url(base: &str, schema: &str, role: Option<&str>) -> String {
    let sep = if base.contains('?') { '&' } else { '?' };
    let role = role.map_or(String::new(), |r| format!("%20-c%20role%3D{r}"));
    format!("{base}{sep}options=-c%20search_path%3D{schema}{role}")
}

fn exec(url: &str, sql: &str) -> R {
    postgres::Client::connect(url, postgres::NoTls)?.batch_execute(sql)?;
    Ok(())
}

fn adapter(url: &str) -> PostgresAdapter {
    let m: SqlMapping = kulyrax_core::yaml::from_str(
        "event: U\ntable: users\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n  v: payload.v\n",
    )
    .expect("mapping");
    PostgresAdapter::new(
        "pg".into(),
        url,
        2,
        10,
        SqlRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

/// The schema each named table is in, if it exists anywhere.
fn schemas_of(base: &str, table: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let mut c = postgres::Client::connect(base, postgres::NoTls)?;
    Ok(c.query(
        "SELECT n.nspname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE c.relname = $1 AND c.relkind = 'r' ORDER BY 1",
        &[&table],
    )?
    .iter()
    .map(|r| r.get(0))
    .collect())
}

fn markers(base: &str, schema: &str) -> Result<Vec<(String, i32)>, Box<dyn std::error::Error>> {
    let mut c = postgres::Client::connect(base, postgres::NoTls)?;
    Ok(c.query(
        &format!("SELECT component, version FROM {schema}.kulyrax_format ORDER BY 1"),
        &[],
    )?
    .iter()
    .map(|r| (r.get(0), r.get(1)))
    .collect())
}

/// Change 2: the marker is in the guard table's schema, a non-default one.
#[test]
fn the_marker_sits_beside_the_guard_table() -> R {
    let Ok(base) = std::env::var("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return Ok(());
    };
    let s = schema(&base)?;
    adapter(&url(&base, &s, None)).prepare()?;
    adapter(&url(&base, &s, None)).prepare()?;
    assert!(schemas_of(&base, "kulyrax_format")?.contains(&s));
    assert!(schemas_of(&base, "kulyrax_projection_state")?.contains(&s));
    assert_eq!(markers(&base, &s)?, [("guards".to_string(), 1)]);
    exec(&base, &format!("DROP SCHEMA {s} CASCADE"))
}

/// A `kulyrax_format` later on the `search_path` (here `public`) is another
/// store's: this schema still gets its own.
#[test]
fn a_marker_elsewhere_on_the_search_path_is_not_this_stores() -> R {
    let Ok(base) = std::env::var("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return Ok(());
    };
    let s = schema(&base)?;
    let other = schema(&base)?;
    exec(
        &base,
        &format!(
            "CREATE TABLE {other}.kulyrax_format (component text PRIMARY KEY, version integer NOT NULL);
             INSERT INTO {other}.kulyrax_format VALUES ('guards', 9)"
        ),
    )?;
    let sep = if base.contains('?') { '&' } else { '?' };
    let both = format!("{base}{sep}options=-c%20search_path%3D{s},{other}");
    adapter(&both).prepare()?;
    assert_eq!(markers(&base, &s)?, [("guards".to_string(), 1)]);
    assert_eq!(markers(&base, &other)?, [("guards".to_string(), 9)]);
    exec(
        &base,
        &format!("DROP SCHEMA {s} CASCADE; DROP SCHEMA {other} CASCADE"),
    )
}

/// A schema with guard rows and no marker (0.12.x): format 1, stamped.
#[test]
fn a_0_12_schema_is_stamped_as_format_1() -> R {
    let Ok(base) = std::env::var("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return Ok(());
    };
    let s = schema(&base)?;
    let u = url(&base, &s, None);
    adapter(&u).prepare()?;
    exec(
        &u,
        "DROP TABLE kulyrax_format;
         INSERT INTO kulyrax_projection_state (target_table, entity_key, last_sequence, last_event_id)
         VALUES ('users', '[\"u1\"]', 7, 'e7')",
    )?;
    adapter(&u).prepare()?;
    assert_eq!(markers(&base, &s)?, [("guards".to_string(), 1)]);
    let mut c = postgres::Client::connect(&u, postgres::NoTls)?;
    let seq: i64 = c
        .query_one("SELECT last_sequence FROM kulyrax_projection_state", &[])?
        .get(0);
    assert_eq!(seq, 7);
    exec(&base, &format!("DROP SCHEMA {s} CASCADE"))
}

#[test]
fn a_newer_schema_is_refused_before_the_guard_table() -> R {
    let Ok(base) = std::env::var("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return Ok(());
    };
    let s = schema(&base)?;
    exec(
        &base,
        &format!(
            "CREATE TABLE {s}.kulyrax_format (component text PRIMARY KEY, version integer NOT NULL);
             INSERT INTO {s}.kulyrax_format VALUES ('guards', 2)"
        ),
    )?;
    let err = adapter(&url(&base, &s, None))
        .prepare()
        .expect_err("refused");
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
    assert!(e.store.contains(&s), "{}", e.store);
    assert!(!schemas_of(&base, "kulyrax_projection_state")?.contains(&s));
    exec(&base, &format!("DROP SCHEMA {s} CASCADE"))
}

/// Change 3: concurrent first starts leave one marker row, one version.
#[test]
fn concurrent_first_starts_leave_one_marker() -> R {
    let Ok(base) = std::env::var("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return Ok(());
    };
    let s = schema(&base)?;
    let u = url(&base, &s, None);
    let results: Vec<Result<(), String>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..8)
            .map(|_| scope.spawn(|| adapter(&u).prepare().map_err(|e| e.to_string())))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err("panicked".into())))
            .collect()
    });
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(markers(&base, &s)?, [("guards".to_string(), 1)]);
    exec(&base, &format!("DROP SCHEMA {s} CASCADE"))
}

/// Change 4: a user who may not create the marker is refused, naming the
/// schema and the grant; with that grant the same user starts. (Postgres
/// checks CREATE even for `CREATE TABLE IF NOT EXISTS` on an existing table,
/// so the guard table already needed it at every start: probed by hand, the
/// app role passes the advisory lock and the marker insert, and fails only
/// there.)
#[test]
fn a_user_without_create_names_the_grant() -> R {
    let Ok(base) = std::env::var("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return Ok(());
    };
    let s = schema(&base)?;
    let role = format!("{s}_app");
    // The administrator sets the schema up; the app role may use its tables
    // but not create in it.
    adapter(&url(&base, &s, None)).prepare()?;
    exec(
        &base,
        &format!(
            "DROP TABLE {s}.kulyrax_format;
             DROP ROLE IF EXISTS {role}; CREATE ROLE {role} NOLOGIN;
             GRANT USAGE ON SCHEMA {s} TO {role};
             GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA {s} TO {role}"
        ),
    )?;
    let app = url(&base, &s, Some(&role));
    let err = adapter(&app).prepare().expect_err("refused");
    let AdapterError::Format(e) = &err else {
        panic!("not a format refusal: {err}");
    };
    let FormatProblem::MarkerNotCreated { grant, .. } = &e.problem else {
        panic!("not MarkerNotCreated: {e}");
    };
    assert_eq!(grant, &format!("CREATE on schema {s}"));
    assert!(e.store.contains(&s), "{}", e.store);

    // The grant the message names is enough.
    exec(&base, &format!("GRANT CREATE ON SCHEMA {s} TO {role}"))?;
    adapter(&app).prepare()?;
    assert_eq!(markers(&base, &s)?, [("guards".to_string(), 1)]);
    exec(
        &base,
        &format!("DROP SCHEMA {s} CASCADE; DROP OWNED BY {role}; DROP ROLE {role}"),
    )
}
