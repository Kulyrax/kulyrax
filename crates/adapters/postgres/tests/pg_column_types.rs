//! G0 (the owner's deferred item, found in B2): the Postgres adapter looks up a
//! mapped table's column types with
//! `SELECT … FROM information_schema.columns WHERE table_name = $1`: by table
//! name only, in every schema the user can see. With the same table name in
//! two schemas, the types of the wrong table can be used.
//!
//! These tests reproduce it. They are expected to FAIL until it is fixed.
//!
//! **Gated on `KULYRAX_PG_URL`.** Each test makes schemas of its own.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use kulyrax_adapter_postgres::PostgresAdapter;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

type R = Result<(), Box<dyn std::error::Error>>;

static SEQ: AtomicU32 = AtomicU32::new(0);

/// The value the event sends. Stored in a `text` column, it must come back
/// exactly as sent.
const AT: &str = "2026-11-28T10:00:00+05:00";

fn name(prefix: &str) -> String {
    format!(
        "kx_coltypes_{prefix}_{}_{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

fn exec(url: &str, sql: &str) -> R {
    postgres::Client::connect(url, postgres::NoTls)?.batch_execute(sql)?;
    Ok(())
}

/// The mapped schema (on the `search_path`) has `seen(id text, at text)`;
/// another schema has a table of the same name with `at timestamptz`.
/// `other_first` creates the other schema's table first.
fn schemas(base: &str, other_first: bool) -> Result<(String, String), Box<dyn std::error::Error>> {
    let mapped = name("mapped");
    let other = name("other");
    let mapped_sql = format!(
        "CREATE SCHEMA {mapped}; CREATE TABLE {mapped}.seen (id text PRIMARY KEY, at text);"
    );
    let other_sql = format!(
        "CREATE SCHEMA {other}; CREATE TABLE {other}.seen (id text PRIMARY KEY, at timestamptz);"
    );
    if other_first {
        exec(base, &other_sql)?;
        exec(base, &mapped_sql)?;
    } else {
        exec(base, &mapped_sql)?;
        exec(base, &other_sql)?;
    }
    Ok((mapped, other))
}

fn url(base: &str, schema: &str) -> String {
    let sep = if base.contains('?') { '&' } else { '?' };
    format!("{base}{sep}options=-c%20search_path%3D{schema}")
}

fn adapter(url: &str) -> PostgresAdapter {
    let m: SqlMapping = kulyrax_core::yaml::from_str(
        "event: Seen\ntable: seen\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n  at: payload.at\n",
    )
    .expect("mapping");
    PostgresAdapter::new(
        "pg".into(),
        url,
        2,
        10,
        SqlRuntimeBuilder::new(HashMap::from([("Seen".to_string(), m)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn event(id: &str) -> Event {
    Event::new(
        format!("e-{id}"),
        "Seen",
        1,
        serde_json::json!({ "id": id, "at": AT }).to_string(),
    )
}

fn stored(url: &str, id: &str) -> Result<Option<String>, Box<dyn std::error::Error>> {
    let mut c = postgres::Client::connect(url, postgres::NoTls)?;
    Ok(c.query_opt("SELECT at FROM seen WHERE id = $1", &[&id])?
        .map(|r| r.get::<_, String>(0)))
}

/// What each path did, for one creation order: (batched, one event at a time).
fn run(base: &str, other_first: bool) -> Result<(String, String), Box<dyn std::error::Error>> {
    let (mapped, other) = schemas(base, other_first)?;
    let u = url(base, &mapped);

    // The batched path: the run loop's default (`begin_batch` … `commit_batch`).
    let a = adapter(&u);
    a.prepare()?;
    let ev = event("batched");
    a.begin_batch(&[&ev])?;
    let outcome = a.handle(&ev).outcome;
    let committed = a.commit_batch();
    let batched = match (&committed, stored(&u, "batched")?) {
        (Ok(()), Some(at)) => format!("stored {at:?}"),
        (Ok(()), None) => format!("nothing stored ({outcome:?})"),
        (Err(e), _) => format!("refused: {e}"),
    };
    drop(a);

    // One event at a time (no batch), on a fresh adapter (a fresh type cache).
    let a = adapter(&u);
    a.prepare()?;
    let outcome = a.handle(&event("single")).outcome;
    let single = match stored(&u, "single")? {
        Some(at) => format!("stored {at:?}"),
        None => format!("not stored: {outcome:?}"),
    };

    exec(
        base,
        &format!("DROP SCHEMA {mapped} CASCADE; DROP SCHEMA {other} CASCADE"),
    )?;
    Ok((batched, single))
}

/// The value sent is stored exactly, whichever schema's table was created
/// first, in the batched path and one event at a time.
#[test]
fn a_same_named_table_in_another_schema_does_not_change_what_is_stored() -> R {
    let Ok(base) = std::env::var("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return Ok(());
    };
    let want = format!("stored {AT:?}");
    let mut wrong = Vec::new();
    for other_first in [false, true] {
        let (batched, single) = run(&base, other_first)?;
        eprintln!("other schema first: {other_first}: batched: {batched}; one at a time: {single}");
        if batched != want {
            wrong.push(format!("other first {other_first}, batched: {batched}"));
        }
        if single != want {
            wrong.push(format!(
                "other first {other_first}, one at a time: {single}"
            ));
        }
    }
    assert!(
        wrong.is_empty(),
        "expected {want} every time; got:\n{}",
        wrong.join("\n")
    );
    Ok(())
}

/// A mapped table named with its schema (`table: <schema>.seen`), the
/// connection's `search_path` elsewhere: the lookup by `table_name` finds no
/// columns at all.
#[test]
fn a_schema_qualified_table_finds_its_columns() -> R {
    let Ok(base) = std::env::var("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return Ok(());
    };
    let (mapped, other) = schemas(&base, false)?;
    let m: SqlMapping = kulyrax_core::yaml::from_str(&format!(
        "event: Seen\ntable: {mapped}.seen\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n  at: payload.at\n"
    ))
    .expect("mapping");
    // The search_path names the *other* schema: only the qualified name can
    // reach the mapped table.
    let a = PostgresAdapter::new(
        "pg".into(),
        &url(&base, &other),
        2,
        10,
        SqlRuntimeBuilder::new(HashMap::from([("Seen".to_string(), m)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );
    a.prepare()?;
    let outcome = a.handle(&event("qualified")).outcome;
    let got = stored(&url(&base, &mapped), "qualified")?;
    exec(
        &base,
        &format!("DROP SCHEMA {mapped} CASCADE; DROP SCHEMA {other} CASCADE"),
    )?;
    assert_eq!(got.as_deref(), Some(AT), "outcome: {outcome:?}");
    Ok(())
}
