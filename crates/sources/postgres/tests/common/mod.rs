//! A fresh Postgres schema for each test binary, each run.
//!
//! The tests share one server (`KULYRAX_PG_URL`), and Kulyrax keeps one guard
//! table per schema. Each test binary drops and recreates a schema of its own,
//! named after the binary (`kx_run_outbox`, …), once per run, and checks
//! that it is empty. Every test connects with `search_path` set to it, so
//! every table it creates, the guard table included, lands there and never
//! in the shared database's own schema. Rust's test harness has no
//! end-of-binary hook, so a schema is dropped at the start of the next run,
//! not at the end of this one. Two concurrent runs of the same binary against
//! one server would share it; CI runs each binary once.

use std::sync::OnceLock;

/// The URL of this binary's fresh schema, or `None` (skip) when
/// `KULYRAX_PG_URL` is unset.
pub fn isolated_pg_url() -> Option<String> {
    static URL: OnceLock<Option<String>> = OnceLock::new();
    URL.get_or_init(|| {
        let base = std::env::var("KULYRAX_PG_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())?;
        let schema = format!("kx_run_{}", env!("CARGO_CRATE_NAME"));
        let mut c = postgres::Client::connect(&base, postgres::NoTls)
            .unwrap_or_else(|e| panic!("connect to KULYRAX_PG_URL: {e}"));
        c.batch_execute(&format!(
            "DROP SCHEMA IF EXISTS {schema} CASCADE; CREATE SCHEMA {schema}"
        ))
        .unwrap_or_else(|e| panic!("create the test schema {schema}: {e}"));
        let objects: i64 = c
            .query_one(
                "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
                 WHERE n.nspname = $1",
                &[&schema],
            )
            .unwrap_or_else(|e| panic!("inspect the test schema {schema}: {e}"))
            .get(0);
        assert_eq!(
            objects, 0,
            "the fresh test schema {schema} already holds {objects} objects"
        );
        let sep = if base.contains('?') { '&' } else { '?' };
        Some(format!("{base}{sep}options=-c%20search_path%3D{schema}"))
    })
    .clone()
}
