//! A fresh MySQL database for each test binary, each run.
//!
//! The tests share one server (`KULYRAX_MYSQL_URL`). Table names restart at
//! `t_0` every run, so in a long-lived database a guard row left by an earlier
//! run would make an event `Skipped(StaleSequence)`. Instead, each test binary
//! drops and recreates a database of its own, named after the binary
//! (`kx_run_mysql_backend`, …), once per run, checks that it is empty, and
//! every test connects to it. The harness has no end-of-binary hook, so the
//! database is dropped at the start of the next run.
//! Nothing piles up across runs. Two concurrent runs of the same binary
//! against one server would share it; CI runs each binary once.

use std::sync::OnceLock;

use mysql::prelude::Queryable;

/// The URL of this binary's fresh database, or `None` (skip) when
/// `KULYRAX_MYSQL_URL` is unset.
pub fn mysql_url() -> Option<String> {
    static URL: OnceLock<Option<String>> = OnceLock::new();
    URL.get_or_init(|| {
        let base = std::env::var("KULYRAX_MYSQL_URL")
            .ok()
            .filter(|s| !s.trim().is_empty())?;
        let db = format!("kx_run_{}", env!("CARGO_CRATE_NAME"));
        let mut conn = mysql::Pool::new(base.as_str())
            .and_then(|p| p.get_conn())
            .unwrap_or_else(|e| panic!("connect to KULYRAX_MYSQL_URL: {e}"));
        conn.query_drop(format!("DROP DATABASE IF EXISTS {db}"))
            .and_then(|()| conn.query_drop(format!("CREATE DATABASE {db}")))
            .unwrap_or_else(|e| panic!("create the test database {db}: {e}"));
        // Fail loudly on leftovers: the fresh database holds nothing.
        let tables: Option<u64> = conn
            .exec_first(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = ?",
                (&db,),
            )
            .unwrap_or_else(|e| panic!("inspect the test database {db}: {e}"));
        assert_eq!(tables, Some(0), "the fresh test database {db} is not empty");
        let (server, _) = base
            .split_once('?')
            .map_or(base.as_str(), |(u, _)| u)
            .rsplit_once('/')
            .unwrap_or_else(|| panic!("KULYRAX_MYSQL_URL names no database: {base}"));
        let query = base
            .split_once('?')
            .map(|(_, q)| format!("?{q}"))
            .unwrap_or_default();
        Some(format!("{server}/{db}{query}"))
    })
    .clone()
}
