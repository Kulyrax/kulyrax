//! `prepare` refuses a server whose binary log is on with
//! `binlog_format=STATEMENT`: every write runs in a `READ COMMITTED`
//! transaction, which MySQL can only log row by row (ERROR 1665).
//!
//! **Gated on `KULYRAX_MYSQL_URL`**, like `mysql_backend.rs`. Its own test
//! file, so its own process: it changes the server's *global*
//! `binlog_format`, which would break other tests' writes if they ran
//! alongside it. The format is restored when the test ends, even on failure.

use kulyrax_adapter_mysql::MySqlAdapter;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use mysql::prelude::Queryable;
use std::collections::HashMap;
use std::sync::Arc;

type R = Result<(), Box<dyn std::error::Error>>;

/// Sets the global `binlog_format` and puts the old one back on drop.
struct GlobalBinlogFormat {
    pool: mysql::Pool,
    old: String,
}

impl GlobalBinlogFormat {
    fn set(url: &str, format: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let pool = mysql::Pool::new(url)?;
        let mut conn = pool.get_conn()?;
        let old: String = conn
            .query_first("SELECT @@GLOBAL.binlog_format")?
            .unwrap_or_else(|| "ROW".to_string());
        conn.query_drop(format!("SET GLOBAL binlog_format = '{format}'"))?;
        Ok(Self { pool, old })
    }
}

impl Drop for GlobalBinlogFormat {
    fn drop(&mut self) {
        if let Ok(mut conn) = self.pool.get_conn() {
            let _ = conn.query_drop(format!("SET GLOBAL binlog_format = '{}'", self.old));
        }
    }
}

fn adapter(url: &str) -> MySqlAdapter {
    MySqlAdapter::new(
        "mysql".into(),
        url,
        2,
        10,
        SqlRuntimeBuilder::new(HashMap::new()),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

#[test]
fn prepare_rejects_statement_binlog_format() -> R {
    let Some(url) = std::env::var("KULYRAX_MYSQL_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
    else {
        eprintln!("SKIP: KULYRAX_MYSQL_URL not set");
        return Ok(());
    };
    let log_bin: i64 = mysql::Pool::new(url.as_str())?
        .get_conn()?
        .query_first("SELECT @@log_bin")?
        .unwrap_or(0);
    if log_bin == 0 {
        eprintln!("SKIP: the server's binary log is off; binlog_format does not apply");
        return Ok(());
    }

    {
        // New connections, such as the adapter's pool, take the global value.
        let _statement = GlobalBinlogFormat::set(&url, "STATEMENT")?;
        let err = adapter(&url)
            .prepare()
            .expect_err("prepare must refuse binlog_format=STATEMENT");
        assert!(err.to_string().contains("binlog_format=ROW"), "{err}");
    }
    adapter(&url)
        .prepare()
        .map_err(|e| format!("prepare with the format restored: {e}"))?;
    Ok(())
}
