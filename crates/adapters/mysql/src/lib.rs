//! MySQL SQL backend over core's `SqlTxn` seam. Registers as `type: mysql`
//! via [`factory`].
//!
//! MySQL coerces a bound string into DATE/DATETIME/numeric columns under its
//! default `sql_mode`, so unlike Postgres this backend does not introspect
//! column types: each JSON scalar binds by its own shape (`bool` → 0/1 to
//! match `TINYINT(1)`, number → int or double, string → bytes, `null` → NULL).
//!
//! Multi-writer safety matches Postgres: a `READ COMMITTED` transaction with
//! `SELECT … FOR UPDATE` on the guard row. Not `REPEATABLE READ`: there, a
//! locking read of a guard row that does not exist yet locks the *gap* where
//! it would go, and every projection in the database shares
//! `conduit_projection_state`, so two writers adding new entities — even to
//! different target tables — deadlocked on neighbouring gaps (ERROR 1213).
//! `READ COMMITTED` takes no gap locks for these reads, as on Postgres.
//! Requires row-based binary logging (`binlog_format=ROW`, the MySQL 8
//! default, or `MIXED`) when the binary log is on; `prepare` refuses
//! `STATEMENT` (see `check_binlog_format`).

use std::sync::Arc;

use mysql::prelude::Queryable;
use mysql::{IsolationLevel, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, TxOpts};
use serde::Deserialize;
use serde_json::Value;

use kulyrax_core::adapter::sql::adapter::SqlError;
use kulyrax_core::adapter::sql::exec::{
    self, GuardRow, Placeholders, SqlOutcome, SqlPlan, SqlTxn, SqlWrite,
};
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::{
    AdapterError, AdapterResult, GuardState, Prepared, SkipReason, StorageAdapter,
};
use kulyrax_core::event::Event;
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::AdapterBuildContext;
use kulyrax_core::runtime::config::{
    ConfigError, CustomAdapterConfig, MigrationPolicy, expand_env,
};
use kulyrax_core::upcast::UpcasterRegistry;

/// The `config:` block of a `type: mysql` adapter entry.
#[derive(Debug, Clone, Deserialize)]
pub struct MySqlConfig {
    /// `mysql://user:pass@host:port/db`; `${ENV_VAR}` references are expanded.
    pub url: String,
    /// Connection pool size (default 4).
    #[serde(default)]
    pub pool_size: Option<u32>,
}

/// Registry factory for `type: mysql`.
pub fn factory(
    entry: &CustomAdapterConfig,
    ctx: &AdapterBuildContext,
) -> Result<Box<dyn StorageAdapter>, ConfigError> {
    let id = entry.id.clone();
    let cfg: MySqlConfig = entry.backend_config()?;
    let url = expand_env(&cfg.url);
    if url.trim().is_empty() {
        return Err(ConfigError::FactoryConfigInvalid(format!(
            "mysql adapter {id:?}: `url` is empty"
        )));
    }
    if Opts::from_url(&url).is_err() {
        return Err(ConfigError::FactoryConfigInvalid(format!(
            "mysql adapter {id:?}: `url` does not parse: {url}"
        )));
    }
    Ok(Box::new(MySqlAdapter::new(
        id,
        &url,
        cfg.pool_size.unwrap_or(4),
        entry.priority,
        SqlRuntimeBuilder::new(ctx.mappings.sql.clone()),
        Arc::clone(&ctx.upcasters),
        ctx.migration_policy,
    )))
}

/// The pool is held as `Result<Pool, String>`: a connection error surfaces
/// on the event that needs it instead of making construction fallible.
pub struct MySqlAdapter {
    id: String,
    priority: u32,
    pool: Result<Pool, String>,
    builder: SqlRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
    /// Set by `prepare` once the guard table exists.
    prepared: Prepared,
}

impl MySqlAdapter {
    pub fn new(
        id: String,
        url: &str,
        pool_size: u32,
        priority: u32,
        builder: SqlRuntimeBuilder,
        upcasters: Arc<UpcasterRegistry>,
        migration_policy: MigrationPolicy,
    ) -> Self {
        let pool = Opts::from_url(url)
            .map_err(|e| format!("invalid mysql url: {e}"))
            .and_then(|opts| {
                let constraints = PoolConstraints::new(1, pool_size.max(1) as usize)
                    .unwrap_or(PoolConstraints::DEFAULT);
                let opts_builder = OptsBuilder::from_opts(opts)
                    .pool_opts(PoolOpts::default().with_constraints(constraints));
                Pool::new(opts_builder).map_err(|e| e.to_string())
            });
        Self {
            id,
            priority,
            pool,
            builder,
            upcasters,
            migration_policy,
            prepared: Prepared::default(),
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Sql, error)
    }
}

impl StorageAdapter for MySqlAdapter {
    fn kind(&self) -> StorageKind {
        StorageKind::Sql
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn priority(&self) -> u32 {
        self.priority
    }

    /// Create the guard table in a transaction of its own, never inside an
    /// event's: MySQL DDL commits the open transaction implicitly (see
    /// `exec::ensure_schema`).
    fn prepare(&self) -> Result<(), AdapterError> {
        let pool = self
            .pool
            .as_ref()
            .map_err(|e| AdapterError::WriteFailed(e.clone()))?;
        self.prepared
            .prepare(|| {
                let mut conn = pool.get_conn().map_err(my_err)?;
                let (log_bin, format): (i64, String) = conn
                    .query_first("SELECT @@log_bin, @@SESSION.binlog_format")
                    .map_err(my_err)?
                    .unwrap_or((0, String::new()));
                check_binlog_format(log_bin != 0, &format)?;
                drop(conn);
                let tx = pool.start_transaction(TxOpts::default()).map_err(my_err)?;
                exec::ensure_schema(MySqlTxn { tx })
            })
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))
    }

    fn handle(&self, event: &Event) -> AdapterResult {
        if let Err(e) = self.prepared.require() {
            return self.failure(AdapterError::WriteFailed(e));
        }
        let projection = match self.builder.build(event, &self.upcasters) {
            Ok(p) => p,
            Err(SqlError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return match self.migration_policy {
                    MigrationPolicy::Strict => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::Sql,
                        reason,
                        from_version,
                        to_version,
                    ),
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::Sql,
                        SkipReason::UnsupportedVersion,
                        from_version,
                        to_version,
                    ),
                };
            }
            Err(e) => return self.failure(AdapterError::WriteFailed(e.to_string())),
        };

        let source_version = projection.source_version;
        let projected_version = projection.projected_version;
        let plan = SqlPlan {
            table: projection.table,
            entity_key: projection.entity_key,
            key_values: projection.key_values,
            primary_key_columns: projection.primary_key_columns,
            on_existing: projection.on_existing,
            operation: projection.operation,
            permanent: projection.permanent,
            facet: projection.facet,
            write: projection.write,
        };

        let pool = match &self.pool {
            Ok(p) => p,
            Err(e) => return self.failure(AdapterError::WriteFailed(e.clone())),
        };

        // READ COMMITTED, not REPEATABLE READ: see the module docs.
        let tx_opts = TxOpts::default().set_isolation_level(Some(IsolationLevel::ReadCommitted));
        let tx = match pool.start_transaction(tx_opts) {
            Ok(tx) => tx,
            Err(e) => return self.failure(AdapterError::WriteFailed(e.to_string())),
        };

        match exec::project(MySqlTxn { tx }, &plan, event) {
            Ok(SqlOutcome::Created) => AdapterResult::created_versioned(
                self.id.clone(),
                StorageKind::Sql,
                source_version,
                projected_version,
            ),
            Ok(SqlOutcome::Updated) => AdapterResult::updated_versioned(
                self.id.clone(),
                StorageKind::Sql,
                source_version,
                projected_version,
            ),
            Ok(SqlOutcome::Deleted) => AdapterResult::deleted_versioned(
                self.id.clone(),
                StorageKind::Sql,
                source_version,
                projected_version,
            ),
            Ok(SqlOutcome::Skipped(reason)) => AdapterResult::skipped_versioned(
                self.id.clone(),
                StorageKind::Sql,
                reason,
                source_version,
                projected_version,
            ),
            Err(e) => self.failure(AdapterError::WriteFailed(e.to_string())),
        }
    }
}

pub struct MySqlTxn<'a> {
    tx: mysql::Transaction<'a>,
}

fn my_err(e: mysql::Error) -> SqlError {
    SqlError::ExecutionFailed(e.to_string())
}

/// Writes run in `READ COMMITTED` transactions (see the module docs), which
/// InnoDB can only log row by row: with the binary log on and
/// `binlog_format=STATEMENT`, every write would fail with ERROR 1665. Caught
/// in `prepare`, before anything is ingested. `ROW` and `MIXED` work.
fn check_binlog_format(log_bin: bool, format: &str) -> Result<(), SqlError> {
    if log_bin && format.eq_ignore_ascii_case("STATEMENT") {
        return Err(SqlError::ExecutionFailed(
            "MySQL has the binary log on with binlog_format=STATEMENT; Kulyrax writes in \
             READ COMMITTED transactions, which need binlog_format=ROW (or MIXED). \
             Set binlog_format=ROW on the server."
                .to_string(),
        ));
    }
    Ok(())
}

/// Binds by the JSON value's own shape; see the module docs for why there is
/// no column-type introspection.
fn json_to_value(v: &Value) -> mysql::Value {
    match v {
        Value::Null => mysql::Value::NULL,
        Value::Bool(b) => mysql::Value::from(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                mysql::Value::from(i)
            } else if let Some(f) = n.as_f64() {
                mysql::Value::from(f)
            } else {
                mysql::Value::from(exec::scalar_to_string(v))
            }
        }
        Value::String(s) => mysql::Value::from(s.clone()),
        Value::Array(_) | Value::Object(_) => mysql::Value::from(exec::scalar_to_string(v)),
    }
}

impl SqlTxn for MySqlTxn<'_> {
    fn ensure_guard_table(&mut self) -> Result<(), SqlError> {
        self.tx
            .query_drop(
                "CREATE TABLE IF NOT EXISTS conduit_projection_state (
                    target_table  VARCHAR(191) NOT NULL,
                    entity_key    VARCHAR(191) NOT NULL,
                    facet         VARCHAR(191) NOT NULL DEFAULT '',
                    last_sequence BIGINT       NOT NULL,
                    last_event_id VARCHAR(191) NOT NULL,
                    deleted       BOOLEAN      NOT NULL DEFAULT FALSE,
                    permanent     BOOLEAN      NOT NULL DEFAULT FALSE,
                    processed_at  DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (target_table, entity_key, facet)
                )",
            )
            .map_err(my_err)
    }

    fn read_guard_lane(
        &mut self,
        table: &str,
        entity_key: &str,
        facet: &str,
        lock: bool,
    ) -> Result<Option<GuardState>, SqlError> {
        // FOR UPDATE serializes concurrent writers on this guard row.
        let sql = if lock {
            "SELECT last_sequence, deleted, permanent FROM conduit_projection_state
             WHERE target_table = ? AND entity_key = ? AND facet = ? FOR UPDATE"
        } else {
            "SELECT last_sequence, deleted, permanent FROM conduit_projection_state
             WHERE target_table = ? AND entity_key = ? AND facet = ?"
        };
        let row: Option<(i64, bool, bool)> = self
            .tx
            .exec_first(
                sql,
                (table.to_string(), entity_key.to_string(), facet.to_string()),
            )
            .map_err(my_err)?;
        Ok(row.map(|(seq, deleted, permanent)| GuardState {
            last_sequence: seq as u64,
            deleted,
            permanent,
        }))
    }

    fn execute_write(&mut self, w: &SqlWrite) -> Result<(), SqlError> {
        let sql = w.render(Placeholders::MySql);
        let params: Vec<mysql::Value> = w.values.iter().map(json_to_value).collect();
        self.tx
            .exec_drop(sql.as_str(), mysql::Params::Positional(params))
            .map_err(my_err)
    }

    fn delete_row(&mut self, table: &str, cols: &[String], vals: &[Value]) -> Result<(), SqlError> {
        let where_clause = cols
            .iter()
            .map(|c| format!("{c} = ?"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!("DELETE FROM {table} WHERE {where_clause}");
        let params: Vec<mysql::Value> = vals.iter().map(json_to_value).collect();
        self.tx
            .exec_drop(sql.as_str(), mysql::Params::Positional(params))
            .map_err(my_err)
    }

    fn upsert_guard_row(&mut self, row: &GuardRow) -> Result<(), SqlError> {
        self.tx
            .exec_drop(
                "INSERT INTO conduit_projection_state
                    (target_table, entity_key, facet, last_sequence, last_event_id, deleted, permanent, processed_at)
                 VALUES (?, ?, ?, ?, ?, ?, ?, NOW())
                 ON DUPLICATE KEY UPDATE
                    last_sequence = VALUES(last_sequence),
                    last_event_id = VALUES(last_event_id),
                    deleted = VALUES(deleted),
                    permanent = VALUES(permanent),
                    processed_at = VALUES(processed_at)",
                (
                    row.table.clone(),
                    row.entity_key.clone(),
                    row.facet.clone(),
                    row.last_sequence,
                    row.last_event_id.clone(),
                    row.deleted,
                    row.permanent,
                ),
            )
            .map_err(my_err)
    }

    fn bump_guard_lane(
        &mut self,
        table: &str,
        entity_key: &str,
        facet: &str,
        seq: u64,
        event_id: &str,
    ) -> Result<(), SqlError> {
        self.tx
            .exec_drop(
                "UPDATE conduit_projection_state
                    SET last_sequence = ?, last_event_id = ?, processed_at = NOW()
                 WHERE target_table = ? AND entity_key = ? AND facet = ?",
                (
                    seq,
                    event_id.to_string(),
                    table.to_string(),
                    entity_key.to_string(),
                    facet.to_string(),
                ),
            )
            .map_err(my_err)
    }

    fn cascade_facets(
        &mut self,
        table: &str,
        entity_key: &str,
        deleted: bool,
        permanent: bool,
        seq: u64,
        event_id: &str,
    ) -> Result<(), SqlError> {
        if deleted {
            self.tx.exec_drop(
                "UPDATE conduit_projection_state
                    SET deleted = TRUE, permanent = ?, last_sequence = ?,
                        last_event_id = ?, processed_at = NOW()
                 WHERE target_table = ? AND entity_key = ? AND facet <> ''",
                (
                    permanent,
                    seq,
                    event_id.to_string(),
                    table.to_string(),
                    entity_key.to_string(),
                ),
            )
        } else {
            self.tx.exec_drop(
                "UPDATE conduit_projection_state
                    SET deleted = FALSE, permanent = FALSE
                 WHERE target_table = ? AND entity_key = ? AND facet <> ''",
                (table.to_string(), entity_key.to_string()),
            )
        }
        .map_err(my_err)
    }

    fn commit(self) -> Result<(), SqlError> {
        self.tx.commit().map_err(my_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_binlog_is_rejected_only_when_the_binlog_is_on() {
        let err = check_binlog_format(true, "STATEMENT").unwrap_err();
        assert!(err.to_string().contains("binlog_format=ROW"), "{err}");
        assert!(check_binlog_format(true, "statement").is_err());
        assert!(check_binlog_format(true, "ROW").is_ok());
        assert!(check_binlog_format(true, "MIXED").is_ok());
        assert!(check_binlog_format(false, "STATEMENT").is_ok());
    }

    fn build(url: &str) -> Result<Box<dyn StorageAdapter>, ConfigError> {
        let raw = serde_yaml::to_value(serde_json::json!({
            "type": "mysql",
            "id": "my1",
            "priority": 10,
            "config": { "url": url },
        }))
        .map_err(|e| ConfigError::FactoryConfigInvalid(e.to_string()))?;
        let entry = CustomAdapterConfig {
            type_name: "mysql".into(),
            id: "my1".into(),
            priority: 10,
            raw,
            capabilities: None,
            depends_on: Vec::new(),
        };
        let ctx = AdapterBuildContext {
            mappings: Default::default(),
            upcasters: Arc::new(UpcasterRegistry::new()),
            migration_policy: MigrationPolicy::default(),
        };
        factory(&entry, &ctx)
    }

    #[test]
    fn factory_rejects_an_empty_url() {
        let err = build("").err().map(|e| e.to_string()).unwrap_or_default();
        assert!(err.contains("`url` is empty"), "{err}");
    }

    #[test]
    fn factory_rejects_an_unparseable_url() {
        let err = build("not a url")
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(err.contains("`url` does not parse"), "{err}");
    }
}
