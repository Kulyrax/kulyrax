//! Phase 25.3: the MySQL SQL backend — a fifth, first-party SQL adapter
//! alongside SQLite/Postgres, over the same `SqlTxn` seam (Phase 19.1). Per
//! an explicit product decision this ships inside `kulyrax-core` exactly
//! like every other backend, not as an out-of-tree example crate — this file
//! doubles as the worked example for [`super::exec::SqlTxn`]'s "public
//! extension surface" doc comment (Phase 25.1).
//!
//! Unlike Postgres, MySQL's wire protocol is comparatively permissive about
//! parameter/column type mismatches — a bound string coerces into a
//! DATE/DATETIME/numeric column under the server's default `sql_mode`. So
//! this backend skips Postgres's `information_schema` column-type
//! introspection and binds every JSON scalar directly by its own shape
//! (`bool` → `Value::Int(0/1)`, matching MySQL's `TINYINT(1)` `BOOLEAN`
//! convention; a JSON number → `Value::Int`/`Value::Double`; a JSON string →
//! `Value::Bytes`; `null` → `Value::NULL`) — the same "trust the server's own
//! coercion" stance SQLite's backend takes, not Postgres's typed one.
//!
//! Multi-writer safety mirrors Postgres: a real `REPEATABLE READ` (MySQL's
//! default) transaction with `SELECT … FOR UPDATE` on the guard row when
//! `lock` is set (InnoDB supports the same pessimistic row lock Postgres
//! does) — no retry loop needed.

use std::sync::Arc;

use mysql::prelude::Queryable;
use mysql::{IsolationLevel, Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, TxOpts};
use serde_json::Value;

use super::adapter::SqlError;
use super::exec::{self, GuardRow, Placeholders, SqlOutcome, SqlPlan, SqlTxn, SqlWrite};
use super::runtime::SqlRuntimeBuilder;
use crate::adapter::{AdapterError, AdapterResult, GuardState, SkipReason, StorageAdapter};
use crate::event::Event;
use crate::routing::StorageKind;
use crate::runtime::config::MigrationPolicy;
use crate::upcast::UpcasterRegistry;

/// MySQL SQL adapter (Phase 25.3). Structurally identical to
/// [`super::postgres::PostgresAdapter`]: a lazily-erroring connection pool
/// held as `Result<Pool, String>` so a bad URL surfaces per-event rather than
/// making the adapter factory fallible.
pub struct MySqlAdapter {
    id: String,
    priority: u32,
    pool: Result<Pool, String>,
    builder: SqlRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
}

impl MySqlAdapter {
    /// Build the pool from a connection URL
    /// (`mysql://user:pass@host:3306/db`). Infallible — a bad URL or pool
    /// size is stored and surfaced per-event, keeping
    /// `build_adapters_from_config` free of a `Result`.
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

    fn handle(&self, event: &Event) -> AdapterResult {
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

        let tx_opts = TxOpts::default().set_isolation_level(Some(IsolationLevel::RepeatableRead));
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

// ---------------------------------------------------------------------------
// MySqlTxn
// ---------------------------------------------------------------------------

pub struct MySqlTxn<'a> {
    tx: mysql::Transaction<'a>,
}

fn my_err(e: mysql::Error) -> SqlError {
    SqlError::ExecutionFailed(e.to_string())
}

/// Convert a JSON scalar to a bindable `mysql::Value` (see the module doc
/// comment for why this skips Postgres-style column-type introspection).
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
        // InnoDB's FOR UPDATE serializes concurrent writers on this row, the
        // same real pessimistic lock Postgres uses (Phase 19.4's pattern).
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
