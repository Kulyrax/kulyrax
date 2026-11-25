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
//! `kulyrax_projection_state`, so two writers adding new entities — even to
//! different target tables — deadlocked on neighbouring gaps (ERROR 1213).
//! `READ COMMITTED` takes no gap locks for these reads, as on Postgres.
//! Requires row-based binary logging (`binlog_format=ROW`, the MySQL 8
//! default, or `MIXED`) when the binary log is on; `prepare` refuses
//! `STATEMENT` (see `check_binlog_format`).
//!
//! A batch (`begin_batch` … `commit_batch`) holds one pooled connection with
//! one transaction open on it, and loads every guard lane it will read up
//! front, locked, in one query per table. The guard table's key columns are
//! binary-collated (see `ensure_binary_keys`), so a lane matches its entity
//! key exactly, as on SQLite and Postgres.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use mysql::prelude::Queryable;
use mysql::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, PooledConn};

mod legacy;
use serde::Deserialize;
use serde_json::Value;

use kulyrax_core::adapter::sql::adapter::SqlError;
use kulyrax_core::adapter::sql::exec::{
    self, GuardCache, GuardRow, KnownGuards, Placeholders, SqlTxn, SqlWrite,
};
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::{AdapterError, AdapterResult, GuardState, Prepared, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::execution::stages::{Stage, timed};
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::AdapterBuildContext;
use kulyrax_core::runtime::config::{
    ConfigError, CustomAdapterConfig, MigrationPolicy, expand_env,
};
use kulyrax_core::upcast::UpcasterRegistry;

/// The `config:` block of a `type: mysql` adapter entry.
#[derive(Clone, Deserialize)]
pub struct MySqlConfig {
    /// `mysql://user:pass@host:port/db`; `${ENV_VAR}` references are expanded.
    pub url: String,
    /// Connection pool size (default 4).
    #[serde(default)]
    pub pool_size: Option<u32>,
}

/// Shows the URL with its password and secret query parameters hidden.
impl std::fmt::Debug for MySqlConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MySqlConfig")
            .field("url", &kulyrax_core::redact::redact_url(&self.url))
            .field("pool_size", &self.pool_size)
            .finish()
    }
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
            "mysql adapter {id:?}: `url` does not parse: {}",
            kulyrax_core::redact::redact_url(&url)
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
    /// The open batch, from `begin_batch` to `commit_batch` / `rollback_batch`.
    batch: Mutex<Option<MyBatch>>,
    /// Entities that can have guard lanes (see [`KnownGuards`]). Locked
    /// after `batch`.
    known: Mutex<KnownGuards>,
    /// Set by `prepare` once the guard table exists.
    prepared: Prepared,
}

/// One batch's held connection, with its transaction open.
struct MyBatch {
    conn: PooledConn,
    cache: GuardCache,
}

fn poisoned() -> AdapterError {
    AdapterError::WriteFailed("MySQL batch lock poisoned by an earlier panic".to_string())
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
            batch: Mutex::new(None),
            known: Mutex::default(),
            prepared: Prepared::default(),
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Sql, error)
    }

    fn pool(&self) -> Result<&Pool, AdapterError> {
        self.pool
            .as_ref()
            .map_err(|e| AdapterError::WriteFailed(e.clone()))
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
        let pool = self.pool()?;
        self.prepared
            .prepare(|| {
                exec::check_identifiers(&self.builder)?;
                let mut conn = pool.get_conn().map_err(my_err)?;
                let (log_bin, format): (i64, String) = conn
                    .query_first("SELECT @@log_bin, @@SESSION.binlog_format")
                    .map_err(my_err)?
                    .unwrap_or((0, String::new()));
                check_binlog_format(log_bin != 0, &format)?;
                exec::ensure_schema(MySqlTxn::begin(&mut conn)?)?;
                ensure_binary_keys(&mut conn)
            })
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))
    }

    /// Check out one connection for the whole batch, start its transaction,
    /// and lock and load every guard lane the batch will read.
    fn begin_batch(&self, events: &[&Event]) -> Result<(), AdapterError> {
        self.prepared.require().map_err(AdapterError::WriteFailed)?;
        let mut batch = self.batch.lock().map_err(|_| poisoned())?;
        if batch.is_some() {
            return Err(AdapterError::WriteFailed(
                "a batch is already open on this adapter".to_string(),
            ));
        }
        let mut conn = self
            .pool()?
            .get_conn()
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))?;
        // Events that fail to plan are left to `handle`, which reports them.
        let plans: Vec<_> = events
            .iter()
            .filter_map(|e| {
                exec::plan_event(
                    &self.id,
                    &self.builder,
                    &self.upcasters,
                    self.migration_policy,
                    e,
                )
                .ok()
            })
            .collect();
        let mut cache = GuardCache::default();
        {
            let mut txn =
                MySqlTxn::begin(&mut conn).map_err(|e| AdapterError::WriteFailed(e.to_string()))?;
            let mut known = self.known.lock().map_err(|_| poisoned())?;
            exec::prefetch_guards(
                &mut txn,
                plans.iter().map(|p| &p.plan),
                &mut cache,
                &mut known,
            )
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))?;
            // From here the batch owns the transaction; `txn` must not roll it back.
            txn.release();
        }
        *batch = Some(MyBatch { conn, cache });
        Ok(())
    }

    fn commit_batch(&self) -> Result<(), AdapterError> {
        let mut batch = self.batch.lock().map_err(|_| poisoned())?;
        let Some(mut b) = batch.take() else {
            return Ok(());
        };
        timed(Stage::Commit, || b.conn.query_drop("COMMIT")).map_err(|e| {
            // A failed COMMIT can leave the transaction open; end it.
            let _ = b.conn.query_drop("ROLLBACK");
            AdapterError::WriteFailed(e.to_string())
        })
    }

    fn rollback_batch(&self) {
        let Ok(mut batch) = self.batch.lock() else {
            return;
        };
        if let Some(mut b) = batch.take() {
            let _ = b.conn.query_drop("ROLLBACK");
        }
    }

    fn handle(&self, event: &Event) -> AdapterResult {
        if let Err(e) = self.prepared.require() {
            return self.failure(AdapterError::WriteFailed(e));
        }
        let planned = match exec::plan_event(
            &self.id,
            &self.builder,
            &self.upcasters,
            self.migration_policy,
            event,
        ) {
            Ok(p) => p,
            Err(result) => return result,
        };
        // Before any write: this entity may have guard lanes from now on.
        match self.known.lock() {
            Ok(mut known) => known.note(&planned.plan.table, &planned.plan.entity_key),
            Err(_) => return self.failure(poisoned()),
        }

        let mut batch = match self.batch.lock() {
            Ok(b) => b,
            Err(_) => return self.failure(poisoned()),
        };
        let outcome = match batch.as_mut() {
            Some(b) => exec::project_in(
                &mut MySqlTxn::joined(&mut b.conn),
                &planned.plan,
                event,
                &mut b.cache,
            ),
            None => {
                let mut conn = match self.pool().and_then(|p| {
                    p.get_conn()
                        .map_err(|e| AdapterError::WriteFailed(e.to_string()))
                }) {
                    Ok(c) => c,
                    Err(e) => return self.failure(e),
                };
                MySqlTxn::begin(&mut conn).and_then(|txn| exec::project(txn, &planned.plan, event))
            }
        };
        exec::outcome_result(&self.id, &planned, outcome)
    }
}

/// A transaction on one connection: its own (`begin`, committed by
/// `commit`, rolled back if dropped uncommitted), or a share of an open batch
/// transaction (`joined`: `commit` and drop leave it alone).
pub struct MySqlTxn<'a> {
    conn: &'a mut PooledConn,
    /// `true` while this value must still end the transaction itself.
    owns: bool,
}

impl<'a> MySqlTxn<'a> {
    /// `READ COMMITTED`, not `REPEATABLE READ`: see the module docs.
    fn begin(conn: &'a mut PooledConn) -> Result<Self, SqlError> {
        conn.query_drop("SET TRANSACTION ISOLATION LEVEL READ COMMITTED")
            .map_err(my_err)?;
        conn.query_drop("START TRANSACTION").map_err(my_err)?;
        Ok(Self { conn, owns: true })
    }

    fn joined(conn: &'a mut PooledConn) -> Self {
        Self { conn, owns: false }
    }

    /// Hand the open transaction over to the caller.
    fn release(&mut self) {
        self.owns = false;
    }
}

impl Drop for MySqlTxn<'_> {
    fn drop(&mut self) {
        if self.owns {
            let _ = self.conn.query_drop("ROLLBACK");
        }
    }
}

fn my_err(e: mysql::Error) -> SqlError {
    SqlError::ExecutionFailed(e.to_string())
}

/// Columns of the guard table's primary key.
const GUARD_KEY_COLUMNS: [&str; 3] = ["target_table", "entity_key", "facet"];

/// Make the guard table's key columns binary-collated, if an older version
/// created them with the server's default (case- and accent-insensitive)
/// collation. There, `A` and `a` share one guard lane, while a batch's guard
/// cache keys lanes by exact string, so a batch could decide differently from
/// one event at a time. Binary keys match SQLite and Postgres. The change
/// cannot fail on existing rows: keys unique without case stay unique with it.
fn ensure_binary_keys(conn: &mut PooledConn) -> Result<(), SqlError> {
    let collations: Vec<(String, Option<String>)> = conn
        .query(
            "SELECT COLUMN_NAME, COLLATION_NAME FROM information_schema.COLUMNS
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'kulyrax_projection_state'",
        )
        .map_err(my_err)?;
    let binary = GUARD_KEY_COLUMNS.iter().all(|col| {
        collations.iter().any(|(name, collation)| {
            name.eq_ignore_ascii_case(col)
                && collation.as_deref().is_some_and(|c| c.ends_with("_bin"))
        })
    });
    if binary {
        return Ok(());
    }
    conn.query_drop(
        "ALTER TABLE kulyrax_projection_state
            MODIFY target_table VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
            MODIFY entity_key   VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
            MODIFY facet        VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT ''",
    )
    .map_err(my_err)
}

/// Lanes per `IN (…)` list (two parameters each), well under MySQL's 65,535
/// bound parameters.
const GUARD_READ_CHUNK: usize = 500;

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
        legacy::migrate_guard_table(self.conn)?;
        self.conn
            .query_drop(
                "CREATE TABLE IF NOT EXISTS kulyrax_projection_state (
                    target_table  VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                    entity_key    VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
                    facet         VARCHAR(191) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL DEFAULT '',
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
            "SELECT last_sequence, deleted, permanent FROM kulyrax_projection_state
             WHERE target_table = ? AND entity_key = ? AND facet = ? FOR UPDATE"
        } else {
            "SELECT last_sequence, deleted, permanent FROM kulyrax_projection_state
             WHERE target_table = ? AND entity_key = ? AND facet = ?"
        };
        let row: Option<(i64, bool, bool)> = self
            .conn
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

    fn has_guard_rows(&mut self, table: &str) -> Result<bool, SqlError> {
        let exists: Option<i64> = self
            .conn
            .exec_first(
                "SELECT EXISTS (SELECT 1 FROM kulyrax_projection_state WHERE target_table = ?)",
                (table.to_string(),),
            )
            .map_err(my_err)?;
        Ok(exists.is_some_and(|n| n != 0))
    }

    /// The requested lanes, `GUARD_READ_CHUNK` per query. Each is matched on
    /// the whole primary key, `(entity_key, facet) IN (…)`, so InnoDB probes
    /// it as a unique key, as `read_guard_lane` does. Matching `entity_key`
    /// alone would scan each entity's range and lock-read the record after
    /// it, which can be another projection's lane: concurrent batches on
    /// different tables deadlocked that way (ERROR 1213). Exact matches,
    /// since the key columns are binary.
    fn read_guard_lanes(
        &mut self,
        table: &str,
        lanes: &[(String, String)],
        lock: bool,
    ) -> Result<Vec<Option<GuardState>>, SqlError> {
        let mut sorted: Vec<(&str, &str)> = lanes
            .iter()
            .map(|(k, f)| (k.as_str(), f.as_str()))
            .collect();
        sorted.sort_unstable();
        sorted.dedup();
        let mut found: HashMap<(String, String), GuardState> = HashMap::new();
        for chunk in sorted.chunks(GUARD_READ_CHUNK) {
            let list = vec!["(?, ?)"; chunk.len()].join(", ");
            let sql = format!(
                "SELECT entity_key, facet, last_sequence, deleted, permanent
                 FROM kulyrax_projection_state
                 WHERE target_table = ? AND (entity_key, facet) IN ({list}){}",
                if lock { " FOR UPDATE" } else { "" }
            );
            let params: Vec<mysql::Value> = std::iter::once(table)
                .chain(chunk.iter().flat_map(|&(k, f)| [k, f]))
                .map(mysql::Value::from)
                .collect();
            let rows: Vec<(String, String, i64, bool, bool)> = self
                .conn
                .exec(sql.as_str(), mysql::Params::Positional(params))
                .map_err(my_err)?;
            for (key, facet, seq, deleted, permanent) in rows {
                let state = GuardState {
                    last_sequence: seq as u64,
                    deleted,
                    permanent,
                };
                found.insert((key, facet), state);
            }
        }
        Ok(lanes
            .iter()
            .map(|(k, f)| found.get(&(k.clone(), f.clone())).copied())
            .collect())
    }

    fn execute_write(&mut self, w: &SqlWrite) -> Result<(), SqlError> {
        let sql = w.render(Placeholders::MySql);
        let params: Vec<mysql::Value> = w.values.iter().map(json_to_value).collect();
        self.conn
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
        self.conn
            .exec_drop(sql.as_str(), mysql::Params::Positional(params))
            .map_err(my_err)
    }

    fn upsert_guard_row(&mut self, row: &GuardRow) -> Result<(), SqlError> {
        self.conn
            .exec_drop(
                "INSERT INTO kulyrax_projection_state
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
        self.conn
            .exec_drop(
                "UPDATE kulyrax_projection_state
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
            self.conn.exec_drop(
                "UPDATE kulyrax_projection_state
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
            self.conn.exec_drop(
                "UPDATE kulyrax_projection_state
                    SET deleted = FALSE, permanent = FALSE
                 WHERE target_table = ? AND entity_key = ? AND facet <> ''",
                (table.to_string(), entity_key.to_string()),
            )
        }
        .map_err(my_err)
    }

    /// Commits a transaction this value began; inside a batch, the batch
    /// commits instead (`StorageAdapter::commit_batch`).
    fn commit(mut self) -> Result<(), SqlError> {
        if !self.owns {
            return Ok(());
        }
        self.owns = false;
        self.conn.query_drop("COMMIT").map_err(my_err)
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
