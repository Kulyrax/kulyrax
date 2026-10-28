//! Phase 19.2–19.4 (moved out of `kulyrax-core` in Phase 28): the Postgres
//! SQL backend.
//!
//! Same `SqlMapping` / `decide()` / guard model as SQLite — this crate
//! supplies only what a driver genuinely owns: an r2d2 connection pool, a
//! `READ COMMITTED` transaction with `SELECT … FOR UPDATE` on the guard row
//! (real multi-writer safety, Phase 19.4), `$N` placeholders, typed column
//! binding (Phase 19.3), and the Postgres guard-table DDL.
//!
//! Registers itself on a [`kulyrax_core::KulyraxRuntime`] under `type:
//! postgres` via [`factory`] — see `kulyrax-backends`, which wires this up by
//! default, or `docs/writing-a-backend.md` for the extension pattern this
//! crate is itself a worked example of.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use postgres::types::ToSql;
use postgres::{NoTls, Statement};
use r2d2_postgres::PostgresConnectionManager;
use serde::Deserialize;
use serde_json::Value;

use kulyrax_core::adapter::sql::adapter::SqlError;
use kulyrax_core::adapter::sql::exec::{
    self, GuardCache, GuardRow, Placeholders, SqlTxn, SqlWrite,
};
use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
use kulyrax_core::adapter::{AdapterError, AdapterResult, GuardState, Prepared, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::execution::stages::{Stage, timed};
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::AdapterBuildContext;
use kulyrax_core::runtime::config::{ConfigError, CustomAdapterConfig, MigrationPolicy};
use kulyrax_core::upcast::UpcasterRegistry;

mod bulk;

use bulk::Pending;

type PgPool = r2d2::Pool<PostgresConnectionManager<NoTls>>;

/// Introspected column type — enough to bind a JSON scalar correctly (Phase 19.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PgColType {
    Text,
    /// `smallint` (int2) — Postgres's wire protocol requires an exact
    /// width match on a bound parameter, not just "any integer", so this
    /// is its own variant rather than folded into `Int`/`BigInt`.
    SmallInt,
    /// `integer` (int4) — the common case or `PgColType` previously
    /// treated no differently from `bigint`, binding every integer column
    /// as `i64`; Postgres rejects an `i64` bind for an int4 parameter
    /// (`WrongType { postgres: Int4, rust: "i64" }`), so a plain
    /// `INTEGER` column — the default width in nearly every real schema —
    /// never actually worked through this adapter until this variant.
    Int,
    /// `bigint` (int8).
    BigInt,
    Float,
    Bool,
    Timestamptz,
    Timestamp,
    Date,
    /// numeric / uuid / json / jsonb / anything else — bound as text and left
    /// to Postgres's assignment cast.
    Other,
}

impl PgColType {
    fn from_data_type(dt: &str) -> Self {
        match dt {
            "smallint" => PgColType::SmallInt,
            "integer" => PgColType::Int,
            "bigint" => PgColType::BigInt,
            "real" | "double precision" => PgColType::Float,
            "boolean" => PgColType::Bool,
            "timestamp with time zone" => PgColType::Timestamptz,
            "timestamp without time zone" => PgColType::Timestamp,
            "date" => PgColType::Date,
            "character varying" | "character" | "text" | "name" | "citext" => PgColType::Text,
            _ => PgColType::Other,
        }
    }
}

type ColTypes = HashMap<String, PgColType>;

/// The `config:` block for `type: postgres` — moved out of
/// `kulyrax-core::runtime::config` in Phase 28 (Postgres no longer has a
/// typed `AdapterConfig` variant; this is parsed by [`factory`] from the
/// `Custom` entry's raw `config:` sub-value).
#[derive(Debug, Deserialize)]
pub struct PostgresConfig {
    /// `postgres://user:pass@host:port/db`. May contain `${ENV_VAR}` references
    /// (Phase 19.2) so credentials stay out of the committed config.
    pub url: String,
    /// r2d2 pool size (default 4).
    #[serde(default)]
    pub pool_size: Option<u32>,
}

/// Postgres SQL adapter (Phase 19). The first adapter **without** a
/// single-writer assumption — concurrent dispatch threads and concurrent
/// Kulyrax instances serialize on the `SELECT … FOR UPDATE` guard row.
pub struct PostgresAdapter {
    id: String,
    priority: u32,
    /// `Err` (a stored message) when the URL didn't parse — every `handle()`
    /// then fails cleanly rather than the factory being fallible.
    pool: Result<PgPool, String>,
    builder: SqlRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
    /// Per-target-table `information_schema.columns` map, introspected once.
    col_types: Arc<Mutex<HashMap<String, Arc<ColTypes>>>>,
    /// The open batch, from `begin_batch` to `commit_batch` /
    /// `rollback_batch` (Phase 2).
    batch: Mutex<Option<PgBatch>>,
    /// Set by `prepare` once the guard table exists.
    prepared: Prepared,
}

/// One batch's held connection, with its transaction open: one checkout and
/// one `BEGIN` per batch instead of per event.
struct PgBatch {
    client: r2d2::PooledConnection<PostgresConnectionManager<NoTls>>,
    /// Statements prepared on `client` during this batch.
    statements: HashMap<String, Statement>,
    cache: GuardCache,
    /// The batch's writes, sent in bulk by `commit_batch` (see [`bulk`]).
    pending: Pending,
}

fn poisoned() -> AdapterError {
    AdapterError::WriteFailed("Postgres batch lock poisoned by an earlier panic".to_string())
}

impl PostgresAdapter {
    /// Build the pool from a connection URL (`postgres://user:pass@host/db`).
    /// Infallible — a bad URL is stored and surfaced per-event, keeping
    /// `build_adapters_from_config` free of a `Result`. The pool is
    /// `build_unchecked` (lazy connect): connectivity problems surface on the
    /// first real write, not at construction.
    pub fn new(
        id: String,
        url: &str,
        pool_size: u32,
        priority: u32,
        builder: SqlRuntimeBuilder,
        upcasters: Arc<UpcasterRegistry>,
        migration_policy: MigrationPolicy,
    ) -> Self {
        let pool = url
            .parse::<postgres::Config>()
            .map_err(|e| format!("invalid postgres url: {e}"))
            .map(|config| {
                let manager = PostgresConnectionManager::new(config, NoTls);
                r2d2::Pool::builder()
                    .max_size(pool_size.max(1))
                    .build_unchecked(manager)
            });
        Self {
            id,
            priority,
            pool,
            builder,
            upcasters,
            migration_policy,
            col_types: Arc::new(Mutex::new(HashMap::new())),
            batch: Mutex::new(None),
            prepared: Prepared::default(),
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Sql, error)
    }

    /// Column types for `table`, introspected + cached on first use.
    fn column_types(
        &self,
        client: &mut postgres::Client,
        table: &str,
    ) -> Result<Arc<ColTypes>, SqlError> {
        if let Some(c) = self
            .col_types
            .lock()
            .ok()
            .and_then(|m| m.get(table).cloned())
        {
            return Ok(c);
        }
        let rows = client
            .query(
                "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1",
                &[&table],
            )
            .map_err(|e| SqlError::ExecutionFailed(e.to_string()))?;
        let mut map = ColTypes::new();
        for row in rows {
            let name: String = row.get(0);
            let dt: String = row.get(1);
            map.insert(name, PgColType::from_data_type(&dt));
        }
        let arc = Arc::new(map);
        if let Ok(mut m) = self.col_types.lock() {
            m.insert(table.to_string(), Arc::clone(&arc));
        }
        Ok(arc)
    }
}

impl StorageAdapter for PostgresAdapter {
    fn kind(&self) -> StorageKind {
        StorageKind::Sql
    }
    fn id(&self) -> &str {
        &self.id
    }
    fn priority(&self) -> u32 {
        self.priority
    }

    /// Create the guard table in a transaction of its own (see
    /// `exec::ensure_schema`).
    fn prepare(&self) -> Result<(), AdapterError> {
        let pool = self
            .pool
            .as_ref()
            .map_err(|e| AdapterError::WriteFailed(e.clone()))?;
        self.prepared
            .prepare(|| {
                let mut client = pool
                    .get()
                    .map_err(|e| SqlError::ExecutionFailed(e.to_string()))?;
                exec::ensure_schema(PostgresTxn::begin(&mut client, Arc::default(), None)?)
            })
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))
    }

    /// Check out one connection for the whole batch, `BEGIN`, and lock and
    /// load every guard lane the batch will read, one query per table.
    fn begin_batch(&self, events: &[&Event]) -> Result<(), AdapterError> {
        self.prepared.require().map_err(AdapterError::WriteFailed)?;
        let mut batch = self.batch.lock().map_err(|_| poisoned())?;
        if batch.is_some() {
            return Err(AdapterError::WriteFailed(
                "a batch is already open on this adapter".to_string(),
            ));
        }
        let pool = self
            .pool
            .as_ref()
            .map_err(|e| AdapterError::WriteFailed(e.clone()))?;
        let mut client = pool
            .get()
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
        let mut statements = HashMap::new();
        let mut cache = GuardCache::default();
        {
            let mut txn = PostgresTxn::begin(&mut client, Arc::default(), Some(&mut statements))
                .map_err(|e| AdapterError::WriteFailed(e.to_string()))?;
            exec::prefetch_guards(&mut txn, plans.iter().map(|p| &p.plan), &mut cache)
                .map_err(|e| AdapterError::WriteFailed(e.to_string()))?;
            // From here the batch owns the transaction; `txn` must not roll it back.
            txn.release();
        }
        *batch = Some(PgBatch {
            client,
            statements,
            cache,
            pending: Pending::default(),
        });
        Ok(())
    }

    /// Send the batch's writes in bulk, then `COMMIT`. A failure leaves the
    /// transaction to be rolled back, and the batch falls back to one event
    /// at a time.
    fn commit_batch(&self) -> Result<(), AdapterError> {
        let mut batch = self.batch.lock().map_err(|_| poisoned())?;
        let Some(mut b) = batch.take() else {
            return Ok(());
        };
        let flushed = timed(Stage::Write, || {
            let mut txn =
                PostgresTxn::joined(&mut b.client, Arc::default(), Some(&mut b.statements));
            txn.flush(&mut b.pending)
        });
        if let Err(e) = flushed {
            let _ = b.client.batch_execute("ROLLBACK");
            return Err(AdapterError::WriteFailed(e.to_string()));
        }
        // Close the batch's statements before the connection goes back to the pool.
        b.statements.clear();
        timed(Stage::Commit, || b.client.batch_execute("COMMIT")).map_err(|e| {
            // A failed COMMIT can leave the transaction open; end it.
            let _ = b.client.batch_execute("ROLLBACK");
            AdapterError::WriteFailed(e.to_string())
        })
    }

    fn rollback_batch(&self) {
        let Ok(mut batch) = self.batch.lock() else {
            return;
        };
        if let Some(mut b) = batch.take() {
            b.statements.clear();
            let _ = b.client.batch_execute("ROLLBACK");
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
        let plan = &planned.plan;

        let mut batch = match self.batch.lock() {
            Ok(b) => b,
            Err(_) => return self.failure(poisoned()),
        };
        let outcome = match batch.as_mut() {
            Some(b) => self
                .column_types(&mut b.client, &plan.table)
                .and_then(|col_types| {
                    let mut txn =
                        PostgresTxn::joined(&mut b.client, col_types, Some(&mut b.statements))
                            .deferred(&mut b.pending, &plan.entity_key);
                    exec::project_in(&mut txn, plan, event, &mut b.cache)
                }),
            None => {
                let pool = match &self.pool {
                    Ok(p) => p,
                    Err(e) => return self.failure(AdapterError::WriteFailed(e.clone())),
                };
                let mut client = match pool.get() {
                    Ok(c) => c,
                    Err(e) => return self.failure(AdapterError::WriteFailed(e.to_string())),
                };
                self.column_types(&mut client, &plan.table)
                    .and_then(|col_types| PostgresTxn::begin(&mut client, col_types, None))
                    .and_then(|txn| exec::project(txn, plan, event))
            }
        };
        exec::outcome_result(&self.id, &planned, outcome)
    }
}

// ---------------------------------------------------------------------------
// PostgresTxn
// ---------------------------------------------------------------------------

/// A transaction on one connection: its own (`begin`: `BEGIN`, committed by
/// `commit`, rolled back if dropped uncommitted), or a share of an open batch
/// transaction (`joined`: `commit` and drop leave it alone).
pub struct PostgresTxn<'a> {
    client: &'a mut postgres::Client,
    col_types: Arc<ColTypes>,
    /// Statements prepared on this connection for the batch, by SQL text, so
    /// each runs in one round trip instead of a prepare plus an execute.
    /// `None` outside a batch.
    statements: Option<&'a mut HashMap<String, Statement>>,
    /// Inside a batch: where target and guard writes are recorded instead of
    /// sent (see [`bulk`]), and the entity they belong to.
    deferred: Option<(&'a mut Pending, &'a str)>,
    /// `true` while this value must still end the transaction itself.
    owns: bool,
}

impl<'a> PostgresTxn<'a> {
    fn begin(
        client: &'a mut postgres::Client,
        col_types: Arc<ColTypes>,
        statements: Option<&'a mut HashMap<String, Statement>>,
    ) -> Result<Self, SqlError> {
        client
            .batch_execute("BEGIN ISOLATION LEVEL READ COMMITTED")
            .map_err(pg_err)?;
        Ok(Self {
            client,
            col_types,
            statements,
            deferred: None,
            owns: true,
        })
    }

    fn joined(
        client: &'a mut postgres::Client,
        col_types: Arc<ColTypes>,
        statements: Option<&'a mut HashMap<String, Statement>>,
    ) -> Self {
        Self {
            client,
            col_types,
            statements,
            deferred: None,
            owns: false,
        }
    }

    /// Hand the open transaction over to the caller.
    fn release(&mut self) {
        self.owns = false;
    }

    /// Record target and guard writes for `entity_key` in `pending` instead
    /// of sending them.
    fn deferred(mut self, pending: &'a mut Pending, entity_key: &'a str) -> Self {
        self.deferred = Some((pending, entity_key));
        self
    }

    /// Send everything recorded in `pending`.
    fn flush(&mut self, pending: &mut Pending) -> Result<(), SqlError> {
        if pending.is_empty() {
            return Ok(());
        }
        pending.flush(|sql, params| self.execute(sql, params))
    }

    /// The batch's prepared statement for `sql`, preparing it on first use.
    fn statement(&mut self, sql: &str) -> Result<Option<Statement>, SqlError> {
        let Some(cache) = self.statements.as_deref_mut() else {
            return Ok(None);
        };
        if let Some(stmt) = cache.get(sql) {
            return Ok(Some(stmt.clone()));
        }
        let stmt = self.client.prepare(sql).map_err(pg_err)?;
        cache.insert(sql.to_string(), stmt.clone());
        Ok(Some(stmt))
    }

    fn execute(&mut self, sql: &str, params: &[&(dyn ToSql + Sync)]) -> Result<(), SqlError> {
        match self.statement(sql)? {
            Some(stmt) => self.client.execute(&stmt, params),
            None => self.client.execute(sql, params),
        }
        .map(|_| ())
        .map_err(pg_err)
    }

    fn query(
        &mut self,
        sql: &str,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<postgres::Row>, SqlError> {
        match self.statement(sql)? {
            Some(stmt) => self.client.query(&stmt, params),
            None => self.client.query(sql, params),
        }
        .map_err(pg_err)
    }
}

impl Drop for PostgresTxn<'_> {
    fn drop(&mut self) {
        if self.owns {
            let _ = self.client.batch_execute("ROLLBACK");
        }
    }
}

fn pg_err(e: postgres::Error) -> SqlError {
    SqlError::ExecutionFailed(e.to_string())
}

/// A single owned, bindable value.
enum Bound {
    Null,
    Text(String),
    SmallInt(i16),
    Int(i32),
    BigInt(i64),
    Float(f64),
    Bool(bool),
    Tstz(DateTime<Utc>),
    Ts(NaiveDateTime),
    Date(NaiveDate),
}

impl Bound {
    fn as_sql(&self) -> &(dyn ToSql + Sync) {
        match self {
            Bound::Null => &Option::<&str>::None,
            Bound::Text(s) => s,
            Bound::SmallInt(v) => v,
            Bound::Int(v) => v,
            Bound::BigInt(v) => v,
            Bound::Float(v) => v,
            Bound::Bool(v) => v,
            Bound::Tstz(v) => v,
            Bound::Ts(v) => v,
            Bound::Date(v) => v,
        }
    }
}

/// Convert a JSON scalar to the bind form for `col_type` (Phase 19.3).
fn bind_value(v: &Value, col_type: PgColType) -> Result<Bound, SqlError> {
    if v.is_null() {
        return Ok(Bound::Null);
    }
    match col_type {
        PgColType::Bool => v
            .as_bool()
            .map(Bound::Bool)
            .ok_or_else(|| SqlError::ExecutionFailed(format!("expected boolean, got {v}"))),
        PgColType::SmallInt => v
            .as_i64()
            .ok_or_else(|| SqlError::ExecutionFailed(format!("expected integer, got {v}")))
            .and_then(|n| {
                i16::try_from(n)
                    .map(Bound::SmallInt)
                    .map_err(|_| SqlError::ExecutionFailed(format!("{n} does not fit in smallint")))
            }),
        PgColType::Int => v
            .as_i64()
            .ok_or_else(|| SqlError::ExecutionFailed(format!("expected integer, got {v}")))
            .and_then(|n| {
                i32::try_from(n)
                    .map(Bound::Int)
                    .map_err(|_| SqlError::ExecutionFailed(format!("{n} does not fit in integer")))
            }),
        PgColType::BigInt => v
            .as_i64()
            .map(Bound::BigInt)
            .ok_or_else(|| SqlError::ExecutionFailed(format!("expected integer, got {v}"))),
        PgColType::Float => v
            .as_f64()
            .map(Bound::Float)
            .ok_or_else(|| SqlError::ExecutionFailed(format!("expected number, got {v}"))),
        PgColType::Timestamptz => v
            .as_str()
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .map(|dt| Bound::Tstz(dt.with_timezone(&Utc)))
            .ok_or_else(|| {
                SqlError::ExecutionFailed(format!("expected an RFC 3339 timestamp, got {v}"))
            }),
        PgColType::Timestamp => v
            .as_str()
            .and_then(|s| {
                NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                    .or_else(|_| NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S"))
                    .ok()
            })
            .map(Bound::Ts)
            .ok_or_else(|| SqlError::ExecutionFailed(format!("expected a timestamp, got {v}"))),
        PgColType::Date => v
            .as_str()
            .and_then(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
            .map(Bound::Date)
            .ok_or_else(|| SqlError::ExecutionFailed(format!("expected a date, got {v}"))),
        PgColType::Text | PgColType::Other => Ok(Bound::Text(exec::scalar_to_string(v))),
    }
}

impl PostgresTxn<'_> {
    fn bind_write(&self, w: &SqlWrite) -> Result<Vec<Bound>, SqlError> {
        let mut out = Vec::with_capacity(w.values.len());
        for (col, val) in w.columns.iter().zip(&w.values) {
            let ct = self.col_types.get(col).copied().ok_or_else(|| {
                SqlError::ExecutionFailed(format!(
                    "column '{col}' of mapping is not in table '{}'",
                    w.table
                ))
            })?;
            out.push(bind_value(val, ct)?);
        }
        Ok(out)
    }
}

fn guard_state(row: &postgres::Row, from: usize) -> GuardState {
    GuardState {
        last_sequence: row.get::<_, i64>(from) as u64,
        deleted: row.get(from + 1),
        permanent: row.get(from + 2),
    }
}

impl SqlTxn for PostgresTxn<'_> {
    fn ensure_guard_table(&mut self) -> Result<(), SqlError> {
        // `IF NOT EXISTS` isn't safe under concurrent first creation: two
        // sessions can both see the table missing and race on the pg_type
        // catalog insert, so the loser gets a `unique_violation` instead of
        // the quiet no-op the SQL text implies. Run the DDL in its own
        // savepoint so that benign race only unwinds to here instead of
        // aborting the caller's whole transaction, and swallow exactly that
        // one error code — by definition the winner already created the
        // table, so there is nothing left to do.
        self.client
            .batch_execute("SAVEPOINT kulyrax_guard_table")
            .map_err(pg_err)?;
        let result = self.client.batch_execute(
            "CREATE TABLE IF NOT EXISTS conduit_projection_state (
                    target_table  text        NOT NULL,
                    entity_key    text        NOT NULL,
                    facet         text        NOT NULL DEFAULT '',
                    last_sequence bigint      NOT NULL,
                    last_event_id text        NOT NULL,
                    deleted       boolean     NOT NULL DEFAULT false,
                    permanent     boolean     NOT NULL DEFAULT false,
                    processed_at  timestamptz NOT NULL DEFAULT now(),
                    PRIMARY KEY (target_table, entity_key, facet)
                )",
        );
        match result {
            Ok(()) => self
                .client
                .batch_execute("RELEASE SAVEPOINT kulyrax_guard_table")
                .map_err(pg_err),
            Err(e) if e.code() == Some(&postgres::error::SqlState::UNIQUE_VIOLATION) => self
                .client
                .batch_execute("ROLLBACK TO SAVEPOINT kulyrax_guard_table")
                .map_err(pg_err),
            Err(e) => Err(pg_err(e)),
        }
    }

    fn read_guard_lane(
        &mut self,
        table: &str,
        entity_key: &str,
        facet: &str,
        lock: bool,
    ) -> Result<Option<GuardState>, SqlError> {
        // Phase 19.4: FOR UPDATE serializes concurrent writers on this row.
        let sql = if lock {
            "SELECT last_sequence, deleted, permanent FROM conduit_projection_state
             WHERE target_table = $1 AND entity_key = $2 AND facet = $3 FOR UPDATE"
        } else {
            "SELECT last_sequence, deleted, permanent FROM conduit_projection_state
             WHERE target_table = $1 AND entity_key = $2 AND facet = $3"
        };
        let rows = self.query(sql, &[&table, &entity_key, &facet])?;
        Ok(rows.first().map(|r| guard_state(r, 0)))
    }

    /// Every lane of the requested entities in one query, locked in
    /// `(entity_key, facet)` order.
    fn read_guard_lanes(
        &mut self,
        table: &str,
        lanes: &[(String, String)],
        lock: bool,
    ) -> Result<Vec<Option<GuardState>>, SqlError> {
        let mut keys: Vec<&str> = lanes.iter().map(|(k, _)| k.as_str()).collect();
        keys.sort_unstable();
        keys.dedup();
        let sql = if lock {
            "SELECT entity_key, facet, last_sequence, deleted, permanent
             FROM conduit_projection_state
             WHERE target_table = $1 AND entity_key = ANY($2)
             ORDER BY entity_key, facet FOR UPDATE"
        } else {
            "SELECT entity_key, facet, last_sequence, deleted, permanent
             FROM conduit_projection_state
             WHERE target_table = $1 AND entity_key = ANY($2)"
        };
        let rows = self.query(sql, &[&table, &keys])?;
        let found: HashMap<(String, String), GuardState> = rows
            .iter()
            .map(|r| ((r.get(0), r.get(1)), guard_state(r, 2)))
            .collect();
        Ok(lanes
            .iter()
            .map(|(k, f)| found.get(&(k.clone(), f.clone())).copied())
            .collect())
    }

    fn execute_write(&mut self, w: &SqlWrite) -> Result<(), SqlError> {
        // Bound here, inside or outside a batch, so a value that doesn't fit
        // its column fails this event's `handle`.
        let bounds = self.bind_write(w)?;
        if let Some((pending, entity_key)) = self.deferred.as_mut() {
            return pending.write(entity_key, &self.col_types, w, bounds);
        }
        let sql = w.render(Placeholders::Dollar);
        let params: Vec<&(dyn ToSql + Sync)> = bounds.iter().map(|b| b.as_sql()).collect();
        self.execute(&sql, &params)
    }

    fn delete_row(&mut self, table: &str, cols: &[String], vals: &[Value]) -> Result<(), SqlError> {
        // The key columns' types come from the same introspected map.
        let mut bounds = Vec::with_capacity(vals.len());
        for (col, v) in cols.iter().zip(vals) {
            let ct = self.col_types.get(col).copied().unwrap_or(PgColType::Text);
            bounds.push(bind_value(v, ct)?);
        }
        if let Some((pending, entity_key)) = self.deferred.as_mut() {
            pending.delete(
                table,
                entity_key,
                &self.col_types,
                cols.iter().cloned().zip(bounds).collect(),
            );
            return Ok(());
        }
        let where_clause = cols
            .iter()
            .enumerate()
            .map(|(i, c)| format!("{c} = ${}", i + 1))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!("DELETE FROM {table} WHERE {where_clause}");
        let params: Vec<&(dyn ToSql + Sync)> = bounds.iter().map(|b| b.as_sql()).collect();
        self.execute(&sql, &params)
    }

    fn upsert_guard_row(&mut self, row: &GuardRow) -> Result<(), SqlError> {
        if let Some((pending, _)) = self.deferred.as_mut() {
            pending.guard(row);
            return Ok(());
        }
        self.execute(
            "INSERT INTO conduit_projection_state
                (target_table, entity_key, facet, last_sequence, last_event_id, deleted, permanent, processed_at)
             VALUES ($1, $2, $3, $4, $5, $6, $7, now())
             ON CONFLICT (target_table, entity_key, facet) DO UPDATE SET
                last_sequence = excluded.last_sequence,
                last_event_id = excluded.last_event_id,
                deleted = excluded.deleted,
                permanent = excluded.permanent,
                processed_at = excluded.processed_at",
            &[
                &row.table,
                &row.entity_key,
                &row.facet,
                &(row.last_sequence as i64),
                &row.last_event_id,
                &row.deleted,
                &row.permanent,
            ],
        )
    }

    fn bump_guard_lane(
        &mut self,
        table: &str,
        entity_key: &str,
        facet: &str,
        seq: u64,
        event_id: &str,
    ) -> Result<(), SqlError> {
        if let Some((pending, _)) = self.deferred.as_mut() {
            pending.bump(table, entity_key, facet, seq, event_id);
            return Ok(());
        }
        self.execute(
            "UPDATE conduit_projection_state
                SET last_sequence = $4, last_event_id = $5, processed_at = now()
             WHERE target_table = $1 AND entity_key = $2 AND facet = $3",
            &[&table, &entity_key, &facet, &(seq as i64), &event_id],
        )
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
        // The cascade updates every named-facet lane of the entity, including
        // lanes this batch never loaded, so it runs as a statement of its own,
        // after everything recorded so far.
        if let Some((pending, key)) = self.deferred.take() {
            let flushed = self.flush(pending);
            self.deferred = Some((pending, key));
            flushed?;
        }
        if deleted {
            self.execute(
                "UPDATE conduit_projection_state
                    SET deleted = true, permanent = $4, last_sequence = $3,
                        last_event_id = $5, processed_at = now()
                 WHERE target_table = $1 AND entity_key = $2 AND facet <> ''",
                &[&table, &entity_key, &(seq as i64), &permanent, &event_id],
            )
        } else {
            self.execute(
                "UPDATE conduit_projection_state
                    SET deleted = false, permanent = false
                 WHERE target_table = $1 AND entity_key = $2 AND facet <> ''",
                &[&table, &entity_key],
            )
        }
    }

    /// Commits a transaction this value began; inside a batch, the batch
    /// commits instead (`StorageAdapter::commit_batch`).
    fn commit(mut self) -> Result<(), SqlError> {
        if !self.owns {
            return Ok(());
        }
        self.client.batch_execute("COMMIT").map_err(pg_err)?;
        self.owns = false;
        Ok(())
    }
}

/// Registry factory for `type: postgres`.
pub fn factory(
    entry: &CustomAdapterConfig,
    ctx: &AdapterBuildContext,
) -> Result<Box<dyn StorageAdapter>, ConfigError> {
    let id = entry.id.clone();
    let cfg: PostgresConfig = entry.backend_config()?;

    // Checked here rather than in config validation, which cannot see
    // registry-resolved types.
    let url = kulyrax_core::runtime::config::expand_env(&cfg.url);
    if url.trim().is_empty() {
        return Err(ConfigError::FactoryConfigInvalid(format!(
            "postgres adapter {id:?}: `url` is empty"
        )));
    }
    if url.parse::<postgres::Config>().is_err() {
        return Err(ConfigError::FactoryConfigInvalid(format!(
            "postgres adapter {id:?}: `url` does not parse: {url}"
        )));
    }

    Ok(Box::new(PostgresAdapter::new(
        id,
        &url,
        cfg.pool_size.unwrap_or(4),
        entry.priority,
        SqlRuntimeBuilder::new(ctx.mappings.sql.clone()),
        Arc::clone(&ctx.upcasters),
        ctx.migration_policy,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_type_names_map_to_bind_categories() {
        assert_eq!(PgColType::from_data_type("smallint"), PgColType::SmallInt);
        assert_eq!(PgColType::from_data_type("integer"), PgColType::Int);
        assert_eq!(PgColType::from_data_type("bigint"), PgColType::BigInt);
        assert_eq!(PgColType::from_data_type("boolean"), PgColType::Bool);
        assert_eq!(
            PgColType::from_data_type("double precision"),
            PgColType::Float
        );
        assert_eq!(
            PgColType::from_data_type("timestamp with time zone"),
            PgColType::Timestamptz
        );
        assert_eq!(PgColType::from_data_type("date"), PgColType::Date);
        assert_eq!(PgColType::from_data_type("text"), PgColType::Text);
        assert_eq!(
            PgColType::from_data_type("character varying"),
            PgColType::Text
        );
        assert_eq!(PgColType::from_data_type("uuid"), PgColType::Other);
        assert_eq!(PgColType::from_data_type("jsonb"), PgColType::Other);
    }

    #[test]
    fn bind_value_coerces_by_column_type() {
        use serde_json::json;
        assert!(matches!(
            bind_value(&json!(42), PgColType::Int).unwrap(),
            Bound::Int(42)
        ));
        assert!(matches!(
            bind_value(&json!(true), PgColType::Bool).unwrap(),
            Bound::Bool(true)
        ));
        assert!(matches!(
            bind_value(&json!("2026-01-02T03:04:05Z"), PgColType::Timestamptz).unwrap(),
            Bound::Tstz(_)
        ));
        assert!(matches!(
            bind_value(&json!("2026-01-02"), PgColType::Date).unwrap(),
            Bound::Date(_)
        ));
        assert!(matches!(
            bind_value(&json!(null), PgColType::Int).unwrap(),
            Bound::Null
        ));
        // wrong shape for the column → a clear error, not a silent coercion
        assert!(bind_value(&json!("not a number"), PgColType::Int).is_err());
        assert!(bind_value(&json!("nonsense"), PgColType::Timestamptz).is_err());
    }

    /// A plain `integer` (int4) column — not `bigint` — must bind as `i32`,
    /// not `i64`. Before `PgColType::Int`/`PgColType::BigInt` were split
    /// apart, every integer column bound as `i64` regardless of its actual
    /// Postgres width; rust-postgres's extended protocol rejects an `i64`
    /// value for a server-negotiated int4 parameter slot
    /// (`WrongType { postgres: Int4, rust: "i64" }`), so a plain `INTEGER`
    /// column — the default width in nearly every real schema — never
    /// actually worked through this adapter. This only asserts the binding
    /// categories are distinct; `pg_backend.rs`'s integration tests exercise
    /// the real wire-level behavior against a live Postgres.
    #[test]
    fn smallint_int_and_bigint_bind_to_their_own_width() {
        use serde_json::json;
        assert!(matches!(
            bind_value(&json!(42), PgColType::SmallInt).unwrap(),
            Bound::SmallInt(42)
        ));
        assert!(matches!(
            bind_value(&json!(42), PgColType::Int).unwrap(),
            Bound::Int(42)
        ));
        assert!(matches!(
            bind_value(&json!(42), PgColType::BigInt).unwrap(),
            Bound::BigInt(42)
        ));
        // A value that doesn't fit the column's width is a clear error, not
        // a silent truncation.
        assert!(bind_value(&json!(i64::MAX), PgColType::Int).is_err());
        assert!(bind_value(&json!(100_000), PgColType::SmallInt).is_err());
    }

    #[test]
    fn factory_rejects_an_unparseable_url() {
        let raw = serde_yaml::to_value(serde_json::json!({
            "type": "postgres",
            "id": "pg1",
            "priority": 10,
            "config": { "url": "" },
        }))
        .unwrap();
        let entry = CustomAdapterConfig {
            type_name: "postgres".into(),
            id: "pg1".into(),
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
        let result = factory(&entry, &ctx);
        assert!(matches!(result, Err(ConfigError::FactoryConfigInvalid(_))));
    }
}
