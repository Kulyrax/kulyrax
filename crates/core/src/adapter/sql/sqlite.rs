use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use rusqlite::{Connection, params, params_from_iter};

use crate::{
    adapter::sql::adapter::SqlError,
    adapter::sql::exec::{self, GuardCache, GuardRow, KnownGuards, Placeholders, SqlTxn, SqlWrite},
    adapter::sql::runtime::SqlRuntimeBuilder,
    adapter::{AdapterError, AdapterResult, GuardState, Prepared, StorageAdapter},
    event::Event,
    execution::stages::{Stage, timed},
    routing::StorageKind,
    runtime::config::MigrationPolicy,
    upcast::UpcasterRegistry,
};

/// SQLite runtime adapter (Phase 4: idempotent; Phase 12: sequence-gated
/// upsert; Phase 13: delete & tombstones; Phase 15: facets). Since Phase 19.1
/// the write path lives in [`exec::project`]; this file is the SQLite driver.
pub struct SqliteAdapter {
    id: String,
    priority: u32,
    path: String,
    builder: SqlRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
    /// Opened by `prepare` and reused. Opening per event, with the
    /// default rollback journal, cost several disk flushes per write.
    conn: Mutex<Option<Connection>>,
    /// The open batch's guard cache, while a batch transaction is open on
    /// `conn` (Phase 2). Always locked after `conn`.
    batch: Mutex<Option<GuardCache>>,
    /// Entities that can have guard lanes (see [`KnownGuards`]). Locked last.
    known: Mutex<KnownGuards>,
    /// Set by `prepare` once the guard table exists.
    prepared: Prepared,
}

/// WAL with `synchronous = FULL` flushes the log once per commit, so every
/// committed projection survives a power loss. That durability matters: the
/// source checkpoint advances right after the batch commits, so a lost write
/// would never be reprojected. `synchronous = NORMAL` would be faster but
/// could lose the last commits on power loss.
fn open_connection(path: &str) -> rusqlite::Result<Connection> {
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "FULL")?;
    conn.busy_timeout(std::time::Duration::from_secs(5))?;
    // Room for the guard statements plus one write per mapped table.
    conn.set_prepared_statement_cache_capacity(64);
    Ok(conn)
}

fn poisoned() -> AdapterError {
    AdapterError::WriteFailed("SQLite connection lock poisoned by an earlier panic".to_string())
}

impl SqliteAdapter {
    pub fn new(
        id: String,
        path: String,
        priority: u32,
        builder: SqlRuntimeBuilder,
        upcasters: Arc<UpcasterRegistry>,
        migration_policy: MigrationPolicy,
    ) -> Self {
        Self {
            id,
            path,
            priority,
            builder,
            upcasters,
            migration_policy,
            conn: Mutex::new(None),
            batch: Mutex::new(None),
            known: Mutex::default(),
            prepared: Prepared::default(),
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Sql, error)
    }

    /// The connection and the open batch, locked in that order.
    #[allow(clippy::type_complexity)]
    fn lock(
        &self,
    ) -> Result<
        (
            MutexGuard<'_, Option<Connection>>,
            MutexGuard<'_, Option<GuardCache>>,
        ),
        AdapterError,
    > {
        let conn = self.conn.lock().map_err(|_| poisoned())?;
        let batch = self.batch.lock().map_err(|_| poisoned())?;
        Ok((conn, batch))
    }
}

fn connection(conn: &Option<Connection>) -> Result<&Connection, AdapterError> {
    conn.as_ref()
        .ok_or_else(|| AdapterError::WriteFailed("SQLite connection unavailable".to_string()))
}

impl StorageAdapter for SqliteAdapter {
    fn kind(&self) -> StorageKind {
        StorageKind::Sql
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn priority(&self) -> u32 {
        self.priority
    }

    /// Open the connection and create the guard table in a transaction of its
    /// own (see `exec::ensure_schema`).
    fn prepare(&self) -> Result<(), AdapterError> {
        let mut guard = self.conn.lock().map_err(|_| poisoned())?;
        self.prepared
            .prepare(|| {
                if guard.is_none() {
                    *guard = Some(open_connection(&self.path).map_err(sql_err)?);
                }
                let Some(conn) = guard.as_ref() else {
                    return Err(SqlError::ExecutionFailed(
                        "SQLite connection unavailable".to_string(),
                    ));
                };
                exec::ensure_schema(SqliteTxn::begin(conn)?)
            })
            .map_err(|e| AdapterError::WriteFailed(e.to_string()))
    }

    /// `BEGIN` on the held connection, then every guard lane the batch will
    /// read, in one query per table.
    fn begin_batch(&self, events: &[&Event]) -> Result<(), AdapterError> {
        self.prepared.require().map_err(AdapterError::WriteFailed)?;
        let (conn, mut batch) = self.lock()?;
        let conn = connection(&conn)?;
        if batch.is_some() {
            return Err(AdapterError::WriteFailed(
                "a batch is already open on this adapter".to_string(),
            ));
        }
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
        let mut txn =
            SqliteTxn::begin(conn).map_err(|e| AdapterError::WriteFailed(e.to_string()))?;
        let mut cache = GuardCache::default();
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
        *batch = Some(cache);
        Ok(())
    }

    fn commit_batch(&self) -> Result<(), AdapterError> {
        let (conn, mut batch) = self.lock()?;
        if batch.take().is_none() {
            return Ok(());
        }
        let conn = connection(&conn)?;
        timed(Stage::Commit, || conn.execute_batch("COMMIT")).map_err(|e| {
            // A failed COMMIT can leave the transaction open; end it.
            let _ = conn.execute_batch("ROLLBACK");
            AdapterError::WriteFailed(e.to_string())
        })
    }

    fn rollback_batch(&self) {
        let Ok((conn, mut batch)) = self.lock() else {
            return;
        };
        if batch.take().is_some()
            && let Some(conn) = conn.as_ref()
        {
            let _ = conn.execute_batch("ROLLBACK");
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

        let (conn, mut batch) = match self.lock() {
            Ok(locks) => locks,
            Err(e) => return self.failure(e),
        };
        let conn = match connection(&conn) {
            Ok(c) => c,
            Err(e) => return self.failure(e),
        };
        let outcome = match batch.as_mut() {
            Some(cache) => {
                exec::project_in(&mut SqliteTxn::joined(conn), &planned.plan, event, cache)
            }
            None => SqliteTxn::begin(conn).and_then(|txn| exec::project(txn, &planned.plan, event)),
        };
        exec::outcome_result(&self.id, &planned, outcome)
    }
}

// ---------------------------------------------------------------------------
// SqliteTxn — the driver
// ---------------------------------------------------------------------------

/// A transaction on the adapter's connection: its own (`begin`: `BEGIN`,
/// committed by `commit`, rolled back if dropped uncommitted), or a share of
/// an open batch transaction (`joined`: `commit` and drop leave it alone).
pub struct SqliteTxn<'a> {
    conn: &'a Connection,
    /// `true` while this value must still end the transaction itself.
    owns: bool,
}

impl<'a> SqliteTxn<'a> {
    fn begin(conn: &'a Connection) -> Result<Self, SqlError> {
        conn.execute_batch("BEGIN").map_err(sql_err)?;
        Ok(Self { conn, owns: true })
    }

    fn joined(conn: &'a Connection) -> Self {
        Self { conn, owns: false }
    }

    /// Hand the open transaction over to the caller.
    fn release(&mut self) {
        self.owns = false;
    }

    fn execute(&self, sql: &str, params: impl rusqlite::Params) -> Result<(), SqlError> {
        self.conn
            .prepare_cached(sql)
            .and_then(|mut stmt| stmt.execute(params))
            .map(|_| ())
            .map_err(sql_err)
    }
}

impl Drop for SqliteTxn<'_> {
    fn drop(&mut self) {
        if self.owns {
            let _ = self.conn.execute_batch("ROLLBACK");
        }
    }
}

fn sql_err(e: rusqlite::Error) -> SqlError {
    SqlError::ExecutionFailed(e.to_string())
}

/// Keys per `IN (…)` list, well under SQLite's bound-parameter limit.
const GUARD_READ_CHUNK: usize = 500;

fn guard_state(row: &rusqlite::Row<'_>, from: usize) -> rusqlite::Result<GuardState> {
    Ok(GuardState {
        last_sequence: row.get::<_, i64>(from)? as u64,
        deleted: row.get::<_, i64>(from + 1)? != 0,
        permanent: row.get::<_, i64>(from + 2)? != 0,
    })
}

impl SqlTxn for SqliteTxn<'_> {
    fn ensure_guard_table(&mut self) -> Result<(), SqlError> {
        self.conn
            .execute(
                "CREATE TABLE IF NOT EXISTS conduit_projection_state (
                    target_table TEXT NOT NULL,
                    entity_key TEXT NOT NULL,
                    facet TEXT NOT NULL DEFAULT '',
                    last_sequence INTEGER NOT NULL,
                    last_event_id TEXT NOT NULL,
                    deleted INTEGER NOT NULL DEFAULT 0,
                    permanent INTEGER NOT NULL DEFAULT 0,
                    processed_at TEXT NOT NULL,
                    PRIMARY KEY (target_table, entity_key, facet)
                )",
                [],
            )
            .map(|_| ())
            .map_err(sql_err)
    }

    fn has_guard_rows(&mut self, table: &str) -> Result<bool, SqlError> {
        self.conn
            .prepare_cached(
                "SELECT EXISTS (SELECT 1 FROM conduit_projection_state WHERE target_table = ?1)",
            )
            .and_then(|mut stmt| stmt.query_row(params![table], |row| row.get::<_, i64>(0)))
            .map(|n| n != 0)
            .map_err(sql_err)
    }

    fn read_guard_lane(
        &mut self,
        table: &str,
        entity_key: &str,
        facet: &str,
        _lock: bool, // SQLite writers already serialize; FOR UPDATE is a no-op.
    ) -> Result<Option<GuardState>, SqlError> {
        let mut stmt = self
            .conn
            .prepare_cached(
                "SELECT last_sequence, deleted, permanent FROM conduit_projection_state
                 WHERE target_table = ?1 AND entity_key = ?2 AND facet = ?3",
            )
            .map_err(sql_err)?;
        match stmt.query_row(params![table, entity_key, facet], |row| guard_state(row, 0)) {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(sql_err(e)),
        }
    }

    /// Every lane of the requested entities, `GUARD_READ_CHUNK` entities per
    /// query.
    fn read_guard_lanes(
        &mut self,
        table: &str,
        lanes: &[(String, String)],
        _lock: bool,
    ) -> Result<Vec<Option<GuardState>>, SqlError> {
        let mut keys: Vec<&str> = lanes.iter().map(|(k, _)| k.as_str()).collect();
        keys.sort_unstable();
        keys.dedup();
        let mut found: HashMap<(String, String), GuardState> = HashMap::new();
        for chunk in keys.chunks(GUARD_READ_CHUNK) {
            let list = (2..chunk.len() + 2)
                .map(|i| format!("?{i}"))
                .collect::<Vec<_>>()
                .join(", ");
            let sql = format!(
                "SELECT entity_key, facet, last_sequence, deleted, permanent
                 FROM conduit_projection_state
                 WHERE target_table = ?1 AND entity_key IN ({list})"
            );
            let mut stmt = self.conn.prepare_cached(&sql).map_err(sql_err)?;
            let params = std::iter::once(table).chain(chunk.iter().copied());
            let rows = stmt
                .query_map(params_from_iter(params), |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        guard_state(row, 2)?,
                    ))
                })
                .map_err(sql_err)?;
            for row in rows {
                let (key, facet, state) = row.map_err(sql_err)?;
                found.insert((key, facet), state);
            }
        }
        Ok(lanes
            .iter()
            .map(|(k, f)| found.get(&(k.clone(), f.clone())).copied())
            .collect())
    }

    fn execute_write(&mut self, w: &SqlWrite) -> Result<(), SqlError> {
        let sql = w.render(Placeholders::Question);
        let binds: Vec<String> = w.values.iter().map(exec::scalar_to_string).collect();
        self.execute(&sql, params_from_iter(binds.iter()))
    }

    fn delete_row(
        &mut self,
        table: &str,
        cols: &[String],
        vals: &[serde_json::Value],
    ) -> Result<(), SqlError> {
        let where_clause = cols
            .iter()
            .map(|c| format!("{c} = ?"))
            .collect::<Vec<_>>()
            .join(" AND ");
        let sql = format!("DELETE FROM {table} WHERE {where_clause}");
        let binds: Vec<String> = vals.iter().map(exec::scalar_to_string).collect();
        self.execute(&sql, params_from_iter(binds.iter()))
    }

    fn upsert_guard_row(&mut self, row: &GuardRow) -> Result<(), SqlError> {
        self.execute(
            "INSERT INTO conduit_projection_state
                (target_table, entity_key, facet, last_sequence, last_event_id, deleted, permanent, processed_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, datetime('now'))
             ON CONFLICT (target_table, entity_key, facet) DO UPDATE SET
                last_sequence = excluded.last_sequence,
                last_event_id = excluded.last_event_id,
                deleted = excluded.deleted,
                permanent = excluded.permanent,
                processed_at = excluded.processed_at",
            params![
                row.table,
                row.entity_key,
                row.facet,
                row.last_sequence as i64,
                row.last_event_id,
                row.deleted as i64,
                row.permanent as i64
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
        self.execute(
            "UPDATE conduit_projection_state
                SET last_sequence = ?4, last_event_id = ?5, processed_at = datetime('now')
             WHERE target_table = ?1 AND entity_key = ?2 AND facet = ?3",
            params![table, entity_key, facet, seq as i64, event_id],
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
        if deleted {
            self.execute(
                "UPDATE conduit_projection_state
                    SET deleted = 1, permanent = ?4, last_sequence = ?3,
                        last_event_id = ?5, processed_at = datetime('now')
                 WHERE target_table = ?1 AND entity_key = ?2 AND facet <> ''",
                params![table, entity_key, seq as i64, permanent as i64, event_id],
            )
        } else {
            self.execute(
                "UPDATE conduit_projection_state
                    SET deleted = 0, permanent = 0
                 WHERE target_table = ?1 AND entity_key = ?2 AND facet <> ''",
                params![table, entity_key],
            )
        }
    }

    /// Commits a transaction this value began; inside a batch, the batch
    /// commits instead (`StorageAdapter::commit_batch`).
    fn commit(mut self) -> Result<(), SqlError> {
        if !self.owns {
            return Ok(());
        }
        self.conn.execute_batch("COMMIT").map_err(sql_err)?;
        self.owns = false;
        Ok(())
    }
}
