use std::sync::Arc;

use rusqlite::{Connection, Transaction, params, params_from_iter};

use crate::{
    adapter::sql::adapter::SqlError,
    adapter::sql::exec::{self, GuardRow, Placeholders, SqlOutcome, SqlPlan, SqlTxn, SqlWrite},
    adapter::sql::runtime::SqlRuntimeBuilder,
    adapter::{AdapterError, AdapterResult, GuardState, SkipReason, StorageAdapter},
    event::Event,
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
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Sql, error)
    }
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

        let mut conn = match Connection::open(&self.path) {
            Ok(c) => c,
            Err(e) => return self.failure(AdapterError::WriteFailed(e.to_string())),
        };
        let tx = match conn.transaction() {
            Ok(tx) => tx,
            Err(e) => return self.failure(AdapterError::WriteFailed(e.to_string())),
        };

        match exec::project(SqliteTxn { tx }, &plan, event) {
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
// SqliteTxn — the driver
// ---------------------------------------------------------------------------

pub struct SqliteTxn<'a> {
    tx: Transaction<'a>,
}

fn sql_err(e: rusqlite::Error) -> SqlError {
    SqlError::ExecutionFailed(e.to_string())
}

impl SqlTxn for SqliteTxn<'_> {
    fn ensure_guard_table(&mut self) -> Result<(), SqlError> {
        self.tx
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

    fn read_guard_lane(
        &mut self,
        table: &str,
        entity_key: &str,
        facet: &str,
        _lock: bool, // SQLite writers already serialize; FOR UPDATE is a no-op.
    ) -> Result<Option<GuardState>, SqlError> {
        match self.tx.query_row(
            "SELECT last_sequence, deleted, permanent FROM conduit_projection_state
             WHERE target_table = ?1 AND entity_key = ?2 AND facet = ?3",
            params![table, entity_key, facet],
            |row| {
                Ok(GuardState {
                    last_sequence: row.get::<_, i64>(0)? as u64,
                    deleted: row.get::<_, i64>(1)? != 0,
                    permanent: row.get::<_, i64>(2)? != 0,
                })
            },
        ) {
            Ok(v) => Ok(Some(v)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(sql_err(e)),
        }
    }

    fn execute_write(&mut self, w: &SqlWrite) -> Result<(), SqlError> {
        let sql = w.render(Placeholders::Question);
        let binds: Vec<String> = w.values.iter().map(exec::scalar_to_string).collect();
        self.tx
            .execute(&sql, params_from_iter(binds.iter()))
            .map(|_| ())
            .map_err(sql_err)
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
        self.tx
            .execute(&sql, params_from_iter(binds.iter()))
            .map(|_| ())
            .map_err(sql_err)
    }

    fn upsert_guard_row(&mut self, row: &GuardRow) -> Result<(), SqlError> {
        self.tx
            .execute(
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
            .map(|_| ())
            .map_err(sql_err)
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
            .execute(
                "UPDATE conduit_projection_state
                    SET last_sequence = ?4, last_event_id = ?5, processed_at = datetime('now')
                 WHERE target_table = ?1 AND entity_key = ?2 AND facet = ?3",
                params![table, entity_key, facet, seq as i64, event_id],
            )
            .map(|_| ())
            .map_err(sql_err)
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
            self.tx.execute(
                "UPDATE conduit_projection_state
                    SET deleted = 1, permanent = ?4, last_sequence = ?3,
                        last_event_id = ?5, processed_at = datetime('now')
                 WHERE target_table = ?1 AND entity_key = ?2 AND facet <> ''",
                params![table, entity_key, seq as i64, permanent as i64, event_id],
            )
        } else {
            self.tx.execute(
                "UPDATE conduit_projection_state
                    SET deleted = 0, permanent = 0
                 WHERE target_table = ?1 AND entity_key = ?2 AND facet <> ''",
                params![table, entity_key],
            )
        }
        .map(|_| ())
        .map_err(sql_err)
    }

    fn commit(self) -> Result<(), SqlError> {
        self.tx.commit().map_err(sql_err)
    }
}
