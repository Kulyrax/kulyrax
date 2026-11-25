//! Kulyrax 0.12 named the guard table `conduit_projection_state`. The first
//! `prepare` renames it to `kulyrax_projection_state` and leaves a tombstone
//! view under the old name (`kulyrax_core::adapter::legacy`).

use mysql::PooledConn;
use mysql::prelude::Queryable;

use kulyrax_core::adapter::legacy;
use kulyrax_core::adapter::sql::adapter::SqlError;
use kulyrax_core::adapter::sql::exec;

use crate::my_err;

/// The tombstone view's temporary name, before the atomic swap.
const TOMBSTONE_TMP: &str = "kulyrax_tombstone_tmp";

/// MySQL DDL is not transactional, so the rename and the tombstone are one multi-table
/// `RENAME TABLE`, which is atomic: the tombstone view is created under a
/// temporary name first and swapped in with the rename. A named lock
/// serialises several processes upgrading one database.
pub(crate) fn migrate_guard_table(conn: &mut PooledConn) -> Result<(), SqlError> {
    let table_type = |conn: &mut PooledConn, name: &str| -> Result<Option<String>, SqlError> {
        conn.exec_first(
            "SELECT TABLE_TYPE FROM information_schema.TABLES
             WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?",
            (name,),
        )
        .map_err(my_err)
    };
    let is_table = |t: &Option<String>| t.as_deref() == Some("BASE TABLE");
    if !is_table(&table_type(conn, legacy::LEGACY_GUARD_TABLE)?) {
        return Ok(());
    }
    let locked: Option<i64> = conn
        .query_first("SELECT GET_LOCK('kulyrax_projection_state', 60)")
        .map_err(my_err)?;
    if locked != Some(1) {
        return Err(SqlError::ExecutionFailed(
            "timed out waiting for another Kulyrax process to migrate the guard table".to_string(),
        ));
    }
    let result = (|| {
        if !is_table(&table_type(conn, legacy::LEGACY_GUARD_TABLE)?) {
            return Ok(());
        }
        if table_type(conn, exec::GUARD_TABLE)?.is_some() {
            return Err(legacy::both_guard_tables("this MySQL database"));
        }
        conn.query_drop(format!("DROP VIEW IF EXISTS {TOMBSTONE_TMP}"))
            .map_err(my_err)?;
        conn.query_drop(legacy::tombstone_view_sql(TOMBSTONE_TMP))
            .map_err(my_err)?;
        kulyrax_core::crash::crash_point("guard-tombstone-created");
        conn.query_drop(format!(
            "RENAME TABLE {old} TO {new}, {TOMBSTONE_TMP} TO {old}",
            old = legacy::LEGACY_GUARD_TABLE,
            new = exec::GUARD_TABLE,
        ))
        .map_err(my_err)?;
        kulyrax_core::crash::crash_point("guard-table-renamed");
        Ok(())
    })();
    let _ = conn.query_drop("DO RELEASE_LOCK('kulyrax_projection_state')");
    result
}
