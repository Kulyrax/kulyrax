//! Kulyrax 0.12 named the guard table `conduit_projection_state`. The first
//! `prepare` renames it (and its primary key's index) to
//! `kulyrax_projection_state` and leaves a tombstone view under the old name
//! (`kulyrax_core::adapter::legacy`).

use kulyrax_core::adapter::legacy;
use kulyrax_core::adapter::sql::adapter::SqlError;
use kulyrax_core::adapter::sql::exec;

use crate::pg_err;

/// Rename, re-index and tombstone in the caller's open transaction: a kill
/// leaves the old state or the new one. The advisory lock serialises several
/// processes upgrading one database.
pub(crate) fn migrate_guard_table(client: &mut postgres::Client) -> Result<(), SqlError> {
    let relkind = |client: &mut postgres::Client, name: &str| -> Result<Option<i8>, SqlError> {
        client
            .query_opt(
                "SELECT c.relkind FROM pg_class c WHERE c.oid = to_regclass($1)",
                &[&name],
            )
            .map(|row| row.map(|r| r.get::<_, i8>(0)))
            .map_err(pg_err)
    };
    let is_table = |kind: Option<i8>| kind == Some(b'r' as i8);
    if !is_table(relkind(client, legacy::LEGACY_GUARD_TABLE)?) {
        return Ok(());
    }
    client
        .batch_execute("SELECT pg_advisory_xact_lock(hashtext('kulyrax_projection_state'))")
        .map_err(pg_err)?;
    // Re-read under the lock: another process may have migrated meanwhile.
    if !is_table(relkind(client, legacy::LEGACY_GUARD_TABLE)?) {
        return Ok(());
    }
    if relkind(client, exec::GUARD_TABLE)?.is_some() {
        return Err(legacy::both_guard_tables("this Postgres database"));
    }
    client
        .batch_execute(&format!(
            "ALTER TABLE {old} RENAME TO {new};
             ALTER INDEX IF EXISTS {old}_pkey RENAME TO {new}_pkey;
             {tombstone};",
            old = legacy::LEGACY_GUARD_TABLE,
            new = exec::GUARD_TABLE,
            tombstone = legacy::tombstone_view_sql(legacy::LEGACY_GUARD_TABLE)
        ))
        .map_err(pg_err)?;
    kulyrax_core::crash::crash_point("guard-table-renamed");
    Ok(())
}
