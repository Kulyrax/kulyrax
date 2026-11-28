//! The names Kulyrax 0.12 and earlier gave to what it persisted, and their
//! migration to the current ones. Not API: kept only so a store or database
//! last opened by 0.12 can be opened again (decision record
//! `docs/ledger/decisions/2026-11-25-rename-conduit.md`).
//!
//! | 0.12 | Now | Migrated by |
//! |---|---|---|
//! | `<root>/.conduit/` (file stores) | `<root>/.kulyrax/` | [`migrate_file_store`] |
//! | table / collection `conduit_projection_state` | `kulyrax_projection_state` | each SQL backend's `ensure_guard_table`; the MongoDB backend |
//! | Neo4j label `__ConduitGuard` | `__KulyraxGuard` | the Neo4j backend |
//!
//! Each migration renames in place once, refuses when both names exist, and
//! leaves a tombstone under the old name so that 0.12 fails loudly instead of
//! starting on an empty store.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::Path;

use crate::adapter::file_store::{
    remove_if_exists, replay_logs, sync_dir, take_lock, write_marker,
};
use crate::adapter::sql::adapter::SqlError;
use crate::crash::crash_point;
use crate::kulyrax_dir::{self, DIR, LAYOUT, LOCK, WAL};

/// A file store's internal directory in 0.12.
pub const LEGACY_DIR: &str = ".conduit";
/// Where a 0.12 `.conduit/` directory is parked once its entries have moved,
/// until the tombstone replaces it.
const LEGACY_PARKED: &str = ".kulyrax-tmp-conduit";
const TOMBSTONE_TMP: &str = ".kulyrax-tmp-tombstone";
/// The file left at `<root>/.conduit` after the migration. Kulyrax 0.12 then
/// fails to create its directories there instead of starting on an empty store.
const TOMBSTONE: &str = "Moved to .kulyrax/ by Kulyrax 1.0. Kulyrax 0.12 cannot open this store.\n";

/// The guard table's (and MongoDB guard collection's) name in 0.12.
pub const LEGACY_GUARD_TABLE: &str = "conduit_projection_state";

/// What the tombstone view returns, and the MongoDB tombstone's validator
/// description: why 0.12 cannot use this database.
pub const LEGACY_TOMBSTONE_TEXT: &str =
    "moved to kulyrax_projection_state by Kulyrax 1.0; Kulyrax 0.12 cannot use this database";

/// `CREATE VIEW <name> AS SELECT '<tombstone text>' AS moved`: a view that
/// cannot be written to and has none of the guard columns, left under the old
/// table name so a 0.12 process fails instead of starting on an empty table.
pub fn tombstone_view_sql(view: &str) -> String {
    format!("CREATE VIEW {view} AS SELECT '{LEGACY_TOMBSTONE_TEXT}' AS moved")
}

/// The error when the 0.12 guard table exists beside the current one.
pub fn both_guard_tables(where_: &str) -> SqlError {
    SqlError::ExecutionFailed(format!(
        "{where_} has both {LEGACY_GUARD_TABLE} (Kulyrax 0.12) and {}; a 0.12 process \
         wrote here after the upgrade, or the old table was restored. Keep the one you want, \
         drop or rename the other, and start again",
        crate::adapter::sql::exec::GUARD_TABLE
    ))
}

// ---------------------------------------------------------------------------
// Migration from Kulyrax 0.12 (`.conduit/`)
// ---------------------------------------------------------------------------

/// The layout markers 0.12.1 wrote in `.conduit/layout/`.
const MARKER_KINDS: &[&str] = &["kv", "document", "graph"];

/// Move a file store's internal files from `.conduit/` (Kulyrax 0.12) to
/// `.kulyrax/`, once. Runs with the new lock held, before anything is read.
///
/// 1. Take `.conduit/lock` too, so a 0.12 process on this store is refused.
/// 2. Replay `.conduit/wal/` under the old paths its entries name.
/// 3. Move every other entry into `.kulyrax/`, one atomic rename each. An
///    entry in both is refused, naming both. Entries already moved (a
///    migration killed halfway) are simply not there any more.
/// 4. Mark the layout version 2, then park `.conduit/` and leave the
///    tombstone file in its place, so 0.12 fails instead of starting empty.
///
/// Entries 0.12.1 never writes under `.conduit/` (0.12.0 key-value guards, in
/// `.conduit/<namespace>/`) are refused, not moved: a namespace could be
/// another owner's entry name.
pub fn migrate_file_store(dir: &Path, known_dirs: &mut HashSet<String>) -> io::Result<()> {
    let legacy = dir.join(LEGACY_DIR);
    let parked = dir.join(LEGACY_PARKED);
    let legacy_is_dir = fs::symlink_metadata(&legacy).is_ok_and(|m| m.is_dir());
    if parked.exists() {
        // Killed after parking, before the tombstone was in place.
        if legacy_is_dir {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "file store {}: both {} and {} exist; a Kulyrax 0.12 process wrote to the \
                     store during its migration. Keep the one you want and move the other aside",
                    dir.display(),
                    legacy.display(),
                    parked.display()
                ),
            ));
        }
        return finish_legacy(dir, &parked);
    }
    if !legacy_is_dir {
        // Never a 0.12 store, or migrated (`.conduit` is the tombstone file).
        return Ok(());
    }

    let old_lock_path = legacy.join(LOCK);
    let old_lock = if old_lock_path.is_file() {
        Some(take_lock(dir, &old_lock_path, false)?)
    } else {
        None
    };

    let old_wal = legacy.join(WAL);
    let mut foreign = Vec::new();
    if old_wal.is_dir() {
        replay_logs(dir, &old_wal, known_dirs)?;
        if fs::read_dir(&old_wal)?.next().is_none() {
            fs::remove_dir(&old_wal)?;
        } else {
            foreign.push(old_wal.display().to_string());
        }
    }

    let new_dir = dir.join(DIR);
    let mut moves = Vec::new();
    let mut conflicts = Vec::new();
    for entry in fs::read_dir(&legacy)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            foreign.push(entry.path().display().to_string());
            continue;
        };
        if name == LOCK || (name == WAL && !foreign.is_empty()) {
            continue;
        }
        let owned = kulyrax_dir::STORE_ENTRIES.contains(&name)
            && (name != LAYOUT || only_markers(&entry.path())?);
        if !owned {
            foreign.push(entry.path().display().to_string());
            continue;
        }
        let target = new_dir.join(name);
        if fs::symlink_metadata(&target).is_ok() {
            conflicts.push(format!(
                "both {} and {}",
                entry.path().display(),
                target.display()
            ));
            continue;
        }
        moves.push((entry.path(), target));
    }
    if !foreign.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "file store {} holds files under {} that Kulyrax 0.12.1 never writes there: {}. \
                 They are key-value guards from Kulyrax 0.12.0 or earlier. Open the store once \
                 with Kulyrax 0.12.1, which moves them, then with this version",
                dir.display(),
                legacy.display(),
                foreign.join(", ")
            ),
        ));
    }
    if !conflicts.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "file store {}: {}. Keep the one you want and move the other aside",
                dir.display(),
                conflicts.join("; ")
            ),
        ));
    }
    for (from, to) in moves {
        fs::rename(&from, &to)?;
        crash_point("legacy-move");
    }
    sync_dir(&new_dir)?;

    let markers = new_dir.join(LAYOUT);
    if markers.is_dir() {
        for entry in fs::read_dir(&markers)? {
            write_marker(&entry?.path())?;
        }
    }

    drop(old_lock);
    remove_if_exists(&old_lock_path)?;
    crash_point("legacy-park");
    fs::rename(&legacy, &parked)?;
    sync_dir(dir)?;
    crash_point("legacy-tombstone");
    finish_legacy(dir, &parked)
}

/// Write the tombstone at `<root>/.conduit` and delete the parked directory.
fn finish_legacy(dir: &Path, parked: &Path) -> io::Result<()> {
    let tmp = dir.join(TOMBSTONE_TMP);
    let mut f = File::create(&tmp)?;
    f.write_all(TOMBSTONE.as_bytes())?;
    f.sync_all()?;
    drop(f);
    fs::rename(&tmp, dir.join(LEGACY_DIR))?;
    sync_dir(dir)?;
    fs::remove_dir_all(parked)?;
    sync_dir(dir)
}

/// Whether `layout` holds only 0.12.1's layout markers (0.12.0 put a
/// key-value namespace named `layout` there).
fn only_markers(layout: &Path) -> io::Result<bool> {
    for entry in fs::read_dir(layout)? {
        let entry = entry?;
        let name = entry.file_name();
        let marker = entry.file_type()?.is_file()
            && name.to_str().is_some_and(|n| MARKER_KINDS.contains(&n));
        if !marker {
            return Ok(false);
        }
    }
    Ok(true)
}

/// SQLite: rename the 0.12 guard table and create the tombstone view, in the
/// caller's open transaction, so a kill leaves either the old state or the
/// new one. Called by the SQLite backend's `ensure_guard_table`.
pub fn migrate_sqlite_guard_table(conn: &rusqlite::Connection) -> Result<(), SqlError> {
    use rusqlite::OptionalExtension;
    let sql_err = |e: rusqlite::Error| SqlError::ExecutionFailed(e.to_string());
    let kind = |name: &str| -> Result<Option<String>, SqlError> {
        conn.query_row(
            "SELECT type FROM sqlite_master WHERE name = ?1 AND type IN ('table', 'view')",
            [name],
            |r| r.get(0),
        )
        .optional()
        .map_err(sql_err)
    };
    if kind(LEGACY_GUARD_TABLE)?.as_deref() != Some("table") {
        return Ok(());
    }
    if kind(crate::adapter::sql::exec::GUARD_TABLE)?.is_some() {
        return Err(both_guard_tables("this SQLite database"));
    }
    conn.execute_batch(&format!(
        "ALTER TABLE {LEGACY_GUARD_TABLE} RENAME TO {}; {};",
        crate::adapter::sql::exec::GUARD_TABLE,
        tombstone_view_sql(LEGACY_GUARD_TABLE)
    ))
    .map_err(sql_err)?;
    crash_point("guard-table-renamed");
    Ok(())
}
