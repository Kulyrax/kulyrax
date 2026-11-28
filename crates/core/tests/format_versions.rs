//! Step 5 (decision 2026-11-28-format-versions): persisted formats carry a
//! version. A newer version is refused, an unversioned (0.12.x) one is read
//! as format 1, and what this version writes reads back unchanged.

use kulyrax_core::format::{FormatError, FormatProblem};
use kulyrax_core::source::checkpoint::{
    CHECKPOINT_FORMAT, checkpoint_path, read_checkpoint, write_checkpoint,
};
use kulyrax_core::source::{SourceError, SourcePosition};

/// What Kulyrax 0.12.1 wrote (`source/checkpoint.rs` at `v0.12.1`): no
/// `format` field.
const CHECKPOINT_0_12_1: &str = r#"{
  "position": "events/0003.json#1",
  "committed_at": "1764000000"
}"#;

#[test]
fn a_checkpoint_round_trips_with_its_format() {
    let dir = tempfile::tempdir().expect("tempdir");
    let position = SourcePosition("events/0003.json#1".to_string());
    write_checkpoint(dir.path(), "in", &position).expect("write");
    let text = std::fs::read_to_string(checkpoint_path(dir.path(), "in")).expect("read");
    let value: serde_json::Value = serde_json::from_str(&text).expect("json");
    assert_eq!(value["format"], CHECKPOINT_FORMAT, "{text}");
    let back = read_checkpoint(dir.path(), "in")
        .expect("read")
        .expect("some");
    assert_eq!(back.format, CHECKPOINT_FORMAT);
    assert_eq!(back.position, position);
}

#[test]
fn a_0_12_1_checkpoint_reads_as_format_1_and_is_stamped_on_commit() {
    let dir = tempfile::tempdir().expect("tempdir");
    std::fs::write(checkpoint_path(dir.path(), "in"), CHECKPOINT_0_12_1).expect("write");
    let cp = read_checkpoint(dir.path(), "in")
        .expect("read")
        .expect("some");
    assert_eq!(cp.format, 1);
    assert_eq!(cp.position.as_str(), "events/0003.json#1");

    write_checkpoint(dir.path(), "in", &cp.position).expect("commit");
    let text = std::fs::read_to_string(checkpoint_path(dir.path(), "in")).expect("read");
    assert!(text.contains("\"format\": 1"), "{text}");
}

fn refused(content: &str) -> FormatError {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = checkpoint_path(dir.path(), "in");
    std::fs::write(&path, content).expect("write");
    let err = read_checkpoint(dir.path(), "in").expect_err("refused");
    assert_eq!(
        std::fs::read_to_string(&path).expect("kept"),
        content,
        "the checkpoint is left as it is"
    );
    match err {
        SourceError::Format(e) => {
            assert!(e.store.ends_with("in.json"), "{}", e.store);
            e
        }
        other => panic!("not a format refusal: {other}"),
    }
}

#[test]
fn a_newer_checkpoint_is_refused() {
    let e = refused(r#"{"format": 2, "position": "p", "committed_at": "0", "more": 1}"#);
    assert_eq!(
        e.problem,
        FormatProblem::Newer {
            found: 2,
            supported: CHECKPOINT_FORMAT
        }
    );
    assert!(e.to_string().contains("newer Kulyrax"), "{e}");
}

#[test]
fn a_checkpoint_with_an_unknown_format_is_refused() {
    for content in [
        r#"{"format": "one", "position": "p", "committed_at": "0"}"#,
        r#"{"format": 0, "position": "p", "committed_at": "0"}"#,
        r#"{"format": -1, "position": "p", "committed_at": "0"}"#,
    ] {
        let e = refused(content);
        assert!(
            matches!(e.problem, FormatProblem::Unrecognized(_)),
            "{content}: {e}"
        );
    }
}

// ---------------------------------------------------------------------------
// SQL guard store: the `kulyrax_format` marker (SQLite here; Postgres and
// MySQL in their crates' Docker suites).
// ---------------------------------------------------------------------------

mod sqlite_marker {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
    use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
    use kulyrax_core::adapter::{AdapterError, StorageAdapter};
    use kulyrax_core::format::FormatProblem;
    use kulyrax_core::runtime::config::MigrationPolicy;
    use kulyrax_core::upcast::UpcasterRegistry;

    fn adapter(db: &Path) -> SqliteAdapter {
        SqliteAdapter::new(
            "sql".to_string(),
            db.to_string_lossy().to_string(),
            10,
            SqlRuntimeBuilder::new(HashMap::new()),
            Arc::new(UpcasterRegistry::new()),
            MigrationPolicy::default(),
        )
    }

    fn markers(db: &Path) -> Vec<(String, i64)> {
        let c = rusqlite::Connection::open(db).expect("open");
        let mut stmt = c
            .prepare("SELECT component, version FROM kulyrax_format ORDER BY 1")
            .expect("marker table");
        stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .expect("query")
            .collect::<Result<_, _>>()
            .expect("rows")
    }

    fn has_table(db: &Path, name: &str) -> bool {
        rusqlite::Connection::open(db)
            .and_then(|c| {
                c.query_row(
                    "SELECT count(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
                    [name],
                    |r| r.get::<_, i64>(0),
                )
            })
            .expect("query")
            == 1
    }

    #[test]
    fn a_new_database_gets_one_marker() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("app.db");
        adapter(&db).prepare().expect("prepare");
        adapter(&db).prepare().expect("a second start");
        assert_eq!(markers(&db), [("guards".to_string(), 1)]);
    }

    /// A 0.12.x database: guard rows and no marker. It is format 1, stamped,
    /// its lanes kept.
    #[test]
    fn a_0_12_database_is_stamped_as_format_1() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("app.db");
        rusqlite::Connection::open(&db)
            .and_then(|c| {
                c.execute_batch(
                    "CREATE TABLE kulyrax_projection_state (
                        target_table TEXT NOT NULL, entity_key TEXT NOT NULL,
                        facet TEXT NOT NULL DEFAULT '', last_sequence INTEGER NOT NULL,
                        last_event_id TEXT NOT NULL, deleted INTEGER NOT NULL DEFAULT 0,
                        permanent INTEGER NOT NULL DEFAULT 0, processed_at TEXT NOT NULL,
                        PRIMARY KEY (target_table, entity_key, facet));
                     INSERT INTO kulyrax_projection_state
                        (target_table, entity_key, last_sequence, last_event_id, processed_at)
                        VALUES ('users', '[\"u1\"]', 7, 'e7', '0');",
                )
            })
            .expect("a 0.12 database");
        adapter(&db).prepare().expect("prepare");
        assert_eq!(markers(&db), [("guards".to_string(), 1)]);
        let lanes: i64 = rusqlite::Connection::open(&db)
            .and_then(|c| {
                c.query_row(
                    "SELECT last_sequence FROM kulyrax_projection_state",
                    [],
                    |r| r.get(0),
                )
            })
            .expect("lane");
        assert_eq!(lanes, 7);
    }

    #[test]
    fn a_newer_database_is_refused_before_the_guard_table() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("app.db");
        rusqlite::Connection::open(&db)
            .and_then(|c| {
                c.execute_batch(
                    "CREATE TABLE kulyrax_format (component TEXT PRIMARY KEY, version INTEGER NOT NULL);
                     INSERT INTO kulyrax_format VALUES ('guards', 2);",
                )
            })
            .expect("a newer database");
        let err = adapter(&db).prepare().expect_err("refused");
        let AdapterError::Format(e) = err else {
            panic!("not a format refusal: {err}");
        };
        assert_eq!(
            e.problem,
            FormatProblem::Newer {
                found: 2,
                supported: 1
            }
        );
        assert!(e.store.contains("app.db"), "{}", e.store);
        assert!(
            !has_table(&db, "kulyrax_projection_state"),
            "guard table untouched"
        );
        assert_eq!(markers(&db), [("guards".to_string(), 2)]);
    }

    /// Change 3: concurrent first starts leave one marker row, one version.
    #[test]
    fn concurrent_first_starts_leave_one_marker() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("app.db");
        let results: Vec<Result<(), String>> = std::thread::scope(|s| {
            let handles: Vec<_> = (0..8)
                .map(|_| s.spawn(|| adapter(&db).prepare().map_err(|e| e.to_string())))
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap_or_else(|_| Err("panicked".into())))
                .collect()
        });
        assert!(results.iter().all(Result::is_ok), "{results:?}");
        assert_eq!(markers(&db), [("guards".to_string(), 1)]);
    }

    /// Change 4: a database Kulyrax may not write refuses the start, naming
    /// the store and the right it needs.
    #[test]
    fn a_read_only_database_names_the_grant() {
        let dir = tempfile::tempdir().expect("tempdir");
        let db = dir.path().join("app.db");
        rusqlite::Connection::open(&db)
            .and_then(|c| c.execute_batch("CREATE TABLE users (id TEXT PRIMARY KEY)"))
            .expect("a database");
        let mut perms = std::fs::metadata(&db).expect("meta").permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&db, perms.clone()).expect("read-only");
        let err = adapter(&db).prepare().expect_err("refused");
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        std::fs::set_permissions(&db, perms).expect("writable again");
        let AdapterError::Format(e) = err else {
            panic!("not a format refusal: {err}");
        };
        let FormatProblem::MarkerNotCreated { grant, .. } = &e.problem else {
            panic!("not MarkerNotCreated: {e}");
        };
        assert!(grant.contains("write access"), "{grant}");
        assert!(e.to_string().contains("app.db"), "{e}");

        // With its marker in place, a read-only database is an ordinary
        // error: the marker is not what is missing.
        adapter(&db)
            .prepare()
            .expect("writable: prepare stamps the marker");
        let mut perms = std::fs::metadata(&db).expect("meta").permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&db, perms.clone()).expect("read-only");
        let err = adapter(&db).prepare().expect_err("still refused");
        #[allow(clippy::permissions_set_readonly_false)]
        perms.set_readonly(false);
        std::fs::set_permissions(&db, perms).expect("writable again");
        assert!(
            matches!(err, AdapterError::WriteFailed(_)),
            "not a marker refusal: {err}"
        );
    }
}

/// Mappings built in code (not loaded from files) that target Kulyrax's own
/// tables are refused by the SQLite adapter's `prepare`, before the database
/// is opened.
#[test]
fn sqlite_prepare_refuses_a_mapping_on_kulyrax_tables() {
    use std::collections::HashMap;
    use std::sync::Arc;

    use kulyrax_core::adapter::StorageAdapter;
    use kulyrax_core::adapter::sql::mapping::SqlMapping;
    use kulyrax_core::adapter::sql::runtime::SqlRuntimeBuilder;
    use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
    use kulyrax_core::runtime::config::MigrationPolicy;
    use kulyrax_core::upcast::UpcasterRegistry;

    let dir = tempfile::tempdir().expect("tempdir");
    let db = dir.path().join("app.db");
    for table in [
        "kulyrax_format",
        "Kulyrax_Projection_State",
        "conduit_projection_state",
    ] {
        let m: SqlMapping = kulyrax_core::yaml::from_str(&format!(
            "event: U\ntable: {table}\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n"
        ))
        .expect("mapping");
        let a = SqliteAdapter::new(
            "sql".into(),
            db.to_string_lossy().to_string(),
            10,
            SqlRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
            Arc::new(UpcasterRegistry::new()),
            MigrationPolicy::default(),
        );
        let err = a.prepare().expect_err("refused").to_string();
        assert!(
            err.contains("Kulyrax keeps for itself") && err.contains(table),
            "{err}"
        );
    }
    assert!(!db.exists(), "refused before the database was opened");
}
