//! Neo4j test isolation: a test binary owns the whole database.
//!
//! Neo4j Community has a single database, and the guard-label migration
//! relabels every guard node in it, so namespacing by label is not enough.
//! Each test takes a [`Neo4jLease`]:
//! - one test at a time in this process (a mutex), and one test process at a
//!   time per target directory (an OS file lock in `CARGO_TARGET_TMPDIR`, held
//!   until the process exits);
//! - the database is wiped before the test and again after it: every node, and
//!   every constraint and index named `kulyrax_…` or `conduit_…` or on a guard
//!   label;
//! - after the first wipe, a guard node of either name fails the test loudly
//!   instead of skewing its result.
//!
//! Locally, run one Neo4j container per branch (CONTRIBUTING.md, "Testing");
//! never point two branches at one Neo4j.

use std::fs::File;
use std::sync::{Mutex, MutexGuard, OnceLock};

type E = Box<dyn std::error::Error>;

static SERIAL: Mutex<()> = Mutex::new(());
static PROCESS_LOCK: OnceLock<File> = OnceLock::new();

/// The guard labels of 0.12 (`conduit`) and of this version.
const GUARD_LABELS: [&str; 2] = ["__ConduitGuard", "__KulyraxGuard"];

pub fn user() -> String {
    std::env::var("KULYRAX_NEO4J_USER").unwrap_or_else(|_| "neo4j".into())
}

pub fn password() -> String {
    std::env::var("KULYRAX_NEO4J_PASSWORD").unwrap_or_else(|_| "kulyrax-test".into())
}

/// Exclusive use of the Neo4j database for one test. Dropping it wipes the
/// database.
pub struct Neo4jLease {
    uri: String,
    _serial: MutexGuard<'static, ()>,
}

impl Neo4jLease {
    pub fn uri(&self) -> &str {
        &self.uri
    }
}

impl Drop for Neo4jLease {
    fn drop(&mut self) {
        if let Err(e) = wipe(&self.uri) {
            eprintln!("warning: wiping Neo4j after the test failed: {e}");
        }
    }
}

/// The database to oneself, wiped, or `None` (skip) when `KULYRAX_NEO4J_URI`
/// is unset.
pub fn lease() -> Option<Neo4jLease> {
    let uri = std::env::var("KULYRAX_NEO4J_URI")
        .ok()
        .filter(|s| !s.trim().is_empty())?;
    let serial = SERIAL.lock().unwrap_or_else(|e| e.into_inner());
    PROCESS_LOCK.get_or_init(|| {
        let path = std::path::Path::new(env!("CARGO_TARGET_TMPDIR")).join("kulyrax-neo4j.lock");
        let file = File::options()
            .create(true)
            .write(true)
            .truncate(false)
            .open(&path)
            .unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
        file.lock()
            .unwrap_or_else(|e| panic!("lock {}: {e}", path.display()));
        file
    });
    wipe(&uri).unwrap_or_else(|e| panic!("wipe Neo4j before the test: {e}"));
    let guards = guard_nodes(&uri).unwrap_or_else(|e| panic!("count Neo4j guard nodes: {e}"));
    assert_eq!(
        guards, 0,
        "Neo4j still holds {guards} guard nodes after the wipe: another run is using this database"
    );
    Some(Neo4jLease {
        uri,
        _serial: serial,
    })
}

fn runtime() -> Result<tokio::runtime::Runtime, E> {
    Ok(tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?)
}

/// Every node; then every constraint and index Kulyrax (or a test) made.
fn wipe(uri: &str) -> Result<(), E> {
    let rt = runtime()?;
    rt.block_on(async {
        let graph = neo4rs::Graph::new(uri, user(), password()).await?;
        graph
            .run(neo4rs::Query::new("MATCH (n) DETACH DELETE n".into()))
            .await?;
        let ours = |name: &str, labels: &[String]| {
            name.starts_with("kulyrax_")
                || name.starts_with("conduit_")
                || labels.iter().any(|l| GUARD_LABELS.contains(&l.as_str()))
        };
        for (show, drop) in [
            ("SHOW CONSTRAINTS YIELD name, labelsOrTypes", "CONSTRAINT"),
            (
                "SHOW INDEXES YIELD name, labelsOrTypes, owningConstraint, type \
                 WHERE owningConstraint IS NULL AND type <> 'LOOKUP'",
                "INDEX",
            ),
        ] {
            let mut rows = graph
                .execute(neo4rs::Query::new(format!(
                    "{show} RETURN name, labelsOrTypes"
                )))
                .await?;
            let mut names = Vec::new();
            while let Some(row) = rows.next().await? {
                let name: String = row.get("name")?;
                let labels: Vec<String> = row.get("labelsOrTypes").unwrap_or_default();
                if ours(&name, &labels) {
                    names.push(name);
                }
            }
            for name in names {
                graph
                    .run(neo4rs::Query::new(format!(
                        "DROP {drop} `{}` IF EXISTS",
                        name.replace('`', "``")
                    )))
                    .await?;
            }
        }
        Ok(())
    })
}

fn guard_nodes(uri: &str) -> Result<i64, E> {
    let rt = runtime()?;
    rt.block_on(async {
        let graph = neo4rs::Graph::new(uri, user(), password()).await?;
        let mut rows = graph
            .execute(neo4rs::Query::new(
                "MATCH (g) WHERE g:__ConduitGuard OR g:__KulyraxGuard RETURN count(g) AS n".into(),
            ))
            .await?;
        let n = match rows.next().await? {
            Some(row) => row.get::<i64>("n")?,
            None => 0,
        };
        Ok(n)
    })
}
