//! Step 5's format marker on Neo4j (decision 2026-11-28-format-versions): one
//! `:__KulyraxFormat {component: 'guards', version}` node, unique on
//! `component`, created by `MERGE` so concurrent first starts leave one; a
//! newer or unrecognised one is refused before the guard constraint is
//! created.
//!
//! The refusal of a user who may not create the marker
//! (`Security.Forbidden`) is not tested: the local and CI Neo4j are Community
//! editions, which have no role-based access control.
//!
//! **Gated on `KULYRAX_NEO4J_URI`.** Each test leases the whole database
//! (`common::lease`), wiped before and after.

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use kulyrax_adapter_neo4j::Neo4jAdapter;
use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::runtime::GraphRuntimeBuilder;
use kulyrax_core::adapter::{AdapterError, StorageAdapter};
use kulyrax_core::format::FormatProblem;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

type R = Result<(), Box<dyn std::error::Error>>;

fn adapter(uri: &str) -> Neo4jAdapter {
    let m: GraphMapping = kulyrax_core::yaml::from_str(
        "kind: node\nevent: U\nlabel: FormatUser\nkey: payload.id\nversion: 1\nproperties:\n  v: payload.v\n",
    )
    .expect("mapping");
    Neo4jAdapter::new(
        "neo4j".into(),
        uri,
        &common::user(),
        &common::password(),
        None,
        10,
        GraphRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

struct Db {
    rt: tokio::runtime::Runtime,
    graph: neo4rs::Graph,
}

impl Db {
    fn connect(uri: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Runtime::new()?;
        let graph = rt.block_on(neo4rs::Graph::new(uri, common::user(), common::password()))?;
        Ok(Self { rt, graph })
    }

    fn run(&self, q: &str) -> R {
        self.rt
            .block_on(self.graph.run(neo4rs::Query::new(q.to_string())))?;
        Ok(())
    }

    /// `(component, version)` of every marker node; `version` as text.
    fn markers(&self) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
        self.rt.block_on(async {
            let mut rows = self
                .graph
                .execute(neo4rs::Query::new(
                    "MATCH (f:__KulyraxFormat) RETURN f.component AS c, toString(f.version) AS v \
                     ORDER BY c"
                        .to_string(),
                ))
                .await?;
            let mut out = Vec::new();
            while let Some(row) = rows.next().await? {
                out.push((row.get::<String>("c")?, row.get::<String>("v")?));
            }
            Ok(out)
        })
    }

    fn constraint_exists(&self, name: &str) -> Result<bool, Box<dyn std::error::Error>> {
        self.rt.block_on(async {
            let mut rows = self
                .graph
                .execute(
                    neo4rs::Query::new(
                        "SHOW CONSTRAINTS YIELD name WHERE name = $name RETURN count(*) AS n"
                            .to_string(),
                    )
                    .param("name", name),
                )
                .await?;
            let row = rows.next().await?.ok_or("no row")?;
            Ok(row.get::<i64>("n")? > 0)
        })
    }
}

fn one_marker() -> Vec<(String, String)> {
    vec![("guards".to_string(), "1".to_string())]
}

#[test]
fn a_new_database_gets_one_marker() -> R {
    let Some(lease) = common::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return Ok(());
    };
    let db = Db::connect(lease.uri())?;
    adapter(lease.uri()).prepare()?;
    adapter(lease.uri()).prepare()?;
    assert_eq!(db.markers()?, one_marker());
    assert!(db.constraint_exists("kulyrax_format_component")?);
    assert!(db.constraint_exists("kulyrax_guard_key")?);
    Ok(())
}

/// A database with guards and no marker (0.12.x): format 1, stamped.
#[test]
fn a_0_12_database_is_stamped_as_format_1() -> R {
    let Some(lease) = common::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return Ok(());
    };
    let db = Db::connect(lease.uri())?;
    db.run(
        "CREATE (:__KulyraxGuard {kind: 'node', target: 'FormatUser', entity_key: 'u1', \
         facet: '', last_sequence: 7, deleted: false, permanent: false})",
    )?;
    adapter(lease.uri()).prepare()?;
    assert_eq!(db.markers()?, one_marker());
    Ok(())
}

#[test]
fn a_newer_or_unknown_marker_is_refused_before_the_guard_constraint() -> R {
    let Some(lease) = common::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return Ok(());
    };
    let db = Db::connect(lease.uri())?;
    for (version, newer) in [("2", true), ("'two'", false)] {
        db.run("MATCH (f:__KulyraxFormat) DELETE f")?;
        db.run(&format!(
            "CREATE (:__KulyraxFormat {{component: 'guards', version: {version}}})"
        ))?;
        let err = adapter(lease.uri()).prepare().expect_err("refused");
        let AdapterError::Format(e) = err else {
            panic!("not a format refusal: {err}");
        };
        assert_eq!(
            matches!(
                e.problem,
                FormatProblem::Newer {
                    found: 2,
                    supported: 1
                }
            ),
            newer,
            "{e}"
        );
        assert!(e.store.starts_with("Neo4j "), "{}", e.store);
        assert!(
            !db.constraint_exists("kulyrax_guard_key")?,
            "guards untouched"
        );
    }
    Ok(())
}

/// Change 3: concurrent first starts leave one marker, one version.
#[test]
fn concurrent_first_starts_leave_one_marker() -> R {
    let Some(lease) = common::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return Ok(());
    };
    let uri = lease.uri().to_string();
    let results: Vec<Result<(), String>> = std::thread::scope(|s| {
        let handles: Vec<_> = (0..8)
            .map(|_| s.spawn(|| adapter(&uri).prepare().map_err(|e| e.to_string())))
            .collect();
        handles
            .into_iter()
            .map(|h| h.join().unwrap_or_else(|_| Err("panicked".into())))
            .collect()
    });
    assert!(results.iter().all(Result::is_ok), "{results:?}");
    assert_eq!(Db::connect(&uri)?.markers()?, one_marker());
    Ok(())
}
