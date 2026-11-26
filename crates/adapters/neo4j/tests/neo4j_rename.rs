//! A Neo4j database last used by Kulyrax 0.12 (guard label
//! `__ConduitGuard`) opens under this version: relabelled in batches, every
//! guard lane kept, the old label's constraint dropped, a lane under both
//! labels refused.
//!
//! **Gated on `KULYRAX_NEO4J_URI`.** One test, in steps: Neo4j has one shared
//! database, and the migration moves every node with the old label, so the
//! steps must not run concurrently with each other. Each step's lanes use a
//! label of their own. The test owns the whole database while it runs
//! (`common::lease`).

mod common;

use std::collections::HashMap;
use std::sync::Arc;

use kulyrax_adapter_neo4j::Neo4jAdapter;
use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::runtime::GraphRuntimeBuilder;
use kulyrax_core::adapter::{AdapterOutcome, SkipReason, StorageAdapter};
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

type R = Result<(), Box<dyn std::error::Error>>;

fn user() -> String {
    common::user()
}

fn password() -> String {
    common::password()
}

fn adapter(uri: &str, label: &str) -> Neo4jAdapter {
    let m: GraphMapping = kulyrax_core::yaml::from_str(&format!(
        "kind: node\nevent: U\nlabel: {label}\nkey: payload.id\nversion: 1\non_existing: replace\nproperties:\n  v: payload.v\n"
    ))
    .unwrap();
    Neo4jAdapter::new(
        "neo4j".into(),
        uri,
        &user(),
        &password(),
        None,
        10,
        GraphRuntimeBuilder::new(HashMap::from([("U".to_string(), m)])),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn ev(id: &str, seq: u64) -> Event {
    Event::new(
        format!("e-{id}-{seq}"),
        "U",
        seq,
        serde_json::json!({ "id": id, "v": seq }).to_string(),
    )
}

fn stale(o: &AdapterOutcome) -> bool {
    matches!(o, AdapterOutcome::Skipped(SkipReason::StaleSequence))
}

struct Db {
    rt: tokio::runtime::Runtime,
    graph: neo4rs::Graph,
}

impl Db {
    fn connect(uri: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Runtime::new()?;
        let graph = rt.block_on(neo4rs::Graph::new(uri, user(), password()))?;
        Ok(Self { rt, graph })
    }

    fn run(&self, q: &str) -> R {
        self.rt
            .block_on(self.graph.run(neo4rs::Query::new(q.to_string())))?;
        Ok(())
    }

    fn count(&self, q: &str) -> Result<i64, Box<dyn std::error::Error>> {
        self.rt.block_on(async {
            let mut rows = self
                .graph
                .execute(neo4rs::Query::new(q.to_string()))
                .await?;
            let row = rows.next().await?.ok_or("no row")?;
            Ok(row.get::<i64>("n")?)
        })
    }
}

/// Lanes for `label` written by this version, then relabelled as 0.12 had them.
fn as_0_12_1(db: &Db, uri: &str, label: &str, ids: &[&str]) -> R {
    let a = adapter(uri, label);
    a.prepare()?;
    for id in ids {
        let o = a.handle(&ev(id, 5)).outcome;
        assert!(matches!(o, AdapterOutcome::Created), "{o:?}");
    }
    drop(a);
    db.run(&format!(
        "MATCH (g:__KulyraxGuard {{target: '{label}'}}) SET g:__ConduitGuard REMOVE g:__KulyraxGuard"
    ))
}

#[test]
fn a_0_12_1_neo4j_database_is_relabelled() -> R {
    // The whole database, wiped before and after (`common::lease`).
    let Some(lease) = common::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return Ok(());
    };
    let uri = lease.uri().to_string();
    let db = Db::connect(&uri)?;
    let pid = std::process::id();

    // 1. Every lane kept, in several batches, and the old constraint dropped.
    let label = format!("Rename{pid}A");
    let ids: Vec<String> = (0..7).map(|i| format!("u{i}")).collect();
    let ids: Vec<&str> = ids.iter().map(String::as_str).collect();
    as_0_12_1(&db, &uri, &label, &ids)?;
    db.run(
        "CREATE CONSTRAINT IF NOT EXISTS FOR (g:__ConduitGuard) \
         REQUIRE (g.kind, g.target, g.entity_key, g.facet) IS UNIQUE",
    )?;
    // Three nodes per relabel batch, so seven lanes take three batches. SAFETY:
    // this test binary runs one test, so no other thread reads the environment.
    unsafe { std::env::set_var("KULYRAX_TEST_RELABEL_BATCH", "3") };
    let a = adapter(&uri, &label);
    a.prepare()?;
    for id in &ids {
        let o = a.handle(&ev(id, 2)).outcome;
        assert!(stale(&o), "{id}: {o:?}");
    }
    assert_eq!(
        db.count("MATCH (g:__ConduitGuard) RETURN count(g) AS n")?,
        0
    );
    assert_eq!(
        db.count(&format!(
            "MATCH (g:__KulyraxGuard {{target: '{label}'}}) RETURN count(g) AS n"
        ))?,
        7
    );
    assert_eq!(
        db.count(
            "SHOW CONSTRAINTS YIELD labelsOrTypes WHERE '__ConduitGuard' IN labelsOrTypes \
             RETURN count(*) AS n"
        )?,
        0
    );
    drop(a);
    // A second start is a no-op.
    let a = adapter(&uri, &label);
    a.prepare()?;
    assert!(stale(&a.handle(&ev("u0", 3)).outcome));
    drop(a);

    // 2. A lane under both labels is refused, and nothing moves.
    let label = format!("Rename{pid}B");
    as_0_12_1(&db, &uri, &label, &["k1"])?;
    db.run(&format!(
        "CREATE (:__KulyraxGuard {{kind: 'node', target: '{label}', entity_key: 'k1', facet: '', \
         last_sequence: 1, last_event_id: 'x', deleted: false, permanent: false}})"
    ))?;
    let err = adapter(&uri, &label).prepare().err().map(|e| e.to_string());
    assert!(
        err.as_deref()
            .is_some_and(|e| e.contains("both __ConduitGuard") && e.contains("k1")),
        "{err:?}"
    );
    assert_eq!(
        db.count("MATCH (g:__ConduitGuard) RETURN count(g) AS n")?,
        1
    );
    db.run(&format!(
        "MATCH (g {{target: '{label}'}}) WHERE g:__ConduitGuard OR g:__KulyraxGuard DETACH DELETE g"
    ))?;
    db.run(&format!("MATCH (n:{label}) DETACH DELETE n"))?;
    db.run(&format!("MATCH (n:Rename{pid}A) DETACH DELETE n"))?;
    db.run(&format!(
        "MATCH (g:__KulyraxGuard) WHERE g.target STARTS WITH 'Rename{pid}' DETACH DELETE g"
    ))?;
    Ok(())
}
