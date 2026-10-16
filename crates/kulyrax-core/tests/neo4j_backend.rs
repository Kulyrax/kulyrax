//! Phase 24.6: Neo4j backend integration scenarios.
//!
//! **Gated on `KULYRAX_NEO4J_URI`** (`bolt://host:port`, plus
//! `KULYRAX_NEO4J_USER`/`KULYRAX_NEO4J_PASSWORD`, defaulting to `neo4j`/
//! `kulyrax-test`). When unset every test prints a skip line and returns
//! `Ok` — the hermetic file-backed graph suite stays the default and CI
//! without a Neo4j server stays green. Point it at a throwaway server
//! (`docker run --rm -p 7687:7687 -e NEO4J_AUTH=neo4j/kulyrax-test neo4j:5`)
//! to actually exercise these.
//!
//! Consolidated into one file, mirroring `pg_backend.rs` (Phase 19.6),
//! `redis_backend.rs` (Phase 22.5), and `mongo_backend.rs` (Phase 23.5) —
//! every scenario shares the same skip guard and fresh-label helper.

use kulyrax_core::adapter::AdapterOutcome;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::graph::neo4j::Neo4jAdapter;
use kulyrax_core::adapter::graph::runtime::GraphRuntimeBuilder;
use kulyrax_core::adapter::graph::store::GraphStore;
use kulyrax_core::event::Event;
use kulyrax_core::runtime::config::MigrationPolicy;
use kulyrax_core::upcast::UpcasterRegistry;

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

type R = Result<(), Box<dyn std::error::Error>>;

fn neo4j_uri() -> Option<String> {
    std::env::var("KULYRAX_NEO4J_URI")
        .ok()
        .filter(|s| !s.trim().is_empty())
}

fn neo4j_user() -> String {
    std::env::var("KULYRAX_NEO4J_USER").unwrap_or_else(|_| "neo4j".into())
}

fn neo4j_password() -> String {
    std::env::var("KULYRAX_NEO4J_PASSWORD").unwrap_or_else(|_| "kulyrax-test".into())
}

macro_rules! require_neo4j {
    ($uri:ident) => {
        let Some($uri) = neo4j_uri() else {
            eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
            return Ok(());
        };
    };
}

static LABEL_SEQ: AtomicU32 = AtomicU32::new(0);

/// A unique node label / edge type per test invocation, so the shared
/// database stays isolated (the Neo4j analog of `pg_backend.rs`'s
/// `fresh_table`).
fn fresh_label() -> String {
    format!("T{}", LABEL_SEQ.fetch_add(1, Ordering::Relaxed))
}

/// A throwaway tokio runtime for direct verification queries — the same
/// bridge the adapter itself uses internally, kept separate here so the
/// test doesn't reach into adapter internals.
fn verify_rt() -> tokio::runtime::Runtime {
    tokio::runtime::Runtime::new().expect("build a tokio runtime for verification queries")
}

fn connect(rt: &tokio::runtime::Runtime, uri: &str) -> Result<neo4rs::Graph, neo4rs::Error> {
    rt.block_on(neo4rs::Graph::new(uri, neo4j_user(), neo4j_password()))
}

fn query_one_row(
    rt: &tokio::runtime::Runtime,
    graph: &neo4rs::Graph,
    q: neo4rs::Query,
) -> Option<neo4rs::Row> {
    rt.block_on(async {
        let mut stream = graph.execute(q).await.ok()?;
        stream.next().await.ok().flatten()
    })
}

fn neo4j_adapter(uri: &str, mappings: HashMap<String, GraphMapping>) -> Neo4jAdapter {
    Neo4jAdapter::new(
        "neo4j".into(),
        uri,
        &neo4j_user(),
        &neo4j_password(),
        None,
        10,
        GraphRuntimeBuilder::new(mappings),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn graph_mapping(yaml: &str) -> GraphMapping {
    serde_yaml::from_str(yaml).unwrap()
}

fn ev(event_type: &str, seq: u64, payload: serde_json::Value) -> Event {
    Event {
        event_id: format!("evt-{event_type}-{seq}"),
        event_type: event_type.into(),
        payload: payload.to_string(),
        metadata: HashMap::new(),
        version: 1,
        sequence: seq,
    }
}

// ---------------------------------------------------------------------------

#[test]
fn neo4j_node_lifecycle() -> R {
    require_neo4j!(uri);
    let label = fresh_label();

    let mut m = HashMap::new();
    m.insert(
        "C".into(),
        graph_mapping(&format!(
            "kind: node\nevent: C\nlabel: {label}\nversion: 1\nkey: payload.id\non_existing: replace\nproperties:\n  name: payload.name\n"
        )),
    );
    m.insert(
        "D".into(),
        graph_mapping(&format!(
            "kind: node\nevent: D\nlabel: {label}\nversion: 1\nkey: payload.id\noperation: delete\n"
        )),
    );
    let a = neo4j_adapter(&uri, m);

    let c = a.handle(&ev(
        "C",
        1,
        serde_json::json!({ "id": "n1", "name": "Ada" }),
    ));
    assert!(
        !matches!(c.outcome, AdapterOutcome::Failed(_)),
        "{:?}",
        c.outcome
    );
    let u = a.handle(&ev(
        "C",
        2,
        serde_json::json!({ "id": "n1", "name": "Ada L" }),
    ));
    assert!(
        matches!(u.outcome, AdapterOutcome::Updated),
        "{:?}",
        u.outcome
    );
    let d = a.handle(&ev("D", 3, serde_json::json!({ "id": "n1" })));
    assert!(
        matches!(d.outcome, AdapterOutcome::Deleted),
        "{:?}",
        d.outcome
    );

    let rt = verify_rt();
    let graph = connect(&rt, &uri)?;
    let found = query_one_row(
        &rt,
        &graph,
        neo4rs::Query::new(format!("MATCH (n:`{label}` {{id: 'n1'}}) RETURN n")),
    );
    assert!(found.is_none(), "node must be absent after delete");

    let guard = query_one_row(
        &rt,
        &graph,
        neo4rs::Query::new(
            "MATCH (g:__ConduitGuard {kind:'node', target:$label, entity_key:'n1', facet:''}) \
             RETURN g.last_sequence AS seq, g.deleted AS deleted"
                .to_string(),
        )
        .param("label", label.as_str()),
    )
    .expect("guard node must exist");
    assert_eq!(guard.get::<i64>("seq")?, 3);
    assert!(guard.get::<bool>("deleted")?);
    Ok(())
}

#[test]
fn neo4j_edge_out_of_order() -> R {
    require_neo4j!(uri);
    let etype = fresh_label();

    let mut m = HashMap::new();
    m.insert(
        "E".into(),
        graph_mapping(&format!(
            "kind: edge\nevent: E\nedge_type: {etype}\nversion: 1\nfrom: payload.from\nto: payload.to\non_existing: replace\nproperties:\n  weight: payload.weight\n"
        )),
    );
    let a = neo4j_adapter(&uri, m);

    for seq in [3u64, 1, 2] {
        let r = a.handle(&ev(
            "E",
            seq,
            serde_json::json!({ "from": "a1", "to": "a2", "weight": format!("w{seq}") }),
        ));
        assert!(
            !matches!(r.outcome, AdapterOutcome::Failed(_)),
            "{:?}",
            r.outcome
        );
    }

    let rt = verify_rt();
    let graph = connect(&rt, &uri)?;
    let row = query_one_row(
        &rt,
        &graph,
        neo4rs::Query::new(format!(
            "MATCH ()-[r:`{etype}`]-() RETURN r.weight AS weight LIMIT 1"
        )),
    )
    .expect("edge must exist");
    assert_eq!(
        row.get::<String>("weight")?,
        "w3",
        "highest sequence wins regardless of arrival order"
    );
    Ok(())
}

#[test]
fn neo4j_detach_delete() -> R {
    require_neo4j!(uri);
    let label = fresh_label();
    let etype = fresh_label();

    let mut m = HashMap::new();
    m.insert(
        "N".into(),
        graph_mapping(&format!(
            "kind: node\nevent: N\nlabel: {label}\nversion: 1\nkey: payload.id\nproperties:\n  name: payload.id\n"
        )),
    );
    m.insert(
        "ND".into(),
        graph_mapping(&format!(
            "kind: node\nevent: ND\nlabel: {label}\nversion: 1\nkey: payload.id\noperation: delete\n"
        )),
    );
    m.insert(
        "E".into(),
        graph_mapping(&format!(
            "kind: edge\nevent: E\nedge_type: {etype}\nversion: 1\nfrom: payload.from\nto: payload.to\ndiscriminator: payload.d\nproperties: {{}}\n"
        )),
    );
    let a = neo4j_adapter(&uri, m);

    a.handle(&ev("N", 1, serde_json::json!({ "id": "center" })));
    a.handle(&ev("N", 2, serde_json::json!({ "id": "leaf1" })));
    a.handle(&ev("N", 3, serde_json::json!({ "id": "leaf2" })));
    a.handle(&ev("N", 4, serde_json::json!({ "id": "leaf3" })));
    for (seq, leaf, d) in [(5u64, "leaf1", "1"), (6, "leaf2", "2"), (7, "leaf3", "3")] {
        let r = a.handle(&ev(
            "E",
            seq,
            serde_json::json!({ "from": "center", "to": leaf, "d": d }),
        ));
        assert!(
            !matches!(r.outcome, AdapterOutcome::Failed(_)),
            "{:?}",
            r.outcome
        );
    }

    let rt = verify_rt();
    let graph = connect(&rt, &uri)?;
    let before = query_one_row(
        &rt,
        &graph,
        neo4rs::Query::new(format!("MATCH ()-[r:`{etype}`]-() RETURN count(r) AS c")),
    )
    .expect("count query must return a row");
    assert_eq!(
        before.get::<i64>("c")?,
        6,
        "3 edges, matched from both directions"
    );

    let del = a.handle(&ev("ND", 8, serde_json::json!({ "id": "center" })));
    assert!(
        matches!(del.outcome, AdapterOutcome::Deleted),
        "{:?}",
        del.outcome
    );

    let node = query_one_row(
        &rt,
        &graph,
        neo4rs::Query::new(format!("MATCH (n:`{label}` {{id: 'center'}}) RETURN n")),
    );
    assert!(node.is_none(), "center node must be gone");

    let after = query_one_row(
        &rt,
        &graph,
        neo4rs::Query::new(format!("MATCH ()-[r:`{etype}`]-() RETURN count(r) AS c")),
    )
    .expect("count query must return a row");
    assert_eq!(
        after.get::<i64>("c")?,
        0,
        "all 3 edges gone via native DETACH DELETE"
    );

    let tombstoned = query_one_row(
        &rt,
        &graph,
        neo4rs::Query::new(format!(
            "MATCH (g:__ConduitGuard {{kind:'edge', target:'{etype}', deleted:true}}) RETURN count(g) AS c"
        )),
    )
    .expect("count query must return a row");
    assert_eq!(
        tombstoned.get::<i64>("c")?,
        3,
        "each edge's guard tombstoned"
    );
    Ok(())
}

/// Two threads racing writes to the same node must never lose a write or
/// corrupt the guard — one thread's CAS succeeds, the other's `WHERE`
/// filters it out (zero rows), retries against the freshly-committed guard,
/// and resolves to either `Updated` or `Skipped(StaleSequence)` depending on
/// which sequence actually won (Phase 24.3's optimistic-CAS guarantee).
#[test]
fn neo4j_concurrent_writers() -> R {
    require_neo4j!(uri);
    let label = fresh_label();

    let mapping_yaml = format!(
        "kind: node\nevent: C\nlabel: {label}\nversion: 1\nkey: payload.id\non_existing: replace\nproperties:\n  state: payload.state\n"
    );
    let mut m = HashMap::new();
    m.insert("C".into(), graph_mapping(&mapping_yaml));
    let a = Arc::new(neo4j_adapter(&uri, m));

    a.handle(&ev(
        "C",
        1,
        serde_json::json!({ "id": "n1", "state": "seed" }),
    ));

    let handles: Vec<_> = [10u64, 11u64]
        .into_iter()
        .map(|seq| {
            let a = Arc::clone(&a);
            std::thread::spawn(move || {
                a.handle(&ev(
                    "C",
                    seq,
                    serde_json::json!({ "id": "n1", "state": format!("s{seq}") }),
                ))
                .outcome
            })
        })
        .collect();

    for h in handles {
        let outcome = h.join().expect("writer thread must not panic");
        assert!(
            !matches!(outcome, AdapterOutcome::Failed(_)),
            "no writer may fail under bounded CAS retry: {outcome:?}"
        );
    }

    let rt = verify_rt();
    let graph = connect(&rt, &uri)?;
    let row = query_one_row(
        &rt,
        &graph,
        neo4rs::Query::new(format!(
            "MATCH (n:`{label}` {{id: 'n1'}}) RETURN n.state AS state"
        )),
    )
    .expect("node must exist");
    assert_eq!(row.get::<String>("state")?, "s11");
    Ok(())
}

/// Regression test for a real lost-update bug: `cas_write`'s original Cypher
/// read `g.last_sequence` in a `WHERE` clause with no write lock held on the
/// guard node, so two concurrent transactions could both pass the check
/// against the same stale value and whichever committed last in real time
/// won, independent of sequence order. Two racing writers (the test above)
/// rarely hit that window; twenty reliably did — a 20-writer stress run
/// reproduced the lost update in roughly 7 of 10 runs before the fix (a
/// forced write-lock acquisition on the guard node before the CAS
/// comparison, added to `cas_write`) and 0 of 30 after.
#[test]
fn neo4j_20_concurrent_writers_converge_to_highest_sequence() -> R {
    require_neo4j!(uri);
    let label = fresh_label();

    let mapping_yaml = format!(
        "kind: node\nevent: C\nlabel: {label}\nversion: 1\nkey: payload.id\non_existing: replace\nproperties:\n  state: payload.state\n"
    );
    let mut m = HashMap::new();
    m.insert("C".into(), graph_mapping(&mapping_yaml));
    let a = Arc::new(neo4j_adapter(&uri, m));

    a.handle(&ev(
        "C",
        1,
        serde_json::json!({ "id": "n1", "state": "seed" }),
    ));

    let handles: Vec<_> = (2u64..=21u64)
        .map(|seq| {
            let a = Arc::clone(&a);
            std::thread::spawn(move || {
                a.handle(&ev(
                    "C",
                    seq,
                    serde_json::json!({ "id": "n1", "state": format!("s{seq}") }),
                ))
                .outcome
            })
        })
        .collect();

    for h in handles {
        let outcome = h.join().expect("writer thread must not panic");
        assert!(
            !matches!(outcome, AdapterOutcome::Failed(_)),
            "no writer may fail under bounded CAS retry: {outcome:?}"
        );
    }

    let rt = verify_rt();
    let graph = connect(&rt, &uri)?;
    let row = query_one_row(
        &rt,
        &graph,
        neo4rs::Query::new(format!(
            "MATCH (n:`{label}` {{id: 'n1'}}) RETURN n.state AS state"
        )),
    )
    .expect("node must exist");
    assert_eq!(
        row.get::<String>("state")?,
        "s21",
        "the highest sequence's write must be the one that lands, never a lower one"
    );
    Ok(())
}

/// The same event stream, projected into the file adapter and Neo4j, must
/// converge to identical logical nodes.
#[test]
fn graph_cross_backend() -> R {
    require_neo4j!(uri);
    let label = fresh_label();
    let dir = tempfile::tempdir()?;

    let mapping_yaml = format!(
        "kind: node\nevent: C\nlabel: {label}\nversion: 1\nkey: payload.id\non_existing: replace\nproperties:\n  state: payload.state\n"
    );
    let mut m_file = HashMap::new();
    m_file.insert("C".into(), graph_mapping(&mapping_yaml));
    let file_store = GraphStore::new(
        "file".into(),
        dir.path().to_path_buf(),
        10,
        GraphRuntimeBuilder::new(m_file),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    );

    let mut m_neo = HashMap::new();
    m_neo.insert("C".into(), graph_mapping(&mapping_yaml));
    let neo_store = neo4j_adapter(&uri, m_neo);

    for (seq, state) in [(1u64, "a"), (3, "c"), (2, "b")] {
        let payload = serde_json::json!({ "id": "n1", "state": state });
        file_store.handle(&ev("C", seq, payload.clone()));
        neo_store.handle(&ev("C", seq, payload));
    }

    let file_value: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(
        dir.path().join("nodes").join(&label).join("n1.json"),
    )?)?;

    let rt = verify_rt();
    let graph = connect(&rt, &uri)?;
    let row = query_one_row(
        &rt,
        &graph,
        neo4rs::Query::new(format!(
            "MATCH (n:`{label}` {{id: 'n1'}}) RETURN n.state AS state"
        )),
    )
    .expect("node must exist");
    assert_eq!(file_value["state"], row.get::<String>("state")?);
    Ok(())
}
