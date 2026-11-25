//! F-01: does the run loop's sort by `event.sequence` reorder events of
//! *different* entities against their delivery order?
//!
//! The producer contract makes `sequence` per entity ("different entities have
//! independent sequences") and asks producers to emit a node before an edge
//! that points at it (§7). Each test delivers events in an order that obeys the
//! contract, with per-entity sequences that do not happen to agree with that
//! order, and checks what the backend holds afterwards. A control run with
//! `--max-batch 1` (one event per batch, so nothing can be reordered) shows the
//! expected result.
//!
//! **Gated on** `KULYRAX_NEO4J_URI` (graph cases) and `KULYRAX_PG_URL` (SQL and
//! outbox cases). A graph case owns the whole Neo4j database while it runs
//! (`support/neo4j/`); an SQL case works in a schema of its own.

#[path = "support/neo4j/mod.rs"]
mod neo4j_isolation;

use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_kulyrax"))
}

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.trim().is_empty())
}

fn unique(prefix: &str) -> String {
    use std::sync::atomic::{AtomicU32, Ordering};
    static N: AtomicU32 = AtomicU32::new(0);
    format!(
        "{prefix}{}x{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

fn write(path: PathBuf, s: &str) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, s).unwrap();
}

/// One line per event: (event type, sequence, payload JSON).
fn events(p: &Path, events: &[(&str, u64, serde_json::Value)]) {
    let lines: Vec<String> = events
        .iter()
        .enumerate()
        .map(|(i, (t, seq, payload))| {
            serde_json::json!({
                "event_id": format!("e{i}"),
                "event_type": t,
                "metadata": {},
                "payload": payload.to_string(),
                "sequence": seq,
                "version": 1,
            })
            .to_string()
        })
        .collect();
    write(p.join("events/000001.ndjson"), &(lines.join("\n") + "\n"));
}

fn run(p: &Path, max_batch: Option<&str>) -> Output {
    let mut cmd = Command::new(bin());
    cmd.current_dir(p)
        .args([
            "run",
            "--config",
            "kulyrax.yaml",
            "--mappings",
            "mappings",
            "--once",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(n) = max_batch {
        cmd.args(["--max-batch", n]);
    }
    cmd.output().unwrap()
}

fn text(out: &Output) -> String {
    format!(
        "{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

// ---------------------------------------------------------------------------
// Graph: an edge emitted after its nodes
// ---------------------------------------------------------------------------

fn neo4j_project(p: &Path, uri: &str, label: &str, edge: &str) {
    let user = neo4j_isolation::user();
    let password = neo4j_isolation::password();
    write(
        p.join("kulyrax.yaml"),
        &format!(
            "version: 1\nrouting:\n  file: routing.yaml\nadapters:\n  - type: neo4j\n    id: g\n    priority: 10\n    config:\n      uri: {uri}\n      user: {user}\n      password: {password}\nsources:\n  - type: directory\n    id: events\n    path: ./events\n    state_dir: ./state\n"
        ),
    );
    write(p.join("routing.yaml"), "N: [g]\nF: [g]\n");
    std::fs::create_dir_all(p.join("mappings/sql")).unwrap();
    std::fs::create_dir_all(p.join("mappings/document")).unwrap();
    write(
        p.join("mappings/graph/n.yaml"),
        &format!(
            "kind: node\nevent: N\nlabel: {label}\nkey: payload.id\nversion: 1\nproperties:\n  name: payload.id\n"
        ),
    );
    write(
        p.join("mappings/graph/f.yaml"),
        &format!(
            "kind: edge\nevent: F\nedge_type: {edge}\nversion: 1\nfrom: payload.from\nto: payload.to\nproperties:\n  since: payload.since\n"
        ),
    );
    // Delivery order obeys the contract: both nodes, then the edge. The edge
    // is its own entity, at its own first sequence (1); the nodes are further
    // along theirs (5 and 3).
    events(
        p,
        &[
            ("N", 5, serde_json::json!({"id": "u1"})),
            ("N", 3, serde_json::json!({"id": "u2"})),
            (
                "F",
                1,
                serde_json::json!({"from": "u1", "to": "u2", "since": 2026}),
            ),
        ],
    );
}

/// (edges between the two labelled nodes, nodes with id u1, nodes with id u2).
fn neo4j_state(uri: &str, label: &str, edge: &str) -> (i64, i64, i64) {
    let user = neo4j_isolation::user();
    let password = neo4j_isolation::password();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let g = neo4rs::Graph::new(uri, user, password).await.unwrap();
        let count = |q: String| {
            let g = g.clone();
            async move {
                let mut rows = g.execute(neo4rs::Query::new(q)).await.unwrap();
                rows.next().await.unwrap().unwrap().get::<i64>("n").unwrap()
            }
        };
        let edges = count(format!(
            "MATCH (:`{label}` {{id: 'u1'}})-[r:`{edge}`]->(:`{label}` {{id: 'u2'}}) RETURN count(r) AS n"
        ))
        .await;
        // Nodes with that id under this test's label or under no label (the
        // placeholders an edge MERGE creates).
        let nodes = |id: &'static str| {
            count(format!(
                "MATCH (n {{id: '{id}'}}) WHERE n:`{label}` OR size(labels(n)) = 0 RETURN count(n) AS n"
            ))
        };
        (edges, nodes("u1").await, nodes("u2").await)
    })
}

fn neo4j_cleanup(uri: &str, label: &str) {
    let user = neo4j_isolation::user();
    let password = neo4j_isolation::password();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async {
        let g = neo4rs::Graph::new(uri, user, password).await.unwrap();
        for q in [
            format!("MATCH (n:`{label}`) DETACH DELETE n"),
            "MATCH (n) WHERE size(labels(n)) = 0 AND n.id IN ['u1', 'u2'] DETACH DELETE n".into(),
            format!("MATCH (g:__KulyraxGuard) WHERE g.target = '{label}' DETACH DELETE g"),
        ] {
            g.run(neo4rs::Query::new(q)).await.unwrap();
        }
    });
}

#[test]
fn an_edge_after_its_nodes_attaches_to_them_in_neo4j() {
    let Some(lease) = neo4j_isolation::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return;
    };
    let uri = lease.uri().to_string();
    // Both runs first, then the verdict, so a failure shows the control too.
    let mut results = Vec::new();
    for (name, max_batch) in [("one batch", None), ("one event per batch", Some("1"))] {
        let dir = tempfile::tempdir().unwrap();
        let label = unique("Ord");
        let edge = unique("FOLLOWS");
        neo4j_project(dir.path(), &uri, &label, &edge);
        let out = run(dir.path(), max_batch);
        assert!(out.status.success(), "{name}: {}", text(&out));
        let state = neo4j_state(&uri, &label, &edge);
        neo4j_cleanup(&uri, &label);
        results.push((name, state));
    }
    for (name, state) in &results {
        assert_eq!(
            *state,
            (1, 1, 1),
            "{name}: (edges between the labelled nodes, nodes with id u1, nodes with id u2); \
             all runs: {results:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// SQL: a child row emitted after its parent, with a foreign key
// ---------------------------------------------------------------------------

#[test]
fn a_child_row_after_its_parent_needs_no_retry() {
    let Some(base) = env("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return;
    };
    let schema = unique("kx_order_");
    let mut c = postgres::Client::connect(&base, postgres::NoTls).unwrap();
    c.batch_execute(&format!(
        "CREATE SCHEMA {schema};
         CREATE TABLE {schema}.parent (id text PRIMARY KEY);
         CREATE TABLE {schema}.child (id text PRIMARY KEY,
             parent_id text NOT NULL REFERENCES {schema}.parent (id))"
    ))
    .unwrap();
    let sep = if base.contains('?') { '&' } else { '?' };
    let url = format!("{base}{sep}options=-c%20search_path%3D{schema}");

    let dir = tempfile::tempdir().unwrap();
    let p = dir.path();
    write(
        p.join("kulyrax.yaml"),
        &format!(
            "version: 1\nrouting:\n  file: routing.yaml\nadapters:\n  - type: postgres\n    id: pg\n    priority: 10\n    config:\n      url: \"{url}\"\nsources:\n  - type: directory\n    id: events\n    path: ./events\n    state_dir: ./state\n"
        ),
    );
    write(p.join("routing.yaml"), "P: [pg]\nC: [pg]\n");
    std::fs::create_dir_all(p.join("mappings/document")).unwrap();
    write(
        p.join("mappings/sql/p.yaml"),
        "event: P\ntable: parent\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n",
    );
    write(
        p.join("mappings/sql/c.yaml"),
        "event: C\ntable: child\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n  parent_id: payload.parent\n",
    );
    // The parent first, as delivered; the child is at its own first sequence.
    events(
        p,
        &[
            ("P", 7, serde_json::json!({"id": "p1"})),
            ("C", 1, serde_json::json!({"id": "c1", "parent": "p1"})),
        ],
    );
    let out = run(p, None);
    let rows: i64 = c
        .query_one(&format!("SELECT count(*) FROM {schema}.child"), &[])
        .unwrap()
        .get(0);
    let _ = c.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"));
    let stdout = String::from_utf8_lossy(&out.stdout).to_string();
    assert!(out.status.success(), "{}", text(&out));
    assert_eq!(rows, 1, "the child row was projected\n{}", text(&out));
    assert!(
        stdout.contains("failed 0") && stdout.contains("retries 0"),
        "delivered in order, nothing should fail or need a retry\n{stdout}"
    );
}

// ---------------------------------------------------------------------------
// Outbox: sequence is the outbox's own global order
// ---------------------------------------------------------------------------

#[test]
fn an_outbox_edge_after_its_nodes_attaches_to_them_in_neo4j() {
    let Some(base) = env("KULYRAX_PG_URL") else {
        eprintln!("SKIP: KULYRAX_PG_URL not set");
        return;
    };
    let Some(lease) = neo4j_isolation::lease() else {
        eprintln!("SKIP: KULYRAX_NEO4J_URI not set");
        return;
    };
    let uri = lease.uri().to_string();
    let schema = unique("kx_outbox_");
    let mut c = postgres::Client::connect(&base, postgres::NoTls).unwrap();
    c.batch_execute(&format!(
        "CREATE SCHEMA {schema};
         CREATE TABLE {schema}.outbox (id bigserial PRIMARY KEY, event_id text NOT NULL,
             event_type text NOT NULL, payload jsonb NOT NULL, metadata jsonb, version int)"
    ))
    .unwrap();
    for (t, payload) in [
        ("N", r#"{"id": "u1"}"#),
        ("N", r#"{"id": "u2"}"#),
        ("F", r#"{"from": "u1", "to": "u2", "since": 2026}"#),
    ] {
        c.execute(
            &format!(
                "INSERT INTO {schema}.outbox (event_id, event_type, payload, version)
                 VALUES (gen_random_uuid()::text, $1, $2::text::jsonb, 1)"
            ),
            &[&t, &payload],
        )
        .unwrap();
    }

    let dir = tempfile::tempdir().unwrap();
    let p = dir.path();
    let label = unique("Ord");
    let edge = unique("FOLLOWS");
    neo4j_project(p, &uri, &label, &edge);
    std::fs::remove_dir_all(p.join("events")).unwrap();
    let cfg = std::fs::read_to_string(p.join("kulyrax.yaml")).unwrap();
    let cfg = cfg.replace(
        "  - type: directory\n    id: events\n    path: ./events\n    state_dir: ./state\n",
        &format!(
            "  - type: postgres_outbox\n    id: outbox\n    state_dir: ./state\n    config:\n      url: {base}\n      table: {schema}.outbox\n      sequence_column: id\n"
        ),
    );
    write(p.join("kulyrax.yaml"), &cfg);
    let out = run(p, None);
    let state = neo4j_state(&uri, &label, &edge);
    neo4j_cleanup(&uri, &label);
    let _ = c.batch_execute(&format!("DROP SCHEMA {schema} CASCADE"));
    assert!(out.status.success(), "{}", text(&out));
    assert_eq!(state, (1, 1, 1), "{}", text(&out));
}
