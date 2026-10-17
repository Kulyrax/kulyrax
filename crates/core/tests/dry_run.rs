//! `dry_run_once` resolves each routed adapter's write with the real projection
//! builders and never builds an adapter, so nothing is written and no store is
//! contacted.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use kulyrax_core::adapter::{OnExisting, Operation};
use kulyrax_core::event::Event;
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::{DryRunOutcome, KulyraxRuntime, PlannedWrite};

fn write(root: &Path, relative: &str, content: &str) {
    let path = root.join(relative);
    fs::create_dir_all(path.parent().expect("has parent")).expect("create dir");
    fs::write(path, content).expect("write file");
}

/// A project with a SQLite adapter and a file-backed document adapter, both
/// rooted inside `root`, plus whatever extra adapter entries the test needs.
fn project(
    root: &Path,
    extra_adapters: &str,
    extra_routing: &str,
    extra_mappings: &[(&str, &str)],
) -> KulyraxRuntime {
    let config = format!(
        "version: 1\nrouting:\n  file: routing.yaml\nadapters:\n  \
         - type: sqlite\n    id: sql-primary\n    priority: 10\n    capabilities: [write, upsert]\n    config:\n      path: {db}\n  \
         - type: file\n    id: doc-readmodel\n    priority: 20\n    config:\n      root: {docs}\n{extra_adapters}",
        db = root.join("app.db").display().to_string().replace('\\', "/"),
        docs = root.join("docs").display().to_string().replace('\\', "/"),
    );
    write(root, "config.yaml", &config);
    write(
        root,
        "routing.yaml",
        &format!("UserRegistered: [sql-primary, doc-readmodel]\n{extra_routing}"),
    );
    write(
        root,
        "mappings/sql/user_registered.yaml",
        "event: UserRegistered\ntable: users\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n  name: payload.name\n",
    );
    write(
        root,
        "mappings/document/user_registered.yaml",
        "event: UserRegistered\ncollection: users\nid: payload.id\nversion: 1\ndocument:\n  name: payload.name\n",
    );
    for (relative, content) in extra_mappings {
        write(root, relative, content);
    }
    KulyraxRuntime::load(&root.join("config.yaml"), &root.join("mappings")).expect("project loads")
}

fn event(event_type: &str, payload: &str) -> Event {
    Event {
        event_id: "e1".into(),
        event_type: event_type.into(),
        payload: payload.into(),
        metadata: HashMap::new(),
        version: 1,
        sequence: 1,
    }
}

fn planned(outcome: &DryRunOutcome) -> &PlannedWrite {
    match outcome {
        DryRunOutcome::Planned(w) => w,
        other => panic!("expected a planned write, got {other:?}"),
    }
}

#[test]
fn dry_run_touches_no_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let runtime = project(dir.path(), "", "", &[]);

    runtime
        .dry_run_once(&event("UserRegistered", r#"{"id":"u1","name":"Ada"}"#))
        .expect("dry run");

    assert!(
        !dir.path().join("app.db").exists(),
        "SQLite file must not be created"
    );
    assert!(
        !dir.path().join("docs").exists(),
        "document root must not be created"
    );
}

#[test]
fn dry_run_reports_each_adapters_write_in_execution_order() {
    let dir = tempfile::tempdir().expect("tempdir");
    let runtime = project(dir.path(), "", "", &[]);

    let report = runtime
        .dry_run_once(&event("UserRegistered", r#"{"id":"u1","name":"Ada"}"#))
        .expect("dry run");

    let ids: Vec<_> = report
        .adapters
        .iter()
        .map(|a| a.adapter_id.as_str())
        .collect();
    assert_eq!(ids, ["sql-primary", "doc-readmodel"]);

    let sql = planned(&report.adapters[0].outcome);
    assert_eq!(report.adapters[0].kind, Some(StorageKind::Sql));
    assert_eq!(sql.operation, Operation::Upsert);
    assert_eq!(sql.on_existing, OnExisting::Ignore);
    assert_eq!(sql.target, "users");
    assert_eq!(sql.entity_key, r#"["u1"]"#);
    assert_eq!(sql.values, serde_json::json!({"id": "u1", "name": "Ada"}));

    let doc = planned(&report.adapters[1].outcome);
    assert_eq!(report.adapters[1].kind, Some(StorageKind::Document));
    assert_eq!(doc.target, "users");
    assert_eq!(doc.entity_key, "u1");
    assert_eq!(doc.values, serde_json::json!({"name": "Ada"}));
}

#[test]
fn unresolvable_write_is_reported_as_failed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let runtime = project(dir.path(), "", "", &[]);

    let report = runtime
        .dry_run_once(&event("UserRegistered", r#"{"name":"Ada"}"#))
        .expect("dry run");

    assert!(
        matches!(report.adapters[0].outcome, DryRunOutcome::Failed { .. }),
        "missing primary key field must fail: {:?}",
        report.adapters[0].outcome
    );
}

#[test]
fn networked_backend_is_previewed_without_connecting() {
    let dir = tempfile::tempdir().expect("tempdir");
    // Nothing listens on this port; building the adapter would fail.
    let runtime = project(
        dir.path(),
        "  - type: postgres\n    id: pg-replica\n    priority: 30\n    config:\n      url: postgres://nobody@127.0.0.1:1/none\n",
        "OrderPlaced: [pg-replica]\n",
        &[(
            "mappings/sql/order_placed.yaml",
            "event: OrderPlaced\ntable: orders\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\n",
        )],
    );

    let report = runtime
        .dry_run_once(&event("OrderPlaced", r#"{"id":"o1"}"#))
        .expect("dry run");

    assert_eq!(report.adapters[0].kind, Some(StorageKind::Sql));
    assert_eq!(planned(&report.adapters[0].outcome).target, "orders");
}

#[test]
fn third_party_adapter_type_is_not_previewable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let runtime = project(
        dir.path(),
        "  - type: acme-store\n    id: acme\n    priority: 30\n    config: {}\n",
        "OrderPlaced: [acme]\n",
        &[],
    );

    let report = runtime
        .dry_run_once(&event("OrderPlaced", r#"{"id":"o1"}"#))
        .expect("dry run");

    assert_eq!(report.adapters[0].kind, None);
    assert!(matches!(
        report.adapters[0].outcome,
        DryRunOutcome::NotPreviewable { .. }
    ));
}
