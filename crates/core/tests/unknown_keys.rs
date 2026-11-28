//! Every built-in config, mapping and routing struct refuses a key it does not
//! know (`deny_unknown_fields`, stability.md "Unknown keys"). The error names
//! the key, the file, and the line. One test per file kind, each with a
//! misspelt key.

use std::fs;
use std::path::Path;

use kulyrax_core::pipeline::{load_config, load_mappings};

/// `err` names `key`, the file `file`, and line `line`.
fn assert_names(err: &str, key: &str, file: &str, line: usize) {
    assert!(
        err.contains(&format!("`{key}`")),
        "no key `{key}` in: {err}"
    );
    assert!(err.contains(file), "no file {file} in: {err}");
    assert!(
        err.contains(&format!("line {line}")),
        "no line {line} in: {err}"
    );
}

fn config_error(yaml: &str) -> String {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("config.yaml");
    fs::write(&path, yaml).expect("write");
    load_config(&path)
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default()
}

const HEAD: &str = "version: 1\nrouting:\n  file: routing.yaml\n";

#[test]
fn config_top_level() {
    let err = config_error(&format!("{HEAD}failure_polcy: fail_fast\nadapters: []\n"));
    assert_names(&err, "failure_polcy", "config.yaml", 4);
}

#[test]
fn config_routing_block() {
    let err = config_error(
        "version: 1\nrouting:\n  file: routing.yaml\n  fiel: other.yaml\nadapters: []\n",
    );
    assert_names(&err, "fiel", "config.yaml", 4);
}

#[test]
fn config_built_in_adapter_entry() {
    let err = config_error(&format!(
        "{HEAD}adapters:\n  - type: sqlite\n    id: sql\n    priority: 10\n    depends_om: []\n    config:\n      path: p.db\n"
    ));
    assert_names(&err, "depends_om", "config.yaml", 8);
}

#[test]
fn config_built_in_adapter_block() {
    let err = config_error(&format!(
        "{HEAD}adapters:\n  - type: file\n    id: doc\n    priority: 10\n    config:\n      root: data\n      roots: other\n"
    ));
    assert_names(&err, "roots", "config.yaml", 10);
}

#[test]
fn config_custom_adapter_entry() {
    // Core reads the entry's own keys; the `config:` block is the backend's.
    let err = config_error(&format!(
        "{HEAD}adapters:\n  - type: postgres\n    id: pg\n    priority: 10\n    depend_on: [x]\n    config:\n      url: postgres://db/app\n"
    ));
    assert_names(&err, "depend_on", "config.yaml", 8);
}

#[test]
fn config_source_entries() {
    let err = config_error(&format!(
        "{HEAD}adapters: []\nsources:\n  - type: directory\n    id: in\n    path: events\n    state_dir: state\n    batch: 10\n"
    ));
    assert_names(&err, "batch", "config.yaml", 10);

    let err = config_error(&format!(
        "{HEAD}adapters: []\nsources:\n  - type: postgres_outbox\n    id: o\n    state_dri: state\n    config: {{}}\n"
    ));
    assert_names(&err, "state_dri", "config.yaml", 8);
}

/// The `mappings/` tree with one file of `kind` holding `yaml`; the others empty.
fn mapping_error(kind: &str, yaml: &str) -> String {
    let dir = tempfile::tempdir().expect("tempdir");
    for k in ["sql", "document", "keyvalue", "graph"] {
        fs::create_dir_all(dir.path().join(k)).expect("mkdir");
    }
    fs::write(dir.path().join(kind).join("m.yaml"), yaml).expect("write");
    load(dir.path())
}

fn load(dir: &Path) -> String {
    load_mappings(dir)
        .err()
        .map(|e| e.to_string())
        .unwrap_or_default()
}

#[test]
fn sql_mapping() {
    let err = mapping_error(
        "sql",
        "event: E\ntable: t\nprimary_key: id\nversion: 1\ncolumns:\n  id: payload.id\non_exsting: replace\n",
    );
    assert_names(&err, "on_exsting", "m.yaml", 7);
}

#[test]
fn document_mapping() {
    let err = mapping_error(
        "document",
        "event: E\ncollection: c\nversion: 1\nid: payload.id\ndocument:\n  id: payload.id\nfacit: f\n",
    );
    assert_names(&err, "facit", "m.yaml", 7);
}

#[test]
fn keyvalue_mapping() {
    let err = mapping_error(
        "keyvalue",
        "event: E\nnamespace: n\nversion: 1\nkey: payload.id\nvalue:\n  v: payload.v\npermanant: true\n",
    );
    assert_names(&err, "permanant", "m.yaml", 7);
}

#[test]
fn graph_node_and_edge_mappings() {
    let err = mapping_error(
        "graph",
        "kind: node\nevent: E\nlabel: L\nversion: 1\nkey: payload.id\nlabels: [X]\n",
    );
    assert_names(&err, "labels", "m.yaml", 6);

    let err = mapping_error(
        "graph",
        "kind: edge\nevent: E\nedge_type: T\nfrom_label: L\nto_label: L\nversion: 1\nfrom: payload.a\nto: payload.b\ndiscriminater: payload.c\n",
    );
    assert_names(&err, "discriminater", "m.yaml", 9);
}

#[test]
fn graph_subgraph_mapping() {
    let err = mapping_error(
        "graph",
        "kind: subgraph\nevent: E\nversion: 1\nrecord: []\n",
    );
    assert_names(&err, "record", "m.yaml", 4);
}
