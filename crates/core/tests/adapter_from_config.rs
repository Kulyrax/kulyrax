//! The built-in adapters' public constructor is `from_config(&entry, &ctx)`
//! (decision 2026-11-26-adapter-from-config), and every config struct a caller
//! may build in code has a builder that agrees with the YAML it stands for.

use std::sync::Arc;

use kulyrax_core::AdapterBuildContext;
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::adapter::document::file::FileDocumentAdapter;
use kulyrax_core::adapter::graph::store::GraphStore;
use kulyrax_core::adapter::keyvalue::store::FileKvStore;
use kulyrax_core::adapter::sql::sqlite::SqliteAdapter;
use kulyrax_core::routing::StorageKind;
use kulyrax_core::runtime::config::{
    AdapterCapability, CustomSourceConfig, DirectorySourceConfig, FileAdapterConfig, FileConfig,
    GraphAdapterConfig, GraphConfig, KeyValueAdapterConfig, KeyValueConfig, MigrationPolicy,
    SqliteAdapterConfig, SqliteConfig, StdinSourceConfig,
};
use kulyrax_core::upcast::UpcasterRegistry;

fn ctx() -> AdapterBuildContext {
    AdapterBuildContext::new(
        Default::default(),
        Arc::new(UpcasterRegistry::new()),
        MigrationPolicy::default(),
    )
}

fn shape(a: &dyn StorageAdapter) -> (&str, u32, StorageKind) {
    (a.id(), a.priority(), a.kind())
}

#[test]
fn each_built_in_adapter_builds_from_its_entry() {
    let dir = tempfile::tempdir().expect("tempdir");
    let root = dir.path().to_string_lossy();
    let db = dir.path().join("p.db");

    let sql = SqliteAdapter::from_config(
        &SqliteAdapterConfig::new("sql", 1, SqliteConfig::new(db.to_string_lossy())),
        &ctx(),
    )
    .expect("sqlite");
    let doc = FileDocumentAdapter::from_config(
        &FileAdapterConfig::new("doc", 2, FileConfig::new(root.clone())),
        &ctx(),
    )
    .expect("file");
    let kv = FileKvStore::from_config(
        &KeyValueAdapterConfig::new("kv", 3, KeyValueConfig::new(root.clone())),
        &ctx(),
    )
    .expect("keyvalue");
    let graph = GraphStore::from_config(
        &GraphAdapterConfig::new("graph", 4, GraphConfig::new(root.clone())),
        &ctx(),
    )
    .expect("graph");

    assert_eq!(shape(&sql), ("sql", 1, StorageKind::Sql));
    assert_eq!(shape(&doc), ("doc", 2, StorageKind::Document));
    assert_eq!(shape(&kv), ("kv", 3, StorageKind::KeyValue));
    assert_eq!(shape(&graph), ("graph", 4, StorageKind::Graph));
}

/// Builder output and YAML output compared through `Debug`, which shows
/// every field.
fn same<T: std::fmt::Debug>(built: T, parsed: T) {
    assert_eq!(format!("{built:?}"), format!("{parsed:?}"));
}

#[test]
fn the_entry_builders_agree_with_yaml() {
    same(
        SqliteAdapterConfig::new("sql", 10, SqliteConfig::new("p.db"))
            .with_capabilities(vec![AdapterCapability::Write])
            .with_depends_on(vec!["doc".into()]),
        kulyrax_core::yaml::from_str(
            "id: sql\npriority: 10\nconfig: { path: p.db }\ncapabilities: [write]\ndepends_on: [doc]\n",
        )
        .expect("yaml"),
    );
    same(
        FileAdapterConfig::new("doc", 20, FileConfig::new("data")),
        kulyrax_core::yaml::from_str("id: doc\npriority: 20\nconfig: { root: data }\n")
            .expect("yaml"),
    );
    same(
        KeyValueAdapterConfig::new("kv", 30, KeyValueConfig::new("data")),
        kulyrax_core::yaml::from_str("id: kv\npriority: 30\nconfig: { root: data }\n")
            .expect("yaml"),
    );
    same(
        GraphAdapterConfig::new("g", 40, GraphConfig::new("data")),
        kulyrax_core::yaml::from_str("id: g\npriority: 40\nconfig: { root: data }\n")
            .expect("yaml"),
    );
}

#[test]
fn the_source_builders_agree_with_yaml() {
    assert_eq!(
        DirectorySourceConfig::new("in", "events", "state"),
        kulyrax_core::yaml::from_str::<DirectorySourceConfig>(
            "id: in\npath: events\nstate_dir: state\n"
        )
        .expect("yaml"),
    );
    assert_eq!(
        StdinSourceConfig::new("in", "state"),
        kulyrax_core::yaml::from_str::<StdinSourceConfig>("id: in\nstate_dir: state\n")
            .expect("yaml"),
    );
    let raw = serde_json::json!({ "type": "x", "id": "in", "state_dir": "s" });
    let built = CustomSourceConfig::new("x", "in", raw).with_state_dir("s");
    assert_eq!(
        (
            built.type_name.as_str(),
            built.id.as_str(),
            built.state_dir.as_deref()
        ),
        ("x", "in", Some("s"))
    );
}
