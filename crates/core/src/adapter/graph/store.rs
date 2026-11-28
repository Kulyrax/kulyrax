use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    adapter::file_names,
    adapter::file_store::{FileStore, FileTxn, Layout},
    adapter::graph::adapter::GraphError,
    adapter::graph::exec::{self, EdgeRef, GraphBackend, GraphGuard, GraphOutcome},
    adapter::graph::mapping::{GraphMapping, encode_edge_key},
    adapter::graph::runtime::GraphRuntimeBuilder,
    adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter},
    event::Event,
    routing::StorageKind,
    runtime::config::MigrationPolicy,
    upcast::UpcasterRegistry,
};

/// One incident edge recorded against a node (Phase 16.2). Carries both
/// endpoints, each a label and an id, so the node-delete detach cascade can
/// drop the edge from the *other* endpoint's index too.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct IncidentRef {
    edge_type: String,
    key: String,
    from_label: String,
    from: String,
    to_label: String,
    to: String,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct IncidentIndex {
    #[serde(default)]
    edges: Vec<IncidentRef>,
}

/// File-backed directed property graph (Phase 16) — the fourth `StorageAdapter`.
/// Nodes and edges are *separately keyed records*, each an independent Phase
/// 12/13 sequence-gated state machine driven by the same `decide()` as every
/// prior adapter. Since Phase 24.1 the write path lives in
/// `exec::project_node`/`project_edge`; this file is the file-sidecar driver
/// — behaviour is byte-identical to the pre-24.1 inlined version. Since
/// Phase 3 (v0.11) every write goes through a `FileStore`: batched, logged,
/// and crash-safe.
pub struct GraphStore {
    id: String,
    priority: u32,
    store: FileStore,
    builder: GraphRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
}

impl GraphStore {
    /// Build the adapter for a `type: graph` entry, with the run's mappings,
    /// upcasters and migration policy from `ctx`. The public constructor
    /// (decision 2026-11-26-adapter-from-config).
    pub fn from_config(
        cfg: &crate::runtime::config::GraphAdapterConfig,
        ctx: &crate::runtime::registry::AdapterBuildContext,
    ) -> Result<Self, crate::runtime::config::ConfigError> {
        Ok(Self::new(
            cfg.id.clone(),
            PathBuf::from(&cfg.config.root),
            cfg.priority,
            crate::adapter::graph::runtime::GraphRuntimeBuilder::new(ctx.mappings.graph.clone()),
            Arc::clone(&ctx.upcasters),
            ctx.migration_policy,
        ))
    }

    #[doc(hidden)]
    pub fn new(
        id: String,
        root: PathBuf,
        priority: u32,
        builder: GraphRuntimeBuilder,
        upcasters: Arc<UpcasterRegistry>,
        migration_policy: MigrationPolicy,
    ) -> Self {
        Self {
            id,
            priority,
            store: FileStore::with_layout(root, layout(&builder)),
            builder,
            upcasters,
            migration_policy,
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Graph, error)
    }
}

/// Nodes and edges live under `nodes/` and `edges/`, one directory per label
/// and edge type.
fn layout(builder: &GraphRuntimeBuilder) -> Layout {
    let mut labels = BTreeSet::new();
    let mut edge_types = BTreeSet::new();
    for record in builder.mappings().values().flat_map(GraphMapping::records) {
        match record {
            GraphMapping::Node(n) => {
                labels.insert(n.label.clone());
            }
            GraphMapping::Edge(e) => {
                edge_types.insert(e.edge_type.clone());
                // Endpoint labels name incidence-index directories.
                labels.insert(e.from_label.clone());
                labels.insert(e.to_label.clone());
            }
            GraphMapping::Subgraph(_) => {}
        }
    }
    let names = labels
        .iter()
        .map(|l| ("label", l.clone()))
        .chain(edge_types.iter().map(|t| ("edge type", t.clone())))
        .collect();
    let labels: Vec<String> = labels.into_iter().collect();
    Layout {
        kind: "graph",
        claims: vec!["nodes".into(), "edges".into()],
        names,
        upgrade: Box::new(move |root| upgrade(root, &labels)),
        older: Box::new(older_graph),
    }
}

/// A graph store of format 2 or older keyed its edges without their
/// endpoints' labels, and those labels were never stored, so its edges cannot
/// be re-keyed. A store with no edge guards moves up as it is; one with any
/// must be cleared and its history replayed (decision 2026-11-28-edge-labels).
fn older_graph(root: &Path) -> io::Result<Option<String>> {
    let has_entries = |dir: &Path| -> io::Result<bool> {
        match std::fs::read_dir(dir) {
            Ok(mut entries) => Ok(entries.next().is_some()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e),
        }
    };
    if has_entries(&root.join(EDGE_GUARD_DIR))? {
        return Ok(Some(
            "it holds edges keyed by an earlier version, without their endpoints' labels, \
             which were never stored. Clear the store and replay the full history into it"
                .to_string(),
        ));
    }
    Ok(None)
}

/// The layout has not moved; refuse the node files an earlier version named
/// from ids this version encodes. Edge files were always named in hex.
fn upgrade(root: &Path, labels: &[String]) -> io::Result<Vec<String>> {
    let mut refused = Vec::new();
    for label in labels {
        file_names::refuse_foreign_names(&root.join("nodes").join(label), ".json", &mut refused)?;
        file_names::refuse_foreign_names(
            &root.join(NODE_GUARD_DIR).join(label),
            ".json",
            &mut refused,
        )?;
    }
    for label in labels {
        file_names::refuse_foreign_names(
            &root.join(INCIDENT_DIR).join(label),
            ".json",
            &mut refused,
        )?;
    }
    Ok(refused)
}

impl GraphStore {
    fn outcome(
        &self,
        outcome: GraphOutcome,
        source_version: u32,
        projected_version: u32,
    ) -> AdapterResult {
        match outcome {
            GraphOutcome::Created => AdapterResult::created_versioned(
                self.id.clone(),
                StorageKind::Graph,
                source_version,
                projected_version,
            ),
            GraphOutcome::Updated => AdapterResult::updated_versioned(
                self.id.clone(),
                StorageKind::Graph,
                source_version,
                projected_version,
            ),
            GraphOutcome::Deleted => AdapterResult::deleted_versioned(
                self.id.clone(),
                StorageKind::Graph,
                source_version,
                projected_version,
            ),
            GraphOutcome::Skipped(reason) => AdapterResult::skipped_versioned(
                self.id.clone(),
                StorageKind::Graph,
                reason,
                source_version,
                projected_version,
            ),
        }
    }
}

impl StorageAdapter for GraphStore {
    fn kind(&self) -> StorageKind {
        StorageKind::Graph
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn priority(&self) -> u32 {
        self.priority
    }

    fn prepare(&self) -> Result<(), AdapterError> {
        self.store.prepare()
    }

    fn begin_batch(&self, _events: &[&Event]) -> Result<(), AdapterError> {
        self.store.begin()
    }

    fn commit_batch(&self) -> Result<(), AdapterError> {
        self.store.commit()
    }

    fn rollback_batch(&self) {
        self.store.rollback()
    }

    fn handle(&self, event: &Event) -> AdapterResult {
        let projections = match self.builder.build_all(event, &self.upcasters) {
            Ok(p) => p,
            Err(GraphError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return match self.migration_policy {
                    MigrationPolicy::Strict => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::Graph,
                        reason,
                        from_version,
                        to_version,
                    ),
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::Graph,
                        SkipReason::UnsupportedVersion,
                        from_version,
                        to_version,
                    ),
                };
            }
            Err(e) => return self.failure(AdapterError::WriteFailed(e.to_string())),
        };

        let (source_version, projected_version) = match projections.first() {
            Some(p) => (p.source_version, p.projected_version),
            None => (event.version(), event.version()),
        };
        // Every record in one per-event overlay: a failing record discards
        // the event's earlier records too.
        let result = self
            .store
            .with_txn(|txn| exec::project_all(&mut FileGraphBackend { txn }, &projections, event));

        match result {
            Ok(Ok(o)) => self.outcome(o, source_version, projected_version),
            Ok(Err(e)) => self.failure(AdapterError::WriteFailed(e.to_string())),
            Err(e) => self.failure(e),
        }
    }
}

// ---------------------------------------------------------------------------
// FileGraphBackend — the driver
// ---------------------------------------------------------------------------

struct FileGraphBackend<'a, 't> {
    txn: &'a mut FileTxn<'t>,
}

/// Where node guards live; one directory per label.
const NODE_GUARD_DIR: &str = ".kulyrax/guard/nodes";

/// Node ids come from events, so they are encoded ([`file_names::stem`]);
/// labels and edge types were checked when the store was prepared.
fn node_value_path(label: &str, node_id: &str) -> String {
    format!("nodes/{label}/{}.json", file_names::stem(node_id))
}

fn node_guard_path(label: &str, node_id: &str) -> String {
    format!(
        "{NODE_GUARD_DIR}/{label}/{}.json",
        file_names::stem(node_id)
    )
}

/// An edge's file name: its canonical key in hex (always a plain name), or a
/// hashed name when that is too long.
fn edge_stem(key: &str) -> String {
    let hex = encode_edge_key(key);
    if hex.len() <= file_names::MAX_STEM {
        hex
    } else {
        file_names::hashed(key)
    }
}

fn edge_value_path(edge_type: &str, key: &str) -> String {
    format!("edges/{edge_type}/{}.json", edge_stem(key))
}

fn edge_guard_path(edge_type: &str, key: &str) -> String {
    format!("{EDGE_GUARD_DIR}/{edge_type}/{}.json", edge_stem(key))
}

const INCIDENT_DIR: &str = ".kulyrax/incident";
const EDGE_GUARD_DIR: &str = ".kulyrax/guard/edges";

/// A node's incidence index: per label, as nodes are (graph format 3). Keyed
/// by id alone, deleting one label's node cascaded into another label's
/// node's edges.
fn incident_path(label: &str, node_id: &str) -> String {
    format!("{INCIDENT_DIR}/{label}/{}.json", file_names::stem(node_id))
}

fn ge_err(e: std::io::Error) -> GraphError {
    GraphError::WriteFailed(e.to_string())
}

fn invalid(e: serde_json::Error) -> GraphError {
    ge_err(std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Which record a `.kulyrax/` file belongs to: the file name stem it was
/// written under and the full id it stands for, so a hashed name can carry
/// the id (see [`file_names::with_id`]).
#[derive(Clone, Copy)]
enum Owner<'a> {
    Node(&'a str),
    Edge(&'a str),
}

impl Owner<'_> {
    fn stem_and_id(self) -> (String, String) {
        match self {
            Owner::Node(id) => (file_names::stem(id), id.to_string()),
            Owner::Edge(key) => (edge_stem(key), key.to_string()),
        }
    }
}

impl FileGraphBackend<'_, '_> {
    fn read_owned(&mut self, path: &str, owner: Owner<'_>) -> Result<Option<String>, GraphError> {
        let Some(content) = self.txn.read(path).map_err(ge_err)? else {
            return Ok(None);
        };
        let (stem, id) = owner.stem_and_id();
        file_names::check_id(&content, &stem, &id).map_err(GraphError::WriteFailed)?;
        Ok(Some(content))
    }

    fn read_guard(
        &mut self,
        path: &str,
        owner: Owner<'_>,
    ) -> Result<Option<GraphGuard>, GraphError> {
        match self.read_owned(path, owner)? {
            Some(content) => serde_json::from_str(&content).map(Some).map_err(invalid),
            None => Ok(None),
        }
    }

    fn read_incident(&mut self, label: &str, node_id: &str) -> Result<IncidentIndex, GraphError> {
        match self.read_owned(&incident_path(label, node_id), Owner::Node(node_id))? {
            Some(content) => serde_json::from_str(&content).map_err(invalid),
            None => Ok(IncidentIndex::default()),
        }
    }

    fn commit_json<T: Serialize>(&mut self, path: &str, value: &T) -> Result<(), GraphError> {
        let content = serde_json::to_string_pretty(value).map_err(invalid)?;
        self.txn.write(path, content);
        Ok(())
    }

    /// A `.kulyrax/` file: under a hashed name it carries the owner's id.
    fn commit_owned<T: Serialize>(
        &mut self,
        path: &str,
        owner: Owner<'_>,
        value: &T,
    ) -> Result<(), GraphError> {
        let content = serde_json::to_string_pretty(value).map_err(invalid)?;
        let (stem, id) = owner.stem_and_id();
        self.txn
            .write(path, file_names::with_id(content, &stem, &id));
        Ok(())
    }

    fn add_incident(
        &mut self,
        label: &str,
        node_id: &str,
        r: &IncidentRef,
    ) -> Result<(), GraphError> {
        let mut idx = self.read_incident(label, node_id)?;
        if !idx
            .edges
            .iter()
            .any(|e| e.edge_type == r.edge_type && e.key == r.key)
        {
            idx.edges.push(r.clone());
            self.commit_owned(&incident_path(label, node_id), Owner::Node(node_id), &idx)?;
        }
        Ok(())
    }

    fn drop_incident(
        &mut self,
        label: &str,
        node_id: &str,
        edge_type: &str,
        key: &str,
    ) -> Result<(), GraphError> {
        let mut idx = self.read_incident(label, node_id)?;
        let before = idx.edges.len();
        idx.edges
            .retain(|e| !(e.edge_type == edge_type && e.key == key));
        if idx.edges.len() != before {
            if idx.edges.is_empty() {
                self.txn.remove(&incident_path(label, node_id));
            } else {
                self.commit_owned(&incident_path(label, node_id), Owner::Node(node_id), &idx)?;
            }
        }
        Ok(())
    }
}

impl GraphBackend for FileGraphBackend<'_, '_> {
    fn read_node_guard(
        &mut self,
        label: &str,
        node_id: &str,
    ) -> Result<Option<GraphGuard>, GraphError> {
        self.read_guard(&node_guard_path(label, node_id), Owner::Node(node_id))
    }

    fn read_edge_guard(
        &mut self,
        edge_type: &str,
        key: &str,
    ) -> Result<Option<GraphGuard>, GraphError> {
        self.read_guard(&edge_guard_path(edge_type, key), Owner::Edge(key))
    }

    fn node_is_tombstoned(&mut self, label: &str, node_id: &str) -> Result<bool, GraphError> {
        Ok(self
            .read_guard(&node_guard_path(label, node_id), Owner::Node(node_id))?
            .is_some_and(|g| g.deleted))
    }

    fn write_node(
        &mut self,
        label: &str,
        node_id: &str,
        properties: Option<&Value>,
        facet: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        if let Some(properties) = properties {
            let value_path = node_value_path(label, node_id);
            if !facet.is_empty() {
                let facet_obj = properties.as_object().ok_or_else(|| {
                    GraphError::BuildFailed(
                        "node facet mapping must resolve to a JSON object".into(),
                    )
                })?;
                let mut existing = match self.txn.read(&value_path).map_err(ge_err)? {
                    Some(s) => serde_json::from_str::<Value>(&s)
                        .unwrap_or_else(|_| Value::Object(Default::default())),
                    None => Value::Object(Default::default()),
                };
                let obj = existing.as_object_mut().ok_or_else(|| {
                    GraphError::BuildFailed("existing node document is not a JSON object".into())
                })?;
                for (k, v) in facet_obj {
                    obj.insert(k.clone(), v.clone());
                }
                self.commit_json(&value_path, &existing)?;
            } else {
                self.commit_json(&value_path, properties)?;
            }
        }
        self.commit_owned(
            &node_guard_path(label, node_id),
            Owner::Node(node_id),
            guard,
        )
    }

    /// An endpoint not projected yet gets its incidence entry under its
    /// label and id now: the node's own write lands on the same `(label,
    /// id)`, so the edge is the node's from then on (the file store's
    /// labelled placeholder).
    fn write_edge(
        &mut self,
        edge: &EdgeRef<'_>,
        properties: Option<&Value>,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        if let Some(properties) = properties {
            self.commit_json(&edge_value_path(edge.edge_type, edge.key), properties)?;
            let r = IncidentRef {
                edge_type: edge.edge_type.to_string(),
                key: edge.key.to_string(),
                from_label: edge.from_label.to_string(),
                from: edge.from.to_string(),
                to_label: edge.to_label.to_string(),
                to: edge.to.to_string(),
            };
            for (label, id) in edge.endpoints() {
                self.add_incident(label, id, &r)?;
            }
        }
        self.commit_owned(
            &edge_guard_path(edge.edge_type, edge.key),
            Owner::Edge(edge.key),
            guard,
        )
    }

    fn delete_node_cascade(
        &mut self,
        label: &str,
        node_id: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        self.txn.remove(&node_value_path(label, node_id));
        let incident = self.read_incident(label, node_id)?;
        for r in &incident.edges {
            self.txn.remove(&edge_value_path(&r.edge_type, &r.key));
            let egp = edge_guard_path(&r.edge_type, &r.key);
            let mut eg = self
                .read_guard(&egp, Owner::Edge(&r.key))?
                .unwrap_or_default();
            eg.last_sequence = guard.last_sequence;
            eg.last_event_id = guard.last_event_id.clone();
            eg.deleted = true;
            eg.permanent = guard.permanent;
            self.commit_owned(&egp, Owner::Edge(&r.key), &eg)?;
            for (other_label, other) in [(&r.from_label, &r.from), (&r.to_label, &r.to)] {
                if (other_label.as_str(), other.as_str()) != (label, node_id) {
                    self.drop_incident(other_label, other, &r.edge_type, &r.key)?;
                }
            }
        }
        self.txn.remove(&incident_path(label, node_id));
        self.commit_owned(
            &node_guard_path(label, node_id),
            Owner::Node(node_id),
            guard,
        )
    }

    fn delete_edge(&mut self, edge: &EdgeRef<'_>, guard: &GraphGuard) -> Result<(), GraphError> {
        self.txn.remove(&edge_value_path(edge.edge_type, edge.key));
        for (label, id) in edge.endpoints() {
            self.drop_incident(label, id, edge.edge_type, edge.key)?;
        }
        self.commit_owned(
            &edge_guard_path(edge.edge_type, edge.key),
            Owner::Edge(edge.key),
            guard,
        )
    }
}
