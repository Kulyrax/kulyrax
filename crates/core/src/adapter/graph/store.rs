use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    adapter::graph::adapter::GraphError,
    adapter::graph::exec::{self, EdgePlan, GraphBackend, GraphGuard, GraphOutcome, NodePlan},
    adapter::graph::mapping::{ResolvedRecord, canonical_edge_key, encode_edge_key},
    adapter::graph::runtime::GraphRuntimeBuilder,
    adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter},
    event::Event,
    routing::StorageKind,
    runtime::config::MigrationPolicy,
    upcast::UpcasterRegistry,
};

/// One incident edge recorded against a node (Phase 16.2). Carries both
/// endpoints so the node-delete detach cascade can drop the edge from the
/// *other* endpoint's index too.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct IncidentRef {
    edge_type: String,
    key: String,
    from: String,
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
/// — behaviour is byte-identical to the pre-24.1 inlined version.
pub struct GraphStore {
    id: String,
    priority: u32,
    root: PathBuf,
    builder: GraphRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
}

impl GraphStore {
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
            root,
            builder,
            upcasters,
            migration_policy,
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Graph, error)
    }

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

    fn handle(&self, event: &Event) -> AdapterResult {
        let projection = match self.builder.build(event, &self.upcasters) {
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

        let source_version = projection.source_version;
        let projected_version = projection.projected_version;
        let mut backend = FileGraphBackend { root: &self.root };

        let result = match &projection.record {
            ResolvedRecord::Node {
                label,
                node_id,
                properties,
            } => {
                let plan = NodePlan {
                    label: label.clone(),
                    node_id: node_id.clone(),
                    properties: properties.clone(),
                    on_existing: projection.on_existing,
                    operation: projection.operation,
                    permanent: projection.permanent,
                    facet: projection.facet.clone(),
                };
                exec::project_node(&mut backend, &plan, event)
            }
            ResolvedRecord::Edge {
                edge_type,
                from,
                to,
                discriminator,
                properties,
            } => {
                let key = canonical_edge_key(edge_type, from, to, discriminator.as_deref());
                let plan = EdgePlan {
                    edge_type: edge_type.clone(),
                    from: from.clone(),
                    to: to.clone(),
                    key,
                    properties: properties.clone(),
                    on_existing: projection.on_existing,
                    operation: projection.operation,
                    permanent: projection.permanent,
                };
                exec::project_edge(&mut backend, &plan, event)
            }
        };

        match result {
            Ok(o) => self.outcome(o, source_version, projected_version),
            Err(e) => self.failure(AdapterError::WriteFailed(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// FileGraphBackend — the driver
// ---------------------------------------------------------------------------

struct FileGraphBackend<'a> {
    root: &'a Path,
}

impl FileGraphBackend<'_> {
    fn node_value_path(&self, label: &str, node_id: &str) -> PathBuf {
        self.root
            .join("nodes")
            .join(label)
            .join(format!("{node_id}.json"))
    }

    fn node_guard_path(&self, label: &str, node_id: &str) -> PathBuf {
        self.root
            .join(".conduit")
            .join("guard")
            .join("nodes")
            .join(label)
            .join(format!("{node_id}.json"))
    }

    fn edge_value_path(&self, edge_type: &str, key: &str) -> PathBuf {
        self.root
            .join("edges")
            .join(edge_type)
            .join(format!("{}.json", encode_edge_key(key)))
    }

    fn edge_guard_path(&self, edge_type: &str, key: &str) -> PathBuf {
        self.root
            .join(".conduit")
            .join("guard")
            .join("edges")
            .join(edge_type)
            .join(format!("{}.json", encode_edge_key(key)))
    }

    fn incident_path(&self, node_id: &str) -> PathBuf {
        self.root
            .join(".conduit")
            .join("incident")
            .join(format!("{node_id}.json"))
    }

    fn read_guard(&self, path: &Path) -> std::io::Result<Option<GraphGuard>> {
        match std::fs::read_to_string(path) {
            Ok(content) => {
                let parsed: GraphGuard = serde_json::from_str(&content)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
                Ok(Some(parsed))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    fn read_incident(&self, node_id: &str) -> std::io::Result<IncidentIndex> {
        match std::fs::read_to_string(self.incident_path(node_id)) {
            Ok(content) => serde_json::from_str(&content)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(IncidentIndex::default()),
            Err(e) => Err(e),
        }
    }

    fn commit_json<T: Serialize>(&self, path: &Path, value: &T) -> std::io::Result<()> {
        let parent = path.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "path has no parent directory",
            )
        })?;
        std::fs::create_dir_all(parent)?;
        let tmp_name = format!(
            ".{}.tmp-{}",
            path.file_name().and_then(|n| n.to_str()).unwrap_or("f"),
            std::process::id()
        );
        let tmp = parent.join(tmp_name);
        let content = serde_json::to_string_pretty(value)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        std::fs::write(&tmp, content)?;
        std::fs::rename(&tmp, path)
    }

    fn remove_if_exists(&self, path: &Path) -> std::io::Result<()> {
        match std::fs::remove_file(path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    fn add_incident(&self, node_id: &str, r: &IncidentRef) -> std::io::Result<()> {
        let mut idx = self.read_incident(node_id)?;
        if !idx
            .edges
            .iter()
            .any(|e| e.edge_type == r.edge_type && e.key == r.key)
        {
            idx.edges.push(r.clone());
            self.commit_json(&self.incident_path(node_id), &idx)?;
        }
        Ok(())
    }

    fn drop_incident(&self, node_id: &str, edge_type: &str, key: &str) -> std::io::Result<()> {
        let mut idx = self.read_incident(node_id)?;
        let before = idx.edges.len();
        idx.edges
            .retain(|e| !(e.edge_type == edge_type && e.key == key));
        if idx.edges.len() != before {
            if idx.edges.is_empty() {
                self.remove_if_exists(&self.incident_path(node_id))?;
            } else {
                self.commit_json(&self.incident_path(node_id), &idx)?;
            }
        }
        Ok(())
    }
}

/// Every `label` directory under `{root}/.conduit/guard/nodes/` — the set of
/// labels a node with a given id could have been projected under. Small in
/// practice; missing dir → empty.
fn read_label_dirs(root: &Path) -> Vec<String> {
    let dir = root.join(".conduit").join("guard").join("nodes");
    match std::fs::read_dir(&dir) {
        Ok(entries) => entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().is_dir())
            .filter_map(|e| e.file_name().into_string().ok())
            .collect(),
        Err(_) => Vec::new(),
    }
}

fn ge_err(e: std::io::Error) -> GraphError {
    GraphError::WriteFailed(e.to_string())
}

impl GraphBackend for FileGraphBackend<'_> {
    fn read_node_guard(
        &mut self,
        label: &str,
        node_id: &str,
    ) -> Result<Option<GraphGuard>, GraphError> {
        self.read_guard(&self.node_guard_path(label, node_id))
            .map_err(ge_err)
    }

    fn read_edge_guard(
        &mut self,
        edge_type: &str,
        key: &str,
    ) -> Result<Option<GraphGuard>, GraphError> {
        self.read_guard(&self.edge_guard_path(edge_type, key))
            .map_err(ge_err)
    }

    fn node_is_tombstoned(&mut self, node_id: &str) -> Result<bool, GraphError> {
        for label in read_label_dirs(self.root) {
            if let Some(g) = self
                .read_guard(&self.node_guard_path(&label, node_id))
                .map_err(ge_err)?
                && g.deleted
            {
                return Ok(true);
            }
        }
        Ok(false)
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
            let value_path = self.node_value_path(label, node_id);
            if !facet.is_empty() {
                let facet_obj = properties.as_object().ok_or_else(|| {
                    GraphError::BuildFailed(
                        "node facet mapping must resolve to a JSON object".into(),
                    )
                })?;
                let mut existing = match std::fs::read_to_string(&value_path) {
                    Ok(s) => serde_json::from_str::<Value>(&s)
                        .unwrap_or_else(|_| Value::Object(Default::default())),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        Value::Object(Default::default())
                    }
                    Err(e) => return Err(ge_err(e)),
                };
                let obj = existing.as_object_mut().ok_or_else(|| {
                    GraphError::BuildFailed("existing node document is not a JSON object".into())
                })?;
                for (k, v) in facet_obj {
                    obj.insert(k.clone(), v.clone());
                }
                self.commit_json(&value_path, &existing).map_err(ge_err)?;
            } else {
                self.commit_json(&value_path, properties).map_err(ge_err)?;
            }
        }
        self.commit_json(&self.node_guard_path(label, node_id), guard)
            .map_err(ge_err)
    }

    fn write_edge(
        &mut self,
        edge_type: &str,
        from: &str,
        to: &str,
        key: &str,
        properties: Option<&Value>,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        if let Some(properties) = properties {
            self.commit_json(&self.edge_value_path(edge_type, key), properties)
                .map_err(ge_err)?;
            let r = IncidentRef {
                edge_type: edge_type.to_string(),
                key: key.to_string(),
                from: from.to_string(),
                to: to.to_string(),
            };
            for endpoint in dedup_pair(from, to) {
                self.add_incident(endpoint, &r).map_err(ge_err)?;
            }
        }
        self.commit_json(&self.edge_guard_path(edge_type, key), guard)
            .map_err(ge_err)
    }

    fn delete_node_cascade(
        &mut self,
        label: &str,
        node_id: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        self.remove_if_exists(&self.node_value_path(label, node_id))
            .map_err(ge_err)?;
        let incident = self.read_incident(node_id).map_err(ge_err)?;
        for r in &incident.edges {
            self.remove_if_exists(&self.edge_value_path(&r.edge_type, &r.key))
                .map_err(ge_err)?;
            let egp = self.edge_guard_path(&r.edge_type, &r.key);
            let mut eg = self.read_guard(&egp).map_err(ge_err)?.unwrap_or_default();
            eg.last_sequence = guard.last_sequence;
            eg.last_event_id = guard.last_event_id.clone();
            eg.deleted = true;
            eg.permanent = guard.permanent;
            self.commit_json(&egp, &eg).map_err(ge_err)?;
            for other in [&r.from, &r.to] {
                if other.as_str() != node_id {
                    self.drop_incident(other, &r.edge_type, &r.key)
                        .map_err(ge_err)?;
                }
            }
        }
        self.remove_if_exists(&self.incident_path(node_id))
            .map_err(ge_err)?;
        self.commit_json(&self.node_guard_path(label, node_id), guard)
            .map_err(ge_err)
    }

    fn delete_edge(
        &mut self,
        edge_type: &str,
        from: &str,
        to: &str,
        key: &str,
        guard: &GraphGuard,
    ) -> Result<(), GraphError> {
        self.remove_if_exists(&self.edge_value_path(edge_type, key))
            .map_err(ge_err)?;
        for endpoint in dedup_pair(from, to) {
            self.drop_incident(endpoint, edge_type, key)
                .map_err(ge_err)?;
        }
        self.commit_json(&self.edge_guard_path(edge_type, key), guard)
            .map_err(ge_err)
    }
}

fn dedup_pair<'a>(a: &'a str, b: &'a str) -> Vec<&'a str> {
    if a == b { vec![a] } else { vec![a, b] }
}
