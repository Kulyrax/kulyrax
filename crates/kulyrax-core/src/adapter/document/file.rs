use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;

use crate::{
    adapter::document::adapter::DocumentError,
    adapter::document::exec::{
        self, DocumentBackend, DocumentOutcome, DocumentPlan, ProjectionGuard,
    },
    adapter::document::runtime::DocumentRuntimeBuilder,
    adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter},
    event::Event,
    routing::StorageKind,
    runtime::config::MigrationPolicy,
    upcast::UpcasterRegistry,
};

/// File-based document adapter (Phase 4: idempotent; Phase 12: sequence-gated
/// upsert; Phase 13: sequence-gated delete & tombstones). Since Phase 23.1
/// the write path lives in [`exec::project`]; this file is the file-sidecar
/// driver — behaviour is byte-identical to the pre-23.1 inlined version.
pub struct FileDocumentAdapter {
    id: String,
    priority: u32,
    root: PathBuf,
    builder: DocumentRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
}

impl FileDocumentAdapter {
    pub fn new(
        id: String,
        root: PathBuf,
        priority: u32,
        builder: DocumentRuntimeBuilder,
        upcasters: Arc<UpcasterRegistry>,
        migration_policy: MigrationPolicy,
    ) -> Self {
        Self {
            id,
            root,
            priority,
            builder,
            upcasters,
            migration_policy,
        }
    }

    fn failure(&self, error: AdapterError) -> AdapterResult {
        AdapterResult::failure(self.id.clone(), StorageKind::Document, error)
    }
}

impl StorageAdapter for FileDocumentAdapter {
    fn kind(&self) -> StorageKind {
        StorageKind::Document
    }

    fn id(&self) -> &str {
        &self.id
    }

    fn priority(&self) -> u32 {
        self.priority
    }

    fn handle(&self, event: &Event) -> AdapterResult {
        // Build document projection (upcasting the payload to the mapping's target
        // version first). Entity identity is resolved as part of the projection
        // (Phase 11.1), so the idempotency guard below is checked by entity —
        // it can't be computed any earlier than this.
        let projection = match self.builder.build(event, &self.upcasters) {
            Ok(p) => p,
            Err(DocumentError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return match self.migration_policy {
                    MigrationPolicy::Strict => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::Document,
                        reason,
                        from_version,
                        to_version,
                    ),
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::Document,
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
        let plan = DocumentPlan {
            collection: projection.collection,
            entity_id: projection.entity_id,
            document: projection.document,
            on_existing: projection.on_existing,
            operation: projection.operation,
            permanent: projection.permanent,
            facet: projection.facet,
        };

        let mut backend = FileDocumentBackend { root: &self.root };

        match exec::project(&mut backend, &plan, event) {
            Ok(DocumentOutcome::Created) => AdapterResult::created_versioned(
                self.id.clone(),
                StorageKind::Document,
                source_version,
                projected_version,
            ),
            Ok(DocumentOutcome::Updated) => AdapterResult::updated_versioned(
                self.id.clone(),
                StorageKind::Document,
                source_version,
                projected_version,
            ),
            Ok(DocumentOutcome::Deleted) => AdapterResult::deleted_versioned(
                self.id.clone(),
                StorageKind::Document,
                source_version,
                projected_version,
            ),
            Ok(DocumentOutcome::Skipped(reason)) => AdapterResult::skipped_versioned(
                self.id.clone(),
                StorageKind::Document,
                reason,
                source_version,
                projected_version,
            ),
            Err(e) => self.failure(AdapterError::WriteFailed(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// FileDocumentBackend — the driver
// ---------------------------------------------------------------------------

struct FileDocumentBackend<'a> {
    root: &'a Path,
}

impl FileDocumentBackend<'_> {
    /// Entity output path, keyed by `(collection, entity_id)` (Phase 13.3) —
    /// the mapping's stable target identity, not `event.event_type`. A delete
    /// mapping has its own event type (`OrderCancelled` vs `OrderCreated`)
    /// but the same `collection`, so it points at the same file its create
    /// mapping wrote.
    fn output_path(&self, collection: &str, entity_id: &str) -> PathBuf {
        self.root
            .join(collection)
            .join(format!("{}.json", entity_id))
    }

    /// Idempotency guard path, keyed by `(collection, entity_id)` (Phase
    /// 11.3, re-keyed by collection in Phase 13.3) — not `event_id` or
    /// `event_type`, so every event type touching one entity shares one guard.
    fn guard_path(&self, collection: &str, entity_id: &str) -> PathBuf {
        self.root
            .join(".conduit")
            .join("entities")
            .join(collection)
            .join(format!("{}.done", entity_id))
    }

    /// Commit the guard via write-to-temp-then-rename, so a reader can never
    /// observe a partially-written guard file (closes the check-then-write
    /// race the old direct `fs::write` guard had).
    fn commit_guard(&self, guard: &Path, state: &ProjectionGuard) -> std::io::Result<()> {
        let parent = guard.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "idempotency guard path has no parent directory",
            )
        })?;
        std::fs::create_dir_all(parent)?;

        let tmp_name = format!(
            ".{}.tmp-{}",
            guard
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("guard"),
            std::process::id()
        );
        let tmp = parent.join(tmp_name);
        let content = serde_json::to_string(state)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        std::fs::write(&tmp, content)?;
        std::fs::rename(&tmp, guard)
    }
}

impl DocumentBackend for FileDocumentBackend<'_> {
    /// `Ok(None)` means no entity has been projected here yet; a corrupt
    /// sidecar is a write failure, not treated as absent (silently
    /// forgetting `last_sequence`/`deleted` would let a stale event through,
    /// or un-delete a tombstoned entity).
    fn read_guard(
        &mut self,
        collection: &str,
        entity_id: &str,
    ) -> Result<Option<ProjectionGuard>, DocumentError> {
        let path = self.guard_path(collection, entity_id);
        match std::fs::read_to_string(&path) {
            Ok(content) => {
                let parsed: ProjectionGuard = serde_json::from_str(&content).map_err(|e| {
                    DocumentError::WriteFailed(format!("corrupt guard sidecar: {e}"))
                })?;
                Ok(Some(parsed))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(DocumentError::WriteFailed(e.to_string())),
        }
    }

    fn write(
        &mut self,
        collection: &str,
        entity_id: &str,
        document: Option<&Value>,
        facet: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError> {
        if let Some(document) = document {
            let out_path = self.output_path(collection, entity_id);
            if !facet.is_empty() {
                // Phase 15.4: shallow-merge the facet's own top-level keys
                // into the existing document; every other key is preserved.
                let facet_obj = document.as_object().ok_or_else(|| {
                    DocumentError::BuildFailed("facet mapping must resolve to a JSON object".into())
                })?;
                let mut existing = match std::fs::read_to_string(&out_path) {
                    Ok(s) => serde_json::from_str::<Value>(&s)
                        .unwrap_or_else(|_| Value::Object(Default::default())),
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        Value::Object(Default::default())
                    }
                    Err(e) => return Err(DocumentError::WriteFailed(e.to_string())),
                };
                let existing_obj = existing.as_object_mut().ok_or_else(|| {
                    DocumentError::BuildFailed("existing document is not a JSON object".into())
                })?;
                for (k, v) in facet_obj {
                    existing_obj.insert(k.clone(), v.clone());
                }
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| DocumentError::WriteFailed(e.to_string()))?;
                }
                std::fs::write(
                    &out_path,
                    serde_json::to_string_pretty(&existing).unwrap_or_default(),
                )
                .map_err(|e| DocumentError::WriteFailed(e.to_string()))?;
            } else {
                if let Some(parent) = out_path.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| DocumentError::WriteFailed(e.to_string()))?;
                }
                std::fs::write(
                    &out_path,
                    serde_json::to_string_pretty(document).unwrap_or_default(),
                )
                .map_err(|e| DocumentError::WriteFailed(e.to_string()))?;
            }
        }
        let guard_path = self.guard_path(collection, entity_id);
        self.commit_guard(&guard_path, guard)
            .map_err(|e| DocumentError::WriteFailed(e.to_string()))
    }

    fn delete(
        &mut self,
        collection: &str,
        entity_id: &str,
        guard: &ProjectionGuard,
    ) -> Result<(), DocumentError> {
        let out_path = self.output_path(collection, entity_id);
        match std::fs::remove_file(&out_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(DocumentError::WriteFailed(e.to_string())),
        }
        let guard_path = self.guard_path(collection, entity_id);
        self.commit_guard(&guard_path, guard)
            .map_err(|e| DocumentError::WriteFailed(e.to_string()))
    }
}
