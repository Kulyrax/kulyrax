use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde_json::Value;

use crate::{
    adapter::keyvalue::adapter::KvError,
    adapter::keyvalue::exec::{self, KvBackend, KvGuard, KvOutcome, KvPlan},
    adapter::keyvalue::runtime::KvRuntimeBuilder,
    adapter::{AdapterError, AdapterResult, SkipReason, StorageAdapter},
    event::Event,
    routing::StorageKind,
    runtime::config::MigrationPolicy,
    upcast::UpcasterRegistry,
};

/// File-backed key-value store (Phase 14). Since Phase 22.1 the write path
/// lives in [`exec::project`]; this file is the file-sidecar driver —
/// behaviour is byte-identical to the pre-22.1 inlined version.
pub struct FileKvStore {
    id: String,
    priority: u32,
    root: PathBuf,
    builder: KvRuntimeBuilder,
    upcasters: Arc<UpcasterRegistry>,
    migration_policy: MigrationPolicy,
}

impl FileKvStore {
    pub fn new(
        id: String,
        root: PathBuf,
        priority: u32,
        builder: KvRuntimeBuilder,
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
        AdapterResult::failure(self.id.clone(), StorageKind::KeyValue, error)
    }
}

impl StorageAdapter for FileKvStore {
    fn kind(&self) -> StorageKind {
        StorageKind::KeyValue
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
            Err(KvError::UnsupportedVersion {
                from_version,
                to_version,
                reason,
                ..
            }) => {
                return match self.migration_policy {
                    MigrationPolicy::Strict => AdapterResult::unsupported_version(
                        self.id.clone(),
                        StorageKind::KeyValue,
                        reason,
                        from_version,
                        to_version,
                    ),
                    MigrationPolicy::IgnoreUnmatched => AdapterResult::skipped_versioned(
                        self.id.clone(),
                        StorageKind::KeyValue,
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
        let plan = KvPlan {
            namespace: projection.namespace,
            entity_key: projection.entity_key,
            value: projection.value,
            on_existing: projection.on_existing,
            operation: projection.operation,
            permanent: projection.permanent,
            facet: projection.facet,
        };

        let mut backend = FileKvBackend { root: &self.root };

        match exec::project(&mut backend, &plan, event) {
            Ok(KvOutcome::Created) => AdapterResult::created_versioned(
                self.id.clone(),
                StorageKind::KeyValue,
                source_version,
                projected_version,
            ),
            Ok(KvOutcome::Updated) => AdapterResult::updated_versioned(
                self.id.clone(),
                StorageKind::KeyValue,
                source_version,
                projected_version,
            ),
            Ok(KvOutcome::Deleted) => AdapterResult::deleted_versioned(
                self.id.clone(),
                StorageKind::KeyValue,
                source_version,
                projected_version,
            ),
            Ok(KvOutcome::Skipped(reason)) => AdapterResult::skipped_versioned(
                self.id.clone(),
                StorageKind::KeyValue,
                reason,
                source_version,
                projected_version,
            ),
            Err(e) => self.failure(AdapterError::WriteFailed(e.to_string())),
        }
    }
}

// ---------------------------------------------------------------------------
// FileKvBackend — the driver
// ---------------------------------------------------------------------------

struct FileKvBackend<'a> {
    root: &'a Path,
}

impl FileKvBackend<'_> {
    /// Value path, keyed by `(namespace, key)` (Phase 14.2).
    fn value_path(&self, namespace: &str, key: &str) -> PathBuf {
        self.root.join(namespace).join(format!("{}.json", key))
    }

    /// Idempotency guard path, keyed by `(namespace, key)`.
    fn guard_path(&self, namespace: &str, key: &str) -> PathBuf {
        self.root
            .join(".conduit")
            .join(namespace)
            .join(format!("{}.guard.json", key))
    }

    /// Commit the guard via write-to-temp-then-rename (Phase 11.3 pattern),
    /// so a reader can never observe a partially-written guard file.
    fn commit_guard(&self, guard: &Path, state: &KvGuard) -> std::io::Result<()> {
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

impl KvBackend for FileKvBackend<'_> {
    /// `Ok(None)` means this key has never been projected here; a corrupt
    /// sidecar is a write failure, not treated as absent (silently forgetting
    /// `last_sequence`/`deleted` would let a stale event through, or
    /// un-delete a tombstoned key).
    fn read_guard(&mut self, namespace: &str, key: &str) -> Result<Option<KvGuard>, KvError> {
        let path = self.guard_path(namespace, key);
        match std::fs::read_to_string(&path) {
            Ok(content) => {
                let parsed: KvGuard = serde_json::from_str(&content)
                    .map_err(|e| KvError::WriteFailed(format!("corrupt guard sidecar: {e}")))?;
                Ok(Some(parsed))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(KvError::WriteFailed(e.to_string())),
        }
    }

    fn read_value(&mut self, namespace: &str, key: &str) -> Result<Option<Value>, KvError> {
        let path = self.value_path(namespace, key);
        match std::fs::read_to_string(&path) {
            Ok(s) => Ok(Some(
                serde_json::from_str(&s).unwrap_or(Value::Object(Default::default())),
            )),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(KvError::WriteFailed(e.to_string())),
        }
    }

    fn write(
        &mut self,
        namespace: &str,
        key: &str,
        value: Option<&Value>,
        guard: &KvGuard,
    ) -> Result<(), KvError> {
        if let Some(value) = value {
            let value_path = self.value_path(namespace, key);
            if let Some(parent) = value_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| KvError::WriteFailed(e.to_string()))?;
            }
            std::fs::write(
                &value_path,
                serde_json::to_string_pretty(value).unwrap_or_default(),
            )
            .map_err(|e| KvError::WriteFailed(e.to_string()))?;
        }
        let guard_path = self.guard_path(namespace, key);
        self.commit_guard(&guard_path, guard)
            .map_err(|e| KvError::WriteFailed(e.to_string()))
    }

    fn delete(&mut self, namespace: &str, key: &str, guard: &KvGuard) -> Result<(), KvError> {
        let value_path = self.value_path(namespace, key);
        match std::fs::remove_file(&value_path) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(KvError::WriteFailed(e.to_string())),
        }
        let guard_path = self.guard_path(namespace, key);
        self.commit_guard(&guard_path, guard)
            .map_err(|e| KvError::WriteFailed(e.to_string()))
    }
}
