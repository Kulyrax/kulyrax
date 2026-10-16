//! Phase 17.1: `directory` source — watches a directory for event files
//! (`*.json` single or array-less JSON, `*.ndjson` / `*.jsonl` one event per
//! line) and hands them over in file-name order. Formalises today's `replay`
//! loader as a resumable, checkpointed source.

use std::path::{Path, PathBuf};

use super::checkpoint::{read_checkpoint, write_checkpoint};
use super::{EventSource, SourceError, SourcePosition, SourcedEvent};
use crate::replay::events_from_path;

pub struct DirectorySource {
    id: String,
    path: PathBuf,
    state_dir: PathBuf,
    committed: Option<SourcePosition>,
}

impl DirectorySource {
    pub fn new(
        id: impl Into<String>,
        path: impl Into<PathBuf>,
        state_dir: impl Into<PathBuf>,
    ) -> Result<Self, SourceError> {
        let id = id.into();
        let state_dir = state_dir.into();
        let committed = read_checkpoint(&state_dir, &id)?.map(|c| c.position);
        Ok(Self {
            id,
            path: path.into(),
            state_dir,
            committed,
        })
    }

    /// Event files in the watched directory, in file-name order.
    fn event_files(&self) -> Result<Vec<PathBuf>, SourceError> {
        let mut files: Vec<PathBuf> = std::fs::read_dir(&self.path)?
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .filter(|p| p.is_file())
            .filter(|p| {
                p.extension()
                    .and_then(|s| s.to_str())
                    .map(|ext| {
                        let ext = ext.to_ascii_lowercase();
                        ext == "json" || ext == "ndjson" || ext == "jsonl"
                    })
                    .unwrap_or(false)
            })
            .collect();
        files.sort();
        Ok(files)
    }

    fn file_name(p: &Path) -> String {
        p.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default()
            .to_string()
    }
}

impl EventSource for DirectorySource {
    fn id(&self) -> &str {
        &self.id
    }

    fn poll(&mut self, max_batch: usize) -> Result<Vec<SourcedEvent>, SourceError> {
        let after = self.committed.as_ref().map(|p| p.0.clone());
        let mut out: Vec<SourcedEvent> = Vec::new();

        for file in self.event_files()? {
            let name = Self::file_name(&file);
            if after.as_deref().is_some_and(|a| name.as_str() <= a) {
                continue;
            }
            // A file is never split across batches — take whole files until the
            // next one would exceed `max_batch` (but always take at least one).
            if !out.is_empty() && out.len() >= max_batch {
                break;
            }
            let position = SourcePosition(name);
            for item in events_from_path(&file)? {
                let event = item.map_err(|e| SourceError::Parse(e.to_string()))?;
                out.push(SourcedEvent {
                    event,
                    position: position.clone(),
                });
            }
        }
        Ok(out)
    }

    fn commit(&mut self, position: SourcePosition) -> Result<(), SourceError> {
        if self.committed.as_ref().is_some_and(|c| &position <= c) {
            return Ok(()); // never moves backward
        }
        write_checkpoint(&self.state_dir, &self.id, &position)?;
        self.committed = Some(position);
        Ok(())
    }

    fn committed_position(&self) -> Option<&SourcePosition> {
        self.committed.as_ref()
    }
}
