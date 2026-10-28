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

/// Position of the event at `index` (0-based) of a file taken in parts:
/// `<file>#<index>`, zero-padded so positions within the file sort in order,
/// after the file's own name and before the next file's. Once a file is
/// split, all its remaining events use this form, so the committed position
/// never moves back to the bare name.
fn split_position(file: &str, index: usize) -> SourcePosition {
    SourcePosition(format!("{file}#{index:012}"))
}

/// A committed position: the file, and for a file taken in parts, the index
/// of its last committed event.
fn parse_position(position: &str) -> (String, Option<usize>) {
    if let Some((file, index)) = position.rsplit_once('#')
        && let Ok(index) = index.parse::<usize>()
    {
        return (file.to_string(), Some(index));
    }
    (position.to_string(), None)
}

impl EventSource for DirectorySource {
    fn id(&self) -> &str {
        &self.id
    }

    /// Whole files while they fit in `max_batch`. A file that does not fit
    /// (or was partly taken already) is taken in parts: its events'
    /// positions are `<file>#<index>`, see [`split_position`].
    fn poll(&mut self, max_batch: usize) -> Result<Vec<SourcedEvent>, SourceError> {
        let max_batch = max_batch.max(1);
        let after = self.committed.as_ref().map(|p| parse_position(&p.0));
        let mut out: Vec<SourcedEvent> = Vec::new();

        for file in self.event_files()? {
            let name = Self::file_name(&file);
            // Events of this file already committed: all of a file before the
            // committed one, or up to the committed index of a split file.
            let skip = match &after {
                Some((done, _)) if name.as_str() < done.as_str() => continue,
                Some((done, None)) if name == *done => continue,
                Some((done, Some(index))) if name == *done => index + 1,
                _ => 0,
            };
            let room = max_batch.saturating_sub(out.len());
            if room == 0 {
                break;
            }
            let events = events_from_path(&file)?
                .map(|item| item.map_err(|e| SourceError::Parse(e.to_string())))
                .collect::<Result<Vec<_>, _>>()?;
            if skip >= events.len() {
                continue;
            }
            if skip == 0 && events.len() <= room {
                let position = SourcePosition(name);
                out.extend(events.into_iter().map(|event| SourcedEvent {
                    event,
                    position: position.clone(),
                }));
                continue;
            }
            out.extend(events.into_iter().enumerate().skip(skip).take(room).map(
                |(index, event)| SourcedEvent {
                    event,
                    position: split_position(&name, index),
                },
            ));
            break;
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
