//! Phase 17.1: `directory` source — watches a directory for event files
//! (`*.json` single or array-less JSON, `*.ndjson` / `*.jsonl` one event per
//! line) and hands them over in file-name order. Formalises today's `replay`
//! loader as a resumable, checkpointed source.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};

use super::checkpoint::{read_checkpoint, write_checkpoint};
use super::{EventSource, SourceError, SourcePosition, SourcedEvent};
use crate::replay::events_from_path;

pub struct DirectorySource {
    id: String,
    path: PathBuf,
    state_dir: PathBuf,
    committed: Option<SourcePosition>,
    /// Event files listed but not yet passed by the committed position, in
    /// name order. Polls read from the front; the directory is listed again
    /// only when this runs out, so a backlog of files is listed once rather
    /// than on every poll. A new listing adds only names after the last one
    /// listed: files must appear in name order (a file named before one
    /// already listed is not read, as a file named before the committed
    /// position never was).
    listed: VecDeque<String>,
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
            listed: VecDeque::new(),
        })
    }

    /// Append to `listed`, in name order, the event files named after the
    /// last one already listed and not yet passed by the committed position
    /// `after` (a file taken in parts, `(name, Some(_))`, is not passed yet).
    fn list_more(&mut self, after: Option<&(String, Option<usize>)>) -> Result<(), SourceError> {
        let last = self.listed.back().cloned();
        let wanted = |name: &str| {
            last.as_deref().is_none_or(|l| name > l)
                && match after {
                    None => true,
                    Some((done, None)) => name > done.as_str(),
                    Some((done, Some(_))) => name >= done.as_str(),
                }
        };
        let mut names: Vec<String> = std::fs::read_dir(&self.path)?
            .filter_map(|e| e.ok())
            // The entry's own type: no extra metadata call per file.
            .filter(|e| e.file_type().is_ok_and(|t| t.is_file()))
            .filter_map(|e| e.file_name().into_string().ok())
            .filter(|name| wanted(name))
            .filter(|name| {
                Path::new(name)
                    .extension()
                    .and_then(|s| s.to_str())
                    .map(|ext| {
                        let ext = ext.to_ascii_lowercase();
                        ext == "json" || ext == "ndjson" || ext == "jsonl"
                    })
                    .unwrap_or(false)
            })
            .collect();
        names.sort();
        self.listed.extend(names);
        Ok(())
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

        // Files the committed position has passed.
        while let Some(front) = self.listed.front() {
            let passed = match &after {
                Some((done, None)) => front.as_str() <= done.as_str(),
                Some((done, Some(_))) => front.as_str() < done.as_str(),
                None => false,
            };
            if !passed {
                break;
            }
            self.listed.pop_front();
        }

        let mut relisted = false;
        let mut i = 0;
        loop {
            if i == self.listed.len() {
                if relisted {
                    break;
                }
                self.list_more(after.as_ref())?;
                relisted = true;
                continue;
            }
            let name = self.listed[i].clone();
            i += 1;
            // Events of this file already committed: up to the committed
            // index of a split file.
            let skip = match &after {
                Some((done, Some(index))) if name == *done => index + 1,
                _ => 0,
            };
            let room = max_batch.saturating_sub(out.len());
            if room == 0 {
                break;
            }
            let events = events_from_path(self.path.join(&name))?
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
