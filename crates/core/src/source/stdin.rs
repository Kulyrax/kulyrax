//! Phase 17.1: `stdin` source — NDJSON on standard input, one event per line
//! (`cat events.ndjson | kulyrax run`). Generic over the reader so tests can
//! feed a `Cursor` instead of the real stdin.
//!
//! A checkpoint is still written for consistency, but note (Phase 17 non-goal):
//! a pipe is not replayable across process restarts — stdin resume is only
//! meaningful *within* one run.

use std::io::BufRead;
use std::path::PathBuf;

use super::checkpoint::{read_checkpoint, write_checkpoint};
use super::{EventSource, SourceError, SourcePosition, SourcedEvent};
use crate::event::Event;

/// Position width — zero-padded so string order matches line order.
const POS_WIDTH: usize = 12;

pub struct StdinSource<R: BufRead> {
    id: String,
    state_dir: PathBuf,
    reader: R,
    /// Index of the next line to yield (0-based).
    next_line: u64,
    committed: Option<SourcePosition>,
    exhausted: bool,
}

fn encode_pos(line: u64) -> SourcePosition {
    SourcePosition(format!("{line:0POS_WIDTH$}"))
}

fn decode_pos(p: &SourcePosition) -> u64 {
    let trimmed = p.0.trim_start_matches('0');
    if trimmed.is_empty() {
        0
    } else {
        trimmed.parse().unwrap_or(0)
    }
}

impl<R: BufRead> StdinSource<R> {
    pub fn new(
        id: impl Into<String>,
        state_dir: impl Into<PathBuf>,
        reader: R,
    ) -> Result<Self, SourceError> {
        let id = id.into();
        let state_dir = state_dir.into();
        let committed = read_checkpoint(&state_dir, &id)?.map(|c| c.position);
        Ok(Self {
            id,
            state_dir,
            reader,
            next_line: 0,
            committed,
            exhausted: false,
        })
    }
}

impl<R: BufRead> EventSource for StdinSource<R> {
    fn id(&self) -> &str {
        &self.id
    }

    fn poll(&mut self, max_batch: usize) -> Result<Vec<SourcedEvent>, SourceError> {
        if self.exhausted || max_batch == 0 {
            return Ok(Vec::new());
        }
        let skip_through = self.committed.as_ref().map(decode_pos);
        let mut out: Vec<SourcedEvent> = Vec::new();
        let mut line = String::new();

        while out.len() < max_batch {
            line.clear();
            let n = self.reader.read_line(&mut line)?;
            if n == 0 {
                self.exhausted = true;
                break;
            }
            let idx = self.next_line;
            self.next_line += 1;

            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            if skip_through.is_some_and(|c| idx <= c) {
                continue; // already committed (within-run resume)
            }
            let event: Event = serde_json::from_str(trimmed)
                .map_err(|e| SourceError::Parse(format!("line {idx}: {e}")))?;
            out.push(SourcedEvent {
                event,
                position: encode_pos(idx),
            });
        }
        Ok(out)
    }

    fn commit(&mut self, position: SourcePosition) -> Result<(), SourceError> {
        if self.committed.as_ref().is_some_and(|c| &position <= c) {
            return Ok(());
        }
        write_checkpoint(&self.state_dir, &self.id, &position)?;
        self.committed = Some(position);
        Ok(())
    }

    fn committed_position(&self) -> Option<&SourcePosition> {
        self.committed.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn position_encoding_is_lexicographically_ordered() {
        assert!(encode_pos(2) < encode_pos(10));
        assert!(encode_pos(9) < encode_pos(100));
        assert_eq!(decode_pos(&encode_pos(0)), 0);
        assert_eq!(decode_pos(&encode_pos(42)), 42);
    }

    #[test]
    fn polls_one_event_per_nonblank_line_then_reports_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let ndjson = "\n{\"event_id\":\"a\",\"event_type\":\"T\",\"payload\":\"{}\",\"metadata\":{},\"version\":1,\"sequence\":1}\n\n{\"event_id\":\"b\",\"event_type\":\"T\",\"payload\":\"{}\",\"metadata\":{},\"version\":1,\"sequence\":2}\n";
        let mut s =
            StdinSource::new("in", tmp.path(), Cursor::new(ndjson.as_bytes().to_vec())).unwrap();
        let batch = s.poll(10).unwrap();
        assert_eq!(batch.len(), 2);
        assert_eq!(batch[0].event.event_id, "a");
        assert_eq!(batch[1].event.event_id, "b");
        assert!(s.poll(10).unwrap().is_empty());
    }
}
