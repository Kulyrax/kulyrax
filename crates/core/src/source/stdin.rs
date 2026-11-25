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

/// Position width: zero-padded, for readability. The source orders positions
/// by their line number, never as text (decision
/// 2026-11-25-source-position-contract).
const POS_WIDTH: usize = 12;

pub struct StdinSource<R: BufRead> {
    id: String,
    state_dir: PathBuf,
    reader: R,
    /// Index of the next line to yield (0-based).
    next_line: u64,
    committed: Option<SourcePosition>,
    /// The line `committed` names.
    committed_line: Option<u64>,
    exhausted: bool,
}

fn encode_pos(line: u64) -> SourcePosition {
    SourcePosition(format!("{line:0POS_WIDTH$}"))
}

/// The line number a position names, or `None` if it names none.
fn decode_pos(p: &SourcePosition) -> Option<u64> {
    if p.0.is_empty() || !p.0.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    p.0.parse().ok()
}

fn not_a_line(id: &str, what: &str, p: &SourcePosition) -> SourceError {
    SourceError::Checkpoint(format!(
        "source {id:?}: {what} {:?} is not a stdin line number",
        p.0
    ))
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
        let committed_line = match &committed {
            Some(p) => Some(decode_pos(p).ok_or_else(|| not_a_line(&id, "checkpoint", p))?),
            None => None,
        };
        Ok(Self {
            id,
            state_dir,
            reader,
            next_line: 0,
            committed,
            committed_line,
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
        let skip_through = self.committed_line;
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
        let line =
            decode_pos(&position).ok_or_else(|| not_a_line(&self.id, "position", &position))?;
        if self.committed_line.is_some_and(|c| line <= c) {
            return Ok(()); // never moves backward
        }
        write_checkpoint(&self.state_dir, &self.id, &position)?;
        self.committed = Some(position);
        self.committed_line = Some(line);
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
    fn positions_round_trip_and_only_line_numbers_decode() {
        assert_eq!(decode_pos(&encode_pos(0)), Some(0));
        assert_eq!(decode_pos(&encode_pos(42)), Some(42));
        // Past the padding width: still a line number, ordered as one.
        let big = 10u64.pow(13);
        assert_eq!(decode_pos(&encode_pos(big)), Some(big));
        for bad in ["", "abc", "-1", "12a", " 1"] {
            assert_eq!(decode_pos(&SourcePosition(bad.into())), None, "{bad:?}");
        }
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
