//! Phase 17.2: source checkpoint sidecar — `{state_dir}/{source_id}.json`,
//! holding the committed [`SourcePosition`] and when it was recorded.
//! Structurally the same idea as the projection guard, kept deliberately
//! separate (input-side vs output-side state).

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use super::{SourceError, SourcePosition};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Checkpoint {
    pub position: SourcePosition,
    /// ISO-8601-ish wall time the commit was written (observability only).
    pub committed_at: String,
}

/// Path of a source's checkpoint file.
pub fn checkpoint_path(state_dir: &Path, source_id: &str) -> PathBuf {
    state_dir.join(format!("{source_id}.json"))
}

/// Read a source's committed position, or `Ok(None)` if it has never committed.
pub fn read_checkpoint(
    state_dir: &Path,
    source_id: &str,
) -> Result<Option<Checkpoint>, SourceError> {
    match std::fs::read_to_string(checkpoint_path(state_dir, source_id)) {
        Ok(s) => serde_json::from_str(&s)
            .map(Some)
            .map_err(|e| SourceError::Checkpoint(e.to_string())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(SourceError::Io(e)),
    }
}

/// Write a source's checkpoint via write-to-temp-then-rename, so a reader can
/// never observe a half-written file.
pub fn write_checkpoint(
    state_dir: &Path,
    source_id: &str,
    position: &SourcePosition,
) -> Result<(), SourceError> {
    std::fs::create_dir_all(state_dir)?;
    let cp = Checkpoint {
        position: position.clone(),
        committed_at: now_string(),
    };
    let path = checkpoint_path(state_dir, source_id);
    let tmp = state_dir.join(format!(".{source_id}.json.tmp-{}", std::process::id()));
    let body =
        serde_json::to_string_pretty(&cp).map_err(|e| SourceError::Checkpoint(e.to_string()))?;
    std::fs::write(&tmp, body)?;
    std::fs::rename(&tmp, &path)?;
    Ok(())
}

fn now_string() -> String {
    // Seconds since the Unix epoch — monotonic enough for an audit field, and
    // no external time-formatting dependency.
    match std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH) {
        Ok(d) => format!("{}", d.as_secs()),
        Err(_) => "0".to_string(),
    }
}
