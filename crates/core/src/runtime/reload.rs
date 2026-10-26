//! `kulyrax reload`: ask a running `run` / `ingest` process to re-read its
//! config, routing and mappings without restarting.
//!
//! The two sides meet in `<config folder>/.kulyrax/control/`, so the command
//! needs only the path of the same config file the process was started with.
//! The command writes `reload-request` holding a fresh token. Between batches
//! the process takes the request, loads and validates the project, builds new
//! adapters, and writes `reload-result` with the token and the outcome. An
//! invalid change is rejected and the process keeps running unchanged.
//!
//! `sources:` cannot change on reload: source positions are checkpointed per
//! source, so a changed source list needs a restart.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::pipeline::PipelineError;
use crate::runtime::config::SourceConfig;
use crate::runtime::engine::KulyraxRuntime;
use crate::source::runner::{Reloaded, Reloader};
use crate::upcast::UpcasterRegistry;

const REQUEST_FILE: &str = "reload-request";
const RESULT_FILE: &str = "reload-result";

/// Where reload requests and results for `config_path` live.
pub fn control_dir(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join(".kulyrax")
        .join("control")
}

/// What the running process did with a reload request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReloadOutcome {
    Applied {
        message: String,
    },
    Rejected {
        reason: String,
    },
    /// No process picked the request up in time.
    NotPickedUp,
}

/// Re-reads the project when a reload request appears, for
/// [`KulyraxRuntime::run_sources_with_reload`].
pub struct FileReloader {
    config_path: PathBuf,
    mappings_dir: PathBuf,
    control_dir: PathBuf,
    sources: Vec<SourceConfig>,
    register_backends: Box<dyn Fn(&mut KulyraxRuntime)>,
    upcasters: Arc<UpcasterRegistry>,
    on_outcome: Box<dyn FnMut(&ReloadOutcome)>,
}

impl FileReloader {
    /// `sources` is the source list the process started with; a reload that
    /// changes it is rejected. `on_outcome` is told about every reload, for
    /// logging.
    pub fn new(
        config_path: &Path,
        mappings_dir: &Path,
        sources: Vec<SourceConfig>,
        register_backends: impl Fn(&mut KulyraxRuntime) + 'static,
        upcasters: Arc<UpcasterRegistry>,
        on_outcome: impl FnMut(&ReloadOutcome) + 'static,
    ) -> Self {
        Self {
            config_path: config_path.to_path_buf(),
            mappings_dir: mappings_dir.to_path_buf(),
            control_dir: control_dir(config_path),
            sources,
            register_backends: Box::new(register_backends),
            upcasters,
            on_outcome: Box::new(on_outcome),
        }
    }

    fn load(&self) -> Result<Reloaded, String> {
        let mut runtime = KulyraxRuntime::load(&self.config_path, &self.mappings_dir)
            .map_err(|e| e.to_string())?
            .with_upcasters(Arc::clone(&self.upcasters));
        if runtime.config().sources != self.sources {
            return Err("`sources:` changed; restart the process to apply that".to_string());
        }
        (self.register_backends)(&mut runtime);
        runtime
            .into_reloaded()
            .map_err(|e: PipelineError| e.to_string())
    }
}

impl Reloader for FileReloader {
    fn poll(&mut self) -> Option<Reloaded> {
        let request = self.control_dir.join(REQUEST_FILE);
        let token = fs::read_to_string(&request).ok()?;
        // Take the request first so a failing reload is not retried forever.
        let _ = fs::remove_file(&request);

        let (outcome, reloaded) = match self.load() {
            Ok(reloaded) => (
                ReloadOutcome::Applied {
                    message: format!(
                        "{} adapter(s), {} routed event type(s)",
                        reloaded.adapters.len(),
                        reloaded.routing_rules.len()
                    ),
                },
                Some(reloaded),
            ),
            Err(reason) => (ReloadOutcome::Rejected { reason }, None),
        };
        // The requester may already have given up; the process carries on
        // either way, so a failed result write is not an error here.
        let _ = write_atomically(
            &self.control_dir.join(RESULT_FILE),
            &encode_result(token.trim(), &outcome),
        );
        (self.on_outcome)(&outcome);
        reloaded
    }
}

/// Ask the process running with `config_path` to reload, and wait up to
/// `timeout` for its answer.
pub fn request_reload(config_path: &Path, timeout: Duration) -> std::io::Result<ReloadOutcome> {
    let dir = control_dir(config_path);
    fs::create_dir_all(&dir)?;
    let token = new_token();
    let request = dir.join(REQUEST_FILE);
    write_atomically(&request, &token)?;

    let result = dir.join(RESULT_FILE);
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let answer = fs::read_to_string(&result)
            .ok()
            .and_then(|content| decode_result(&content, &token));
        if let Some(outcome) = answer {
            return Ok(outcome);
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    // Withdraw our request so a process started later doesn't act on it.
    if fs::read_to_string(&request).is_ok_and(|t| t.trim() == token) {
        let _ = fs::remove_file(&request);
    }
    Ok(ReloadOutcome::NotPickedUp)
}

fn new_token() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_nanos());
    format!("{}-{}", std::process::id(), nanos)
}

/// Result file: the request token, `applied` or `rejected`, then the message.
fn encode_result(token: &str, outcome: &ReloadOutcome) -> String {
    match outcome {
        ReloadOutcome::Applied { message } => format!("{token}\napplied\n{message}\n"),
        ReloadOutcome::Rejected { reason } => format!("{token}\nrejected\n{reason}\n"),
        ReloadOutcome::NotPickedUp => format!("{token}\nnot-picked-up\n"),
    }
}

/// `None` when the file answers some other request.
fn decode_result(content: &str, token: &str) -> Option<ReloadOutcome> {
    let mut lines = content.splitn(3, '\n');
    if lines.next()? != token {
        return None;
    }
    let status = lines.next()?;
    let message = lines.next().unwrap_or("").trim_end().to_string();
    Some(match status {
        "applied" => ReloadOutcome::Applied { message },
        "rejected" => ReloadOutcome::Rejected { reason: message },
        _ => ReloadOutcome::NotPickedUp,
    })
}

/// Write via a temporary file and rename, so a reader never sees half a file.
fn write_atomically(path: &Path, content: &str) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, content)?;
    fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn result_round_trips_and_ignores_other_tokens() {
        let applied = ReloadOutcome::Applied {
            message: "2 adapter(s)".into(),
        };
        let encoded = encode_result("t1", &applied);
        assert_eq!(decode_result(&encoded, "t1"), Some(applied));
        assert_eq!(decode_result(&encoded, "t2"), None);

        let rejected = ReloadOutcome::Rejected {
            reason: "Invalid routing file x: bad\nsecond line".into(),
        };
        assert_eq!(
            decode_result(&encode_result("t1", &rejected), "t1"),
            Some(rejected)
        );
    }

    #[test]
    fn unanswered_request_is_withdrawn() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = dir.path().join("config.yaml");

        let outcome = request_reload(&config, Duration::from_millis(250)).expect("request");

        assert_eq!(outcome, ReloadOutcome::NotPickedUp);
        assert!(!control_dir(&config).join(REQUEST_FILE).exists());
    }
}
