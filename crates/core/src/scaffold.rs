//! `kulyrax init`: write a small, runnable starter project.
//!
//! The starter projects into the file-backed document adapter because it
//! needs no setup. Kulyrax never creates a user's SQL tables, so a SQL-based
//! starter could not run until the user did. The config carries a commented
//! SQLite adapter as the next step.

use std::fmt;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

const CONFIG: &str = r#"# Kulyrax project. Run from this folder:
#   kulyrax run --config config.yaml --mappings mappings --once
#
# Source paths are relative to this file; adapter paths (root, path) are
# relative to the folder you run kulyrax from.
version: 1

routing:
  file: routing.yaml

adapters:
  # Writes one JSON file per entity under ./out. Needs no database.
  - type: file
    id: docs
    priority: 10
    config:
      root: ./out

  # To project into SQLite as well: create the table yourself first (Kulyrax
  # never creates your tables), add a mapping under mappings/sql/, uncomment
  # this adapter, and add `sql` to the event's list in routing.yaml.
  # - type: sqlite
  #   id: sql
  #   priority: 20
  #   capabilities: [write, upsert]
  #   config:
  #     path: ./app.db

sources:
  # Reads *.json / *.ndjson / *.jsonl event files from ./events and remembers
  # how far it got in ./.kulyrax/state. Delete that folder to replay.
  - type: directory
    id: events
    path: ./events
    state_dir: ./.kulyrax/state
"#;

const ROUTING: &str = r#"# event_type: [adapter ids it reaches]
UserRegistered: [docs]
"#;

const DOCUMENT_MAPPING: &str = r#"# One mapping per event type. Values are paths into the event:
# payload.<field> or metadata.<field>.
event: UserRegistered
collection: users
id: payload.id
version: 1
document:
  name: payload.name
  email: payload.email
"#;

/// Events are JSON, one per line. `payload` is a JSON string, and `sequence`
/// increases per entity: it is what makes redelivery safe.
const EVENTS: &str = concat!(
    r#"{"event_id":"e1","event_type":"UserRegistered","payload":"{\"id\":\"u1\",\"name\":\"Ada\",\"email\":\"ada@example.com\"}","metadata":{},"version":1,"sequence":1}"#,
    "\n",
    r#"{"event_id":"e2","event_type":"UserRegistered","payload":"{\"id\":\"u2\",\"name\":\"Grace\",\"email\":\"grace@example.com\"}","metadata":{},"version":1,"sequence":1}"#,
    "\n",
);

const GITIGNORE: &str = "out/\n.kulyrax/\napp.db\n";

/// Every file the starter writes, relative to the project folder.
/// `mappings/sql/` must exist even when empty, hence its `.gitkeep`.
const FILES: &[(&str, &str)] = &[
    ("config.yaml", CONFIG),
    ("routing.yaml", ROUTING),
    ("mappings/document/user_registered.yaml", DOCUMENT_MAPPING),
    ("mappings/sql/.gitkeep", ""),
    ("events/0001-users.ndjson", EVENTS),
    (".gitignore", GITIGNORE),
];

#[derive(Debug)]
pub enum InitError {
    /// Nothing was written because these files already exist.
    WouldOverwrite(Vec<PathBuf>),
    Io(io::Error),
}

impl fmt::Display for InitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            InitError::WouldOverwrite(paths) => {
                write!(f, "not overwriting existing files, nothing was written:")?;
                for path in paths {
                    write!(f, "\n  {}", path.display())?;
                }
                Ok(())
            }
            InitError::Io(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for InitError {}

impl From<io::Error> for InitError {
    fn from(e: io::Error) -> Self {
        InitError::Io(e)
    }
}

/// Write the starter project into `dir`, creating it if needed. Returns the
/// files written. Refuses, writing nothing, if any of them already exists.
pub fn init_project(dir: &Path) -> Result<Vec<PathBuf>, InitError> {
    let targets: Vec<(PathBuf, &str)> = FILES
        .iter()
        .map(|(relative, content)| {
            let path = relative
                .split('/')
                .fold(dir.to_path_buf(), |p, part| p.join(part));
            (path, *content)
        })
        .collect();

    let existing: Vec<PathBuf> = targets
        .iter()
        .filter(|(path, _)| path.exists())
        .map(|(path, _)| path.clone())
        .collect();
    if !existing.is_empty() {
        return Err(InitError::WouldOverwrite(existing));
    }

    let mut written = Vec::with_capacity(targets.len());
    for (path, content) in targets {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, content)?;
        written.push(path);
    }
    Ok(written)
}
