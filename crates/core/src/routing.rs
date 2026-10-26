use core::fmt;
use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::event::Event;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageKind {
    Sql,
    Document,
    KeyValue,
    Graph,
    /// Phase 25.2: reported only by the internal `FailedAdapter` placeholder
    /// pushed for an `AdapterConfig::Custom` whose `type:` has no factory
    /// registered at build time — never by a real (built-in or registered)
    /// adapter, which always reports one of the four kinds above.
    Custom,
}

pub type AdapterId = String;

impl fmt::Display for StorageKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            StorageKind::Sql => "sql",
            StorageKind::Document => "document",
            Self::KeyValue => "keyvalue",
            Self::Graph => "graph",
            Self::Custom => "custom",
        };
        write!(f, "{s}")
    }
}

/// Resolve adapter IDs for an event given explicit routing rules.
///
/// One event type may list several distinct adapters (e.g. `pgsql_master`, `pgsql_slave`); each
/// receives the same logical projection via shared mappings, with execution order by adapter priority.
pub fn route_with_rules(event: &Event, rules: &HashMap<String, Vec<AdapterId>>) -> Vec<AdapterId> {
    rules.get(&event.event_type).cloned().unwrap_or_default()
}

/// Load the routing table from a YAML file (`.yaml` or `.yml`): a map of
/// `event_type` to the adapter ids it reaches.
///
/// Every file a user writes is YAML, so a JSON routing file is rejected with
/// a message saying how to convert it rather than a bare parse error.
pub fn load_routing<P: AsRef<Path>>(path: P) -> Result<HashMap<String, Vec<AdapterId>>, String> {
    let path = path.as_ref();
    let extension = path
        .extension()
        .and_then(|ext| ext.to_str())
        .map(str::to_ascii_lowercase);
    match extension.as_deref() {
        Some("yaml" | "yml") => {}
        Some("json") => {
            return Err(format!(
                "JSON routing files are no longer supported ({}). Rename it to \
                 routing.yaml, write each entry as `EventType: [adapter-id, ...]`, \
                 and point `routing: file:` in the config at it",
                path.display()
            ));
        }
        _ => {
            return Err(format!(
                "Routing file must end in .yaml or .yml: {}",
                path.display()
            ));
        }
    }

    let data = fs::read_to_string(path)
        .map_err(|e| format!("Failed to read routing file {}: {e}", path.display()))?;
    serde_yaml::from_str(&data).map_err(|e| format!("Invalid routing file {}: {e}", path.display()))
}
