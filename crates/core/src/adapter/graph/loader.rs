use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::adapter::graph::mapping::GraphMapping;

pub fn load_graph_mappings<P: AsRef<Path>>(
    dir: P,
) -> Result<HashMap<String, GraphMapping>, String> {
    let mut mappings = HashMap::new();

    let entries =
        fs::read_dir(&dir).map_err(|e| format!("Failed to read graph mappings dir: {}", e))?;

    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        if path.extension().and_then(|s| s.to_str()) != Some("yaml") {
            continue;
        }

        let content =
            fs::read_to_string(&path).map_err(|e| format!("Failed to read {:?}: {}", path, e))?;

        let mapping: GraphMapping = serde_yaml::from_str(&content)
            .map_err(|e| format!("Invalid YAML in {:?}: {}", path, e))?;

        let event = mapping.event().to_string();
        if mappings.contains_key(&event) {
            return Err(format!("Duplicate graph mapping for event '{}'", event));
        }

        mappings.insert(event, mapping);
    }

    Ok(mappings)
}
