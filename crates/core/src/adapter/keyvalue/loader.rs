use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::adapter::keyvalue::mapping::KvMapping;

pub fn load_keyvalue_mappings<P: AsRef<Path>>(
    dir: P,
) -> Result<HashMap<String, KvMapping>, String> {
    let mut mappings = HashMap::new();

    let entries =
        fs::read_dir(&dir).map_err(|e| format!("Failed to read key-value mappings dir: {}", e))?;

    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        // Only YAML files
        if path.extension().and_then(|s| s.to_str()) != Some("yaml") {
            continue;
        }

        let content =
            fs::read_to_string(&path).map_err(|e| format!("Failed to read {:?}: {}", path, e))?;

        let mapping: KvMapping = serde_yaml::from_str(&content)
            .map_err(|e| format!("Invalid YAML in {:?}: {}", path, e))?;

        if mappings.contains_key(&mapping.event) {
            return Err(format!(
                "Duplicate key-value mapping for event '{}'",
                mapping.event
            ));
        }

        mappings.insert(mapping.event.clone(), mapping);
    }

    Ok(mappings)
}
