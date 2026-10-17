use std::{collections::HashMap, fs, path::Path};

use crate::adapter::sql::mapping::SqlMapping;

pub fn load_sql_mappings<P: AsRef<Path>>(dir: P) -> Result<HashMap<String, SqlMapping>, String> {
    let mut mappings = HashMap::new();

    let entries =
        fs::read_dir(&dir).map_err(|e| format!("Failed to read SQL mappings dir: {}", e))?;

    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        let path = entry.path();

        // Only YAML files
        if path.extension().and_then(|s| s.to_str()) != Some("yaml") {
            continue;
        }

        let content =
            fs::read_to_string(&path).map_err(|e| format!("Failed to read {:?}: {}", path, e))?;

        let mapping: SqlMapping = serde_yaml::from_str(&content)
            .map_err(|e| format!("Invalid YAML in {:?}: {}", path, e))?;

        // Enforce one mapping per event
        if mappings.contains_key(&mapping.event) {
            return Err(format!(
                "Duplicate SQL mapping for event '{}'",
                mapping.event
            ));
        }

        mappings.insert(mapping.event.clone(), mapping);
    }

    Ok(mappings)
}
