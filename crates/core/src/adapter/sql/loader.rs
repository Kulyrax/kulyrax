use std::collections::HashMap;
use std::path::Path;

use crate::adapter::loader::load_mapping_dir;
use crate::adapter::sql::mapping::SqlMapping;

/// Load every `SqlMapping` from `.yaml` / `.yml` files under `dir`, subfolders included,
/// keyed by event type. See `docs/mapping-reference.md` for the file rules.
pub fn load_sql_mappings<P: AsRef<Path>>(
    dir: P,
) -> Result<HashMap<String, SqlMapping>, crate::pipeline::LoadError> {
    load_mapping_dir(
        dir.as_ref(),
        "SQL",
        |m: &SqlMapping| m.event.as_str(),
        |m: &SqlMapping| {
            crate::adapter::reserved::sql_table(&m.table)
                .map(|why| ("table", m.table.clone(), why))
                .into_iter()
                .collect()
        },
    )
}
