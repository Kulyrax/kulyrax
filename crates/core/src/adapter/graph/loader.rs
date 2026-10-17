use std::collections::HashMap;
use std::path::Path;

use crate::adapter::graph::mapping::GraphMapping;
use crate::adapter::loader::load_mapping_dir;

/// Load every `GraphMapping` from `.yaml` / `.yml` files under `dir`, subfolders included,
/// keyed by event type. See `docs/mapping-reference.md` for the file rules.
pub fn load_graph_mappings<P: AsRef<Path>>(
    dir: P,
) -> Result<HashMap<String, GraphMapping>, String> {
    load_mapping_dir(dir.as_ref(), "graph", |m: &GraphMapping| m.event())
}
