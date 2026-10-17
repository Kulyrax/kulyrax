use std::collections::HashMap;
use std::path::Path;

use crate::adapter::keyvalue::mapping::KvMapping;
use crate::adapter::loader::load_mapping_dir;

/// Load every `KvMapping` from `.yaml` / `.yml` files under `dir`, subfolders included,
/// keyed by event type. See `docs/mapping-reference.md` for the file rules.
pub fn load_keyvalue_mappings<P: AsRef<Path>>(
    dir: P,
) -> Result<HashMap<String, KvMapping>, String> {
    load_mapping_dir(dir.as_ref(), "key-value", |m: &KvMapping| m.event.as_str())
}
