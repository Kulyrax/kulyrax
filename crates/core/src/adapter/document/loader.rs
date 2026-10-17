use std::collections::HashMap;
use std::path::Path;

use crate::adapter::document::mapping::DocumentMapping;
use crate::adapter::loader::load_mapping_dir;

/// Load every `DocumentMapping` from `.yaml` / `.yml` files under `dir`, subfolders included,
/// keyed by event type. See `docs/mapping-reference.md` for the file rules.
pub fn load_document_mappings<P: AsRef<Path>>(
    dir: P,
) -> Result<HashMap<String, DocumentMapping>, String> {
    load_mapping_dir(dir.as_ref(), "document", |m: &DocumentMapping| {
        m.event.as_str()
    })
}
