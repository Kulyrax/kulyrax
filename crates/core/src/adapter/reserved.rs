//! The names Kulyrax keeps for its own objects, per storage kind, in one
//! list. A mapping that targets one is refused when the mapping files are
//! loaded (exit 2), naming the file, the key and the name: projecting into a
//! guard table, a format marker or a tombstone would corrupt it, or be
//! corrupted by it. Backends that check names of their own at `prepare` use
//! the same list (Redis: [`KV_NAMESPACE_PREFIX`]).
//!
//! Decision `2026-11-28-format-versions` (the owner's reserved-names rule);
//! user docs: `docs/mapping-reference.md`, "Names".
//!
//! File stores need nothing here: their names may hold only letters, digits,
//! `_` and `-` (`file_names::check_config_name`), so no mapping can name
//! `.kulyrax/` or 0.12's directory (`legacy::LEGACY_DIR`).
//!
//! 0.12's names come from `legacy`, the one place they are spelled.

use crate::adapter::legacy::{LEGACY_GUARD_TABLE, LEGACY_LABEL_PREFIX};
use crate::adapter::sql::exec::{FORMAT_TABLE, GUARD_TABLE};
use crate::rebuild::SHADOW_SUFFIX;

/// The temporary name MySQL and MongoDB give the 0.12 tombstone while it is
/// swapped in (step 2's migration).
const TOMBSTONE_TMP: &str = "kulyrax_tombstone_tmp";

/// SQL tables Kulyrax owns: the guard table, the format marker, the 0.12
/// tombstone and its temporary name. Compared without case: Postgres folds
/// unquoted names and MySQL's default collation ignores case.
pub const SQL_TABLES: &[&str] = &[GUARD_TABLE, FORMAT_TABLE, LEGACY_GUARD_TABLE, TOMBSTONE_TMP];
/// SQL table-name endings Kulyrax owns: rebuild shadows. Compared without case.
pub const SQL_TABLE_SUFFIXES: &[&str] = &[SHADOW_SUFFIX];
/// Collections and indexes Kulyrax owns, for every document backend:
/// MongoDB's guard collection, format marker, 0.12 tombstone and its
/// temporary name, and Elasticsearch's per-cluster marker index
/// (`kulyrax_format`). A document mapping does not know which backend it
/// feeds, so the list is one. Compared without case.
pub const DOCUMENT_COLLECTIONS: &[&str] =
    &[GUARD_TABLE, FORMAT_TABLE, LEGACY_GUARD_TABLE, TOMBSTONE_TMP];
/// Top-level document fields Kulyrax owns: Elasticsearch keeps each
/// document's guard in `__kulyrax_guard`, and would drop a mapped field of
/// that name without a word.
pub const DOCUMENT_FIELD_PREFIX: &str = "__kulyrax";
/// Graph labels Kulyrax owns: Neo4j's guard (`__KulyraxGuard`) and format
/// marker (`__KulyraxFormat`) labels, and 0.12's guard label
/// (`legacy::LEGACY_LABEL_PREFIX`). Labels are case-sensitive, so the
/// prefixes are too.
pub const GRAPH_LABEL_PREFIXES: &[&str] = &["__Kulyrax", LEGACY_LABEL_PREFIX];
/// Key-value namespaces Kulyrax owns: Redis keeps guards under
/// `__kulyrax:guard:` and its format marker at `__kulyrax:format`, so a
/// namespace starting with this could reach them.
pub const KV_NAMESPACE_PREFIX: &str = "__kulyrax";

/// Why `table` is reserved in a mapping file, if it is: one of
/// [`SQL_TABLES`] or a name ending in one of [`SQL_TABLE_SUFFIXES`]. A
/// schema-qualified `schema.table` is judged by its table: Kulyrax's own
/// tables may be in any schema.
pub fn sql_table(table: &str) -> Option<String> {
    if let Some(why) = sql_own_table(table) {
        return Some(why);
    }
    let lower = table_name(table).to_ascii_lowercase();
    SQL_TABLE_SUFFIXES
        .iter()
        .find(|s| lower.ends_with(*s))
        .map(|s| format!("tables ending in {s} are Kulyrax's rebuild shadows"))
}

/// Why `table` is reserved in any mapping, the ones Kulyrax builds itself
/// included, if it is: one of [`SQL_TABLES`]. The SQL backends check this at
/// `prepare`. Not the shadow suffix: a rebuild projects through mappings
/// that Kulyrax points at the shadow tables, and the rebuild's own preflight
/// refuses a mapped table that is named like one.
pub fn sql_own_table(table: &str) -> Option<String> {
    let lower = table_name(table).to_ascii_lowercase();
    SQL_TABLES
        .iter()
        .find(|t| **t == lower)
        .map(|own| format!("{own} is a table Kulyrax keeps its own data in"))
}

fn table_name(table: &str) -> &str {
    table.rsplit('.').next().unwrap_or(table)
}

/// Why `collection` is reserved, if it is.
pub fn document_collection(collection: &str) -> Option<String> {
    let lower = collection.to_ascii_lowercase();
    DOCUMENT_COLLECTIONS
        .iter()
        .find(|c| **c == lower)
        .map(|own| format!("{own} is a collection or index Kulyrax keeps its own data in"))
}

/// Why the top-level document field `field` is reserved, if it is.
pub fn document_field(field: &str) -> Option<String> {
    field
        .starts_with(DOCUMENT_FIELD_PREFIX)
        .then(|| format!("fields starting with {DOCUMENT_FIELD_PREFIX} are Kulyrax's"))
}

/// Why the graph label `label` is reserved, if it is.
pub fn graph_label(label: &str) -> Option<String> {
    GRAPH_LABEL_PREFIXES
        .iter()
        .find(|p| label.starts_with(*p))
        .map(|p| format!("labels starting with {p} are Kulyrax's"))
}

/// Why the key-value namespace `namespace` is reserved, if it is.
pub fn kv_namespace(namespace: &str) -> Option<String> {
    namespace
        .starts_with(KV_NAMESPACE_PREFIX)
        .then(|| format!("namespaces starting with {KV_NAMESPACE_PREFIX} are Kulyrax's"))
}

/// A reserved name one mapping uses: the mapping key, the name, and why.
pub type Collision = (&'static str, String, String);

/// Every reserved name a document mapping uses: its collection, and its
/// top-level document fields.
pub fn in_document_mapping(
    m: &crate::adapter::document::mapping::DocumentMapping,
) -> Vec<Collision> {
    let mut found: Vec<Collision> = document_collection(&m.collection)
        .map(|why| ("collection", m.collection.clone(), why))
        .into_iter()
        .collect();
    if let Some(fields) = m.document.as_object() {
        found.extend(
            fields
                .keys()
                .filter_map(|f| document_field(f).map(|why| ("document field", f.clone(), why))),
        );
    }
    found
}

/// Every reserved label a graph mapping uses: its nodes' labels and its
/// edges' endpoint labels (an edge creates a placeholder with that label).
pub fn in_graph_mapping(m: &crate::adapter::graph::mapping::GraphMapping) -> Vec<Collision> {
    use crate::adapter::graph::mapping::GraphMapping;
    let mut found = Vec::new();
    for r in m.records() {
        let labels: Vec<(&'static str, &String)> = match r {
            GraphMapping::Node(n) => vec![("label", &n.label)],
            GraphMapping::Edge(e) => vec![("from_label", &e.from_label), ("to_label", &e.to_label)],
            _ => Vec::new(),
        };
        for (key, label) in labels {
            if let Some(why) = graph_label(label) {
                found.push((key, label.clone(), why));
            }
        }
    }
    found
}

/// `prepare`'s refusal for mappings built in code (the loader refuses mapping
/// files first): `None` when no mapping uses a reserved name.
pub fn refusal(adapter: &str, found: Vec<(String, Collision)>) -> Option<String> {
    if found.is_empty() {
        return None;
    }
    let mut lines: Vec<String> = found
        .into_iter()
        .map(|(event, (key, name, why))| format!("{event}: {key} {name:?} ({why})"))
        .collect();
    lines.sort();
    lines.dedup();
    Some(format!(
        "{adapter}: mappings name what Kulyrax keeps for itself: {}",
        lines.join(", ")
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `name` with its first letter in upper case.
    fn capitalised(name: &str) -> String {
        let mut chars = name.chars();
        chars.next().map_or(String::new(), |c| {
            c.to_ascii_uppercase().to_string() + chars.as_str()
        })
    }

    #[test]
    fn sql_tables_without_case() {
        let legacy_mixed = capitalised(LEGACY_GUARD_TABLE);
        for t in [
            "kulyrax_projection_state",
            "KULYRAX_FORMAT",
            legacy_mixed.as_str(),
            "kulyrax_tombstone_tmp",
            "users__kulyrax_rebuild",
            "Users__KULYRAX_REBUILD",
            "app.kulyrax_format",
        ] {
            assert!(sql_table(t).is_some(), "{t}");
        }
        for t in [
            "users",
            "kulyrax_formats",
            "my_kulyrax_format",
            "users__kulyrax",
        ] {
            assert_eq!(sql_table(t), None, "{t}");
        }
    }

    #[test]
    fn document_collections_and_fields() {
        let legacy_upper = LEGACY_GUARD_TABLE.to_ascii_uppercase();
        for c in [
            "kulyrax_projection_state",
            "kulyrax_format",
            legacy_upper.as_str(),
        ] {
            assert!(document_collection(c).is_some(), "{c}");
        }
        assert_eq!(document_collection("users"), None);
        assert!(document_field("__kulyrax_guard").is_some());
        assert_eq!(document_field("kulyrax_guard"), None);
    }

    #[test]
    fn graph_labels_by_prefix_with_case() {
        for l in [
            "__KulyraxGuard",
            "__KulyraxFormat",
            crate::adapter::legacy::LEGACY_GUARD_LABEL,
            "__KulyraxAnything",
        ] {
            assert!(graph_label(l).is_some(), "{l}");
        }
        for l in ["User", "__kulyraxguard", "KulyraxGuard"] {
            assert_eq!(graph_label(l), None, "{l}");
        }
    }

    #[test]
    fn kv_namespaces_cover_the_marker() {
        // `__kulyrax` + `:` + `format` would be the marker's key.
        assert!(kv_namespace("__kulyrax").is_some());
        assert!(kv_namespace("__kulyraxguard").is_some());
        assert_eq!(kv_namespace("users"), None);
    }
}
