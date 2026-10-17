use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

use serde::de::DeserializeOwned;

/// Load every mapping file under `dir` into a table keyed by event type.
///
/// Mapping files end in `.yaml` or `.yml` (any case) and may sit in
/// subdirectories, so a project can group them however it likes. Entries whose
/// name starts with `.` are skipped (`.gitkeep`, editor and VCS folders).
/// Symlinked directories are not followed, which rules out cycles; symlinked
/// files are read.
///
/// Files are read in sorted path order, so a duplicate event is always reported
/// against the same pair of files.
pub(crate) fn load_mapping_dir<M: DeserializeOwned>(
    dir: &Path,
    kind: &str,
    event_of: impl Fn(&M) -> &str,
) -> Result<HashMap<String, M>, String> {
    let mut files = Vec::new();
    collect_mapping_files(dir, kind, &mut files)?;
    files.sort();

    let mut mappings = HashMap::new();
    let mut sources: HashMap<String, PathBuf> = HashMap::new();
    for path in files {
        let content =
            fs::read_to_string(&path).map_err(|e| format!("Failed to read {:?}: {}", path, e))?;
        let mapping: M = serde_yaml::from_str(&content)
            .map_err(|e| format!("Invalid YAML in {:?}: {}", path, e))?;

        let event = event_of(&mapping).to_string();
        if let Some(first) = sources.get(&event) {
            return Err(format!(
                "Duplicate {} mapping for event '{}' in {:?} and {:?}",
                kind, event, first, path
            ));
        }
        sources.insert(event.clone(), path);
        mappings.insert(event, mapping);
    }

    Ok(mappings)
}

fn collect_mapping_files(dir: &Path, kind: &str, files: &mut Vec<PathBuf>) -> Result<(), String> {
    let entries = fs::read_dir(dir)
        .map_err(|e| format!("Failed to read {} mappings dir {:?}: {}", kind, dir, e))?;

    for entry in entries {
        let entry = entry.map_err(|e| e.to_string())?;
        if entry.file_name().to_string_lossy().starts_with('.') {
            continue;
        }
        let path = entry.path();
        let file_type = entry.file_type().map_err(|e| e.to_string())?;
        if file_type.is_dir() {
            collect_mapping_files(&path, kind, files)?;
        } else if is_yaml(&path) {
            files.push(path);
        }
    }
    Ok(())
}

fn is_yaml(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .is_some_and(|ext| ext.eq_ignore_ascii_case("yaml") || ext.eq_ignore_ascii_case("yml"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct TestMapping {
        event: String,
        table: String,
    }

    fn write(root: &Path, relative: &str, event: &str) {
        let path = root.join(relative);
        fs::create_dir_all(path.parent().expect("test path has a parent")).expect("create dir");
        fs::write(&path, format!("event: {event}\ntable: {relative}\n")).expect("write mapping");
    }

    fn load(root: &Path) -> Result<HashMap<String, TestMapping>, String> {
        load_mapping_dir(root, "SQL", |m: &TestMapping| m.event.as_str())
    }

    #[test]
    fn reads_yaml_and_yml_in_any_case() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "a.yaml", "A");
        write(dir.path(), "b.yml", "B");
        write(dir.path(), "c.YAML", "C");

        let mappings = load(dir.path()).expect("loads");
        let mut events: Vec<_> = mappings.keys().cloned().collect();
        events.sort();
        assert_eq!(events, ["A", "B", "C"]);
    }

    #[test]
    fn reads_nested_subdirectories() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "orders/created.yaml", "OrderCreated");
        write(dir.path(), "billing/invoices/issued.yml", "InvoiceIssued");

        let mappings = load(dir.path()).expect("loads");
        assert_eq!(
            mappings["InvoiceIssued"].table,
            "billing/invoices/issued.yml"
        );
        assert_eq!(mappings.len(), 2);
    }

    #[test]
    fn skips_hidden_entries_and_other_extensions() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "kept.yaml", "Kept");
        write(dir.path(), ".hidden/ignored.yaml", "HiddenDir");
        write(dir.path(), ".ignored.yaml", "HiddenFile");
        fs::write(dir.path().join(".gitkeep"), "").expect("write .gitkeep");
        fs::write(dir.path().join("notes.md"), "not yaml").expect("write notes");

        let mappings = load(dir.path()).expect("loads");
        assert_eq!(mappings.keys().collect::<Vec<_>>(), ["Kept"]);
    }

    #[test]
    fn duplicate_event_names_both_files() {
        let dir = tempfile::tempdir().expect("tempdir");
        write(dir.path(), "orders/a.yaml", "OrderCreated");
        write(dir.path(), "z.yml", "OrderCreated");

        let err = load(dir.path()).expect_err("duplicate is rejected");
        assert!(
            err.contains("Duplicate SQL mapping for event 'OrderCreated'"),
            "{err}"
        );
        assert!(err.contains("a.yaml") && err.contains("z.yml"), "{err}");
    }

    #[test]
    fn missing_directory_names_the_kind_and_path() {
        let dir = tempfile::tempdir().expect("tempdir");
        let err = load(&dir.path().join("absent")).expect_err("missing dir is an error");
        assert!(err.starts_with("Failed to read SQL mappings dir"), "{err}");
        assert!(err.contains("absent"), "{err}");
    }

    #[test]
    fn invalid_yaml_names_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        fs::write(dir.path().join("broken.yaml"), "event: [unclosed").expect("write");
        let err = load(dir.path()).expect_err("invalid YAML is an error");
        assert!(
            err.starts_with("Invalid YAML in") && err.contains("broken.yaml"),
            "{err}"
        );
    }
}
