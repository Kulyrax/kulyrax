//! The golden test of the YAML parser change (decision 2026-11-25-yaml-parser).
//!
//! 1. **Same reading.** Every YAML file in the repository, and every `yaml` code
//!    block in its Markdown, reads the same with serde_yaml 0.9 (Kulyrax 0.12's
//!    parser) and with `kulyrax_core::yaml`: into `serde_json::Value`, and into
//!    the struct the file is (the config, a mapping kind, routing). The YAML
//!    strings inside Rust tests are compared by running the whole suite with
//!    the `kulyrax-core/yaml-golden` feature. This part goes when serde_yaml
//!    goes.
//! 2. **The refusal corpus**: inputs serde_yaml 0.9 refused, which Kulyrax must
//!    keep refusing, whatever the parser. It is permanent.

use std::collections::HashMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};

use kulyrax_core::adapter::document::mapping::DocumentMapping;
use kulyrax_core::adapter::graph::mapping::GraphMapping;
use kulyrax_core::adapter::keyvalue::mapping::KvMapping;
use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::runtime::config::KulyraxConfig;
use serde::Deserialize;
use serde::de::DeserializeOwned;

// ---------------------------------------------------------------------------
// 1. Same reading as serde_yaml 0.9
// ---------------------------------------------------------------------------

fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every file under the repository with one of `extensions`, outside build
/// output, version control and the private folder.
fn files(extensions: &[&str]) -> Vec<PathBuf> {
    fn walk(dir: &Path, extensions: &[&str], out: &mut Vec<PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if path.is_dir() {
                if !matches!(
                    name.as_str(),
                    "target" | ".git" | ".private" | "node_modules"
                ) {
                    walk(&path, extensions, out);
                }
            } else if path
                .extension()
                .and_then(|e| e.to_str())
                .is_some_and(|e| extensions.contains(&e))
            {
                out.push(path);
            }
        }
    }
    let mut out = Vec::new();
    walk(&repo_root(), extensions, &mut out);
    out.sort();
    out
}

/// `{:#?}`, one line per field or entry, sorted: a `HashMap`'s order differs
/// between two maps with the same contents.
fn debug_lines<T: Debug>(value: &T) -> Vec<String> {
    let mut lines: Vec<String> = format!("{value:#?}")
        .lines()
        .map(|l| l.trim_end_matches(',').to_string())
        .collect();
    lines.sort();
    lines
}

/// Both parsers read `input` into `T`: the same value, or both refuse.
fn same_typed<T: DeserializeOwned + Debug>(input: &str) -> Result<(), String> {
    let new = kulyrax_core::yaml::from_str::<T>(input);
    let old = serde_yaml::from_str::<T>(input);
    match (&new, &old) {
        (Ok(n), Ok(o)) if debug_lines(n) == debug_lines(o) => Ok(()),
        (Ok(n), Ok(o)) => Err(format!("new {n:#?}\nold {o:#?}")),
        (Err(_), Err(_)) => Ok(()),
        (Err(e), Ok(_)) => Err(format!("only the new parser refuses: {e}")),
        (Ok(_), Err(e)) => Err(format!("only serde_yaml refuses: {e}")),
    }
}

/// The struct a document is, judged by where it lives or what it holds.
fn same_as_kind(
    path_hint: &str,
    value: Option<&serde_json::Value>,
    input: &str,
) -> Result<(), String> {
    let parent = Path::new(path_hint)
        .parent()
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .unwrap_or("");
    let in_mappings = Path::new(path_hint)
        .parent()
        .and_then(|p| p.parent())
        .and_then(|p| p.file_name())
        .and_then(|n| n.to_str())
        .is_some_and(|n| n.starts_with("mappings"));
    let has = |key: &str| value.is_some_and(|v| v.get(key).is_some());
    let file = Path::new(path_hint)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("");
    if in_mappings && parent == "sql" || has("table") && has("event") {
        same_typed::<SqlMapping>(input)
    } else if in_mappings && parent == "document" || has("collection") && has("event") {
        same_typed::<DocumentMapping>(input)
    } else if in_mappings && parent == "keyvalue" || has("namespace") && has("event") {
        same_typed::<KvMapping>(input)
    } else if in_mappings && parent == "graph" || has("kind") && has("event") {
        same_typed::<GraphMapping>(input)
    } else if file.starts_with("routing") {
        same_typed::<HashMap<String, Vec<String>>>(input)
    } else if has("adapters") {
        same_typed::<KulyraxConfig>(input)
    } else {
        Ok(())
    }
}

/// Both parsers read `input` into `serde_json::Value` alike, and into the
/// struct it is.
fn compare(name: &str, input: &str) -> Result<(), String> {
    let new = kulyrax_core::yaml::from_str::<serde_json::Value>(input);
    let old = serde_yaml::from_str::<serde_json::Value>(input);
    let value = match (&new, &old) {
        (Ok(n), Ok(o)) if n == o => Some(n),
        (Ok(n), Ok(o)) => return Err(format!("{name}: value: new {n}, old {o}")),
        (Err(_), Err(_)) => None,
        (Err(e), Ok(_)) => return Err(format!("{name}: value: only the new parser refuses: {e}")),
        (Ok(_), Err(e)) => return Err(format!("{name}: value: only serde_yaml refuses: {e}")),
    };
    same_as_kind(name, value, input).map_err(|e| format!("{name}: typed: {e}"))
}

#[test]
fn every_yaml_file_reads_as_serde_yaml_read_it() {
    let files = files(&["yaml", "yml"]);
    assert!(files.len() >= 40, "found only {} YAML files", files.len());
    let differences: Vec<String> = files
        .iter()
        .filter_map(|path| {
            let input = std::fs::read_to_string(path).ok()?;
            compare(&path.display().to_string(), &input).err()
        })
        .collect();
    assert!(
        differences.is_empty(),
        "{} of {} files read differently:\n{}",
        differences.len(),
        files.len(),
        differences.join("\n\n")
    );
}

/// The ` ```yaml ` / ` ```yml ` blocks of a Markdown file.
fn yaml_blocks(markdown: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current: Option<String> = None;
    for line in markdown.lines() {
        let fence = line.trim_start();
        match &mut current {
            None if fence.starts_with("```yaml") || fence.starts_with("```yml") => {
                current = Some(String::new());
            }
            Some(block) if fence.starts_with("```") => {
                blocks.push(std::mem::take(block));
                current = None;
            }
            Some(block) => {
                block.push_str(line);
                block.push('\n');
            }
            None => {}
        }
    }
    blocks
}

#[test]
fn every_yaml_block_in_the_docs_reads_as_serde_yaml_read_it() {
    let mut blocks = 0;
    let mut differences = Vec::new();
    for path in files(&["md"]) {
        let Ok(markdown) = std::fs::read_to_string(&path) else {
            continue;
        };
        for (i, block) in yaml_blocks(&markdown).iter().enumerate() {
            blocks += 1;
            let name = format!("{} block {}", path.display(), i + 1);
            if let Err(e) = compare(&name, block) {
                differences.push(e);
            }
        }
    }
    assert!(blocks >= 15, "found only {blocks} YAML blocks");
    assert!(
        differences.is_empty(),
        "{} of {blocks} blocks read differently:\n{}",
        differences.len(),
        differences.join("\n\n")
    );
}

// ---------------------------------------------------------------------------
// 2. The refusal corpus (permanent)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Flag {
    flag: bool,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Mode {
    mode: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Point {
    x: i64,
    y: i64,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Merged {
    item: Point,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct Once {
    a: i64,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
#[allow(dead_code)]
struct Strict {
    id: String,
    priority: u32,
}

struct Refusal {
    name: &'static str,
    input: &'static str,
    /// Whether `kulyrax_core::yaml` refuses it.
    refused: fn(&str) -> bool,
    /// Whether serde_yaml 0.9 refuses it (goes with serde_yaml).
    refused_by_serde_yaml: fn(&str) -> bool,
}

macro_rules! refusal {
    ($name:literal, $ty:ty, $input:expr) => {
        Refusal {
            name: $name,
            input: $input,
            refused: |s| kulyrax_core::yaml::from_str::<$ty>(s).is_err(),
            refused_by_serde_yaml: |s| serde_yaml::from_str::<$ty>(s).is_err(),
        }
    };
}

fn refusal_corpus() -> Vec<Refusal> {
    vec![
        refusal!("`yes` into a bool", Flag, "flag: yes\n"),
        refusal!("`on` into a bool", Flag, "flag: on\n"),
        refusal!("`no` into a bool", Flag, "flag: no\n"),
        refusal!("`0755` into an integer", Mode, "mode: 0755\n"),
        refusal!(
            "a merge key",
            Merged,
            "base: &b\n  x: 1\nitem:\n  <<: *b\n  y: 2\n"
        ),
        refusal!("a duplicate key in a struct", Once, "a: 1\na: 2\n"),
        refusal!("a wrong type", Strict, "id: a\npriority: high\n"),
        refusal!(
            "an unknown field under deny_unknown_fields",
            Strict,
            "id: a\npriority: 1\npriorty: 2\n"
        ),
        refusal!(
            "a wrong type in the config",
            KulyraxConfig,
            "version: 1\nrouting:\n  file: r.yaml\nadapters:\n  - type: sqlite\n    id: a\n    priority: high\n    config:\n      path: a.db\n"
        ),
        refusal!(
            "a wrong type in a mapping",
            SqlMapping,
            "event: E\ntable: t\nprimary_key: id\nversion: high\ncolumns:\n  id: payload.id\n"
        ),
    ]
}

#[test]
fn the_refusal_corpus_is_refused() {
    let accepted: Vec<&str> = refusal_corpus()
        .iter()
        .filter(|c| !(c.refused)(c.input))
        .map(|c| c.name)
        .collect();
    assert!(accepted.is_empty(), "accepted: {accepted:?}");
}

/// The corpus is what serde_yaml 0.9 refused: refusing it keeps 0.12's reading.
#[test]
fn serde_yaml_refused_the_refusal_corpus() {
    let accepted: Vec<&str> = refusal_corpus()
        .iter()
        .filter(|c| !(c.refused_by_serde_yaml)(c.input))
        .map(|c| c.name)
        .collect();
    assert!(accepted.is_empty(), "serde_yaml accepted: {accepted:?}");
}

/// Not a refusal: in a map (routing, a mapping's `columns:`), a duplicate key
/// keeps the last value and `<<` is an ordinary key, as in Kulyrax 0.12
/// (owner's answer, 2026-11-25). Permanent.
#[test]
fn a_map_keeps_the_last_duplicate_and_a_literal_merge_key() {
    let routing: HashMap<String, Vec<String>> =
        kulyrax_core::yaml::from_str("A: [x]\nA: [y]\n").unwrap();
    assert_eq!(routing["A"], ["y"]);
    let value: serde_json::Value =
        kulyrax_core::yaml::from_str("base: &b\n  x: 1\nitem:\n  <<: *b\n").unwrap();
    assert_eq!(value["item"]["<<"]["x"], 1);
}

/// The owner's test: a wrong-type error in a mapping file names the file, the
/// line and the field path.
#[test]
fn a_wrong_type_in_a_mapping_file_names_the_file_the_line_and_the_field() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("users.yaml");
    std::fs::write(
        &path,
        "event: UserCreated\ntable: users\nprimary_key: id\nversion: high\ncolumns:\n  id: payload.id\n",
    )
    .unwrap();
    let err = kulyrax_core::yaml::from_file::<SqlMapping>(&path).unwrap_err();
    let text = err.to_string();
    assert!(text.contains("users.yaml"), "{text}");
    assert!(text.contains("line 4"), "{text}");
    assert_eq!(err.field(), Some("version"), "{text}");
    assert!(text.contains("field `version`"), "{text}");
}
