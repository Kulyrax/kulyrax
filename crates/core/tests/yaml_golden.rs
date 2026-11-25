//! The YAML parser's permanent regression test (decision 2026-11-25-yaml-parser).
//!
//! Before serde_yaml was removed, this file compared it with
//! `kulyrax_core::yaml` on every YAML file in the repository and every `yaml`
//! block in its docs, and a test-only feature compared the two on every read in
//! the test suite: no difference (the ledger has the numbers). What stays:
//!
//! - **the refusal corpus**: inputs serde_yaml 0.9 (Kulyrax 0.12's parser)
//!   refused, which Kulyrax must keep refusing, whatever the parser;
//! - how a map reads a duplicate key and `<<`, as 0.12 did;
//! - that a wrong-type error names the file, the line and the field.

use std::collections::HashMap;

use kulyrax_core::adapter::sql::mapping::SqlMapping;
use kulyrax_core::runtime::config::KulyraxConfig;
use serde::Deserialize;

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
}

macro_rules! refusal {
    ($name:literal, $ty:ty, $input:expr) => {
        Refusal {
            name: $name,
            input: $input,
            refused: |s| kulyrax_core::yaml::from_str::<$ty>(s).is_err(),
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
