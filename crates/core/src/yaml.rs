//! The one place Kulyrax reads YAML: the config, every mapping kind, and
//! routing (decision `2026-11-25-yaml-parser`).
//!
//! The parser is serde-saphyr, pure Rust with no `unsafe`. It is configured to
//! read YAML the way Kulyrax 0.12 did (serde_yaml 0.9, YAML 1.2 core schema),
//! so no existing file changes meaning:
//! - only `true` and `false` are booleans; `yes`, `on` and the like are refused
//!   where a `bool` is expected, and read as strings elsewhere;
//! - a number with a leading zero (`0755`) is a string, not an octal integer;
//! - a merge key (`<<`) is not expanded: it is an ordinary key, so a struct
//!   that expected the merged fields refuses the document;
//! - a duplicate key is refused by a struct (a repeated field); in a map
//!   (routing, a mapping's `columns:`) the last value wins.
//!
//! Every error names the file, when there is one; the field path (`adapters[0].
//! priority`), when the error belongs to a field; and the line and column, with
//! the offending lines quoted.
//!
//! No other file names the parser crate (a CI check enforces it), and none of
//! its types are in Kulyrax's API, so its major versions are not Kulyrax's.

use std::fmt;
use std::path::Path;

use serde::de::DeserializeOwned;

/// Why a YAML document could not be read.
#[derive(Debug)]
pub struct YamlError {
    /// The file the document came from, if it came from one.
    file: Option<String>,
    /// The field the error belongs to (`adapters[0].priority`), if any.
    field: Option<String>,
    kind: Kind,
}

#[derive(Debug)]
enum Kind {
    Read(std::io::Error),
    // Boxed: the parser's error is large, and `YamlError` travels in `Result`s.
    Parse(Box<serde_saphyr::Error>),
}

impl YamlError {
    /// The file the document came from, if it came from one.
    pub fn file(&self) -> Option<&str> {
        self.file.as_deref()
    }

    /// The path of the field the error belongs to (`adapters[0].priority`), if
    /// the error belongs to one.
    pub fn field(&self) -> Option<&str> {
        self.field.as_deref()
    }

    /// The document the error is in, for errors raised without the file name.
    pub fn in_file(mut self, file: impl Into<String>) -> Self {
        self.file = Some(file.into());
        self
    }
}

impl fmt::Display for YamlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(file) = &self.file {
            write!(f, "{file}: ")?;
        }
        if let Some(field) = &self.field {
            write!(f, "field `{field}`: ")?;
        }
        match &self.kind {
            Kind::Read(e) => write!(f, "cannot read: {e}"),
            Kind::Parse(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for YamlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            Kind::Read(e) => Some(e),
            Kind::Parse(e) => Some(e.as_ref()),
        }
    }
}

/// What a YAML document can be read into: any `DeserializeOwned` type.
///
/// Only the `yaml-golden` test feature adds a bound (`Debug`, to compare the
/// two parsers' results); it goes when serde_yaml does.
#[doc(hidden)]
#[cfg(not(feature = "yaml-golden"))]
pub trait Target: DeserializeOwned {}
#[cfg(not(feature = "yaml-golden"))]
impl<T: DeserializeOwned> Target for T {}

#[doc(hidden)]
#[cfg(feature = "yaml-golden")]
pub trait Target: DeserializeOwned + fmt::Debug {}
#[cfg(feature = "yaml-golden")]
impl<T: DeserializeOwned + fmt::Debug> Target for T {}

/// Read a YAML document into `T`. Prefer [`from_file`] or [`from_str_in`],
/// which put the file name into the error.
pub fn from_str<T: Target>(input: &str) -> Result<T, YamlError> {
    parse(input, None)
}

/// Read a YAML document that came from `file` (used only in errors) into `T`.
pub fn from_str_in<T: Target>(input: &str, file: &str) -> Result<T, YamlError> {
    parse(input, Some(file))
}

/// Read the YAML file at `path` into `T`.
pub fn from_file<T: Target>(path: &Path) -> Result<T, YamlError> {
    let file = path.display().to_string();
    let input = std::fs::read_to_string(path).map_err(|e| YamlError {
        file: Some(file.clone()),
        field: None,
        kind: Kind::Read(e),
    })?;
    parse(&input, Some(&file))
}

/// How Kulyrax reads YAML: as serde_yaml 0.9 did, with errors that quote the
/// offending lines. Duplicate keys and `<<` reach the receiving type as
/// serde_yaml passed them: a struct refuses a repeated field, a map keeps the
/// last value, and `<<` is an ordinary key (owner's answer, 2026-11-25).
fn options() -> serde_saphyr::Options {
    serde_saphyr::options! {
        strict_booleans: true,
        merge_keys: serde_saphyr::MergeKeyPolicy::AsOrdinary,
        duplicate_keys: serde_saphyr::DuplicateKeyPolicy::PassThrough,
        leading_zero_numbers_as_strings: true,
        with_snippet: true,
    }
}

fn parse<T: Target>(input: &str, file: Option<&str>) -> Result<T, YamlError> {
    let result = parse_only::<T>(input, file);
    #[cfg(feature = "yaml-golden")]
    golden::compare(input, &result);
    result
}

fn parse_only<T: DeserializeOwned>(input: &str, file: Option<&str>) -> Result<T, YamlError> {
    let mut field = None;
    serde_saphyr::with_deserializer_from_str_with_options(input, options(), |de| {
        serde_path_to_error::deserialize(de).map_err(|e| {
            let path = e.path().to_string();
            if path != "." {
                field = Some(path);
            }
            e.into_inner()
        })
    })
    .map_err(|e| YamlError {
        file: file.map(str::to_string),
        field,
        kind: Kind::Parse(Box::new(e)),
    })
}

/// The golden test of the parser change (step 3): every read is made again
/// with serde_yaml 0.9, into the same type and into `serde_json::Value`, and
/// the two are compared. Each comparison is one JSON line in
/// `<KULYRAX_YAML_GOLDEN_LOG>.<process id>` (`same: true|false`, and for a
/// difference, the input and both results). Nothing is logged when the
/// variable is unset.
#[cfg(feature = "yaml-golden")]
mod golden {
    use super::*;
    use std::io::Write;

    pub(super) fn compare<T: Target>(input: &str, new: &Result<T, YamlError>) {
        let Ok(log) = std::env::var("KULYRAX_YAML_GOLDEN_LOG") else {
            return;
        };
        let mut differences = Vec::new();

        let old: Result<T, serde_yaml::Error> = serde_yaml::from_str(input);
        match (new, &old) {
            (Ok(n), Ok(o)) if debug_lines(n) != debug_lines(o) => {
                differences.push(format!("typed: new {n:#?}, old {o:#?}"));
            }
            (Err(e), Ok(o)) => differences.push(format!("typed: new refuses ({e}), old {o:#?}")),
            (Ok(n), Err(e)) => differences.push(format!("typed: new {n:#?}, old refuses ({e})")),
            _ => {}
        }

        let new_value = parse_only::<serde_json::Value>(input, None);
        let old_value: Result<serde_json::Value, serde_yaml::Error> = serde_yaml::from_str(input);
        match (&new_value, &old_value) {
            (Ok(n), Ok(o)) if n != o => differences.push(format!("value: new {n}, old {o}")),
            (Err(e), Ok(o)) => differences.push(format!("value: new refuses ({e}), old {o}")),
            (Ok(n), Err(e)) => differences.push(format!("value: new {n}, old refuses ({e})")),
            _ => {}
        }

        let entry = if differences.is_empty() {
            serde_json::json!({ "type": std::any::type_name::<T>(), "same": true })
        } else {
            serde_json::json!({
                "type": std::any::type_name::<T>(),
                "same": false,
                "input": input,
                "differences": differences,
            })
        };
        // One file per process, one writer at a time: test binaries run in
        // parallel threads, and interleaved appends would garble lines.
        static WRITE: std::sync::Mutex<()> = std::sync::Mutex::new(());
        let _guard = WRITE.lock().unwrap_or_else(|e| e.into_inner());
        let path = format!("{log}.{}", std::process::id());
        if let Ok(mut f) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
        {
            let _ = f.write_all(format!("{entry}\n").as_bytes());
        }
    }

    /// `{:#?}`, one line per field or entry, sorted: a `HashMap`'s order differs
    /// between two maps with the same contents.
    fn debug_lines<T: fmt::Debug>(value: &T) -> Vec<String> {
        let mut lines: Vec<String> = format!("{value:#?}")
            .lines()
            .map(|l| l.trim_end_matches(',').to_string())
            .collect();
        lines.sort();
        lines
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, Deserialize, PartialEq)]
    #[serde(deny_unknown_fields)]
    struct Entry {
        id: String,
        priority: u32,
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Doc {
        adapters: Vec<Entry>,
    }

    #[test]
    fn reads_a_document() {
        let d: Doc = from_str("adapters:\n  - id: a\n    priority: 1\n").unwrap();
        assert_eq!(
            d.adapters,
            [Entry {
                id: "a".into(),
                priority: 1
            }]
        );
    }

    #[test]
    fn a_wrong_type_names_the_file_the_line_and_the_field() {
        let err = from_str_in::<Doc>("adapters:\n  - id: a\n    priority: high\n", "kulyrax.yaml")
            .unwrap_err();
        let text = err.to_string();
        assert_eq!(err.file(), Some("kulyrax.yaml"));
        assert_eq!(err.field(), Some("adapters[0].priority"));
        assert!(
            text.starts_with("kulyrax.yaml: field `adapters[0].priority`: "),
            "{text}"
        );
        assert!(text.contains("line 3"), "{text}");
    }

    #[test]
    fn a_syntax_error_names_the_file_and_the_line() {
        // Inside `adapters`, so the error belongs to that field.
        let err = from_str_in::<Doc>("adapters: [\n", "kulyrax.yaml").unwrap_err();
        let text = err.to_string();
        assert_eq!(err.field(), Some("adapters"));
        assert!(text.starts_with("kulyrax.yaml: "), "{text}");
        assert!(text.contains("line "), "{text}");
    }

    #[test]
    fn an_error_outside_any_field_has_no_field() {
        let err = from_str_in::<Doc>("- a\n", "kulyrax.yaml").unwrap_err();
        assert_eq!(err.field(), None, "{err}");
        assert!(err.to_string().starts_with("kulyrax.yaml: "), "{err}");
    }

    #[test]
    fn a_missing_file_names_the_file() {
        let err = from_file::<Doc>(Path::new("no/such/kulyrax.yaml")).unwrap_err();
        assert!(
            err.to_string().contains("kulyrax.yaml: cannot read"),
            "{err}"
        );
    }
}
