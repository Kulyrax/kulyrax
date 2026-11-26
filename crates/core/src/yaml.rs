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
///
/// One pointer wide (the details are boxed), so the `Result`s and the error
/// enums that carry it (`PipelineError::ConfigParse`, `LoadError::Parse`) stay
/// small.
#[derive(Debug)]
pub struct YamlError(Box<Details>);

#[derive(Debug)]
struct Details {
    /// The file the document came from, if it came from one.
    file: Option<String>,
    /// The field the error belongs to (`adapters[0].priority`), if any.
    field: Option<String>,
    /// Set when the error is an unknown key whose own line was found.
    unknown_key: Option<UnknownKey>,
    kind: Kind,
}

#[derive(Debug)]
enum Kind {
    Read(std::io::Error),
    // Boxed: the parser's error is large.
    Parse(Box<serde_saphyr::Error>),
}

impl YamlError {
    /// The file the document came from, if it came from one.
    pub fn file(&self) -> Option<&str> {
        self.0.file.as_deref()
    }

    /// The path of the field the error belongs to (`adapters[0].priority`), if
    /// the error belongs to one.
    pub fn field(&self) -> Option<&str> {
        self.0.field.as_deref()
    }

    /// The document the error is in, for errors raised without the file name.
    pub fn in_file(mut self, file: impl Into<String>) -> Self {
        self.0.file = Some(file.into());
        self
    }
}

impl fmt::Display for YamlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let d = &self.0;
        if let Some(file) = &d.file {
            write!(f, "{file}: ")?;
        }
        if let Some(field) = &d.field {
            write!(f, "field `{field}`: ")?;
        }
        if let Some(UnknownKey { line, message }) = &d.unknown_key {
            return write!(f, "line {line}: {message}");
        }
        match &d.kind {
            Kind::Read(e) => write!(f, "cannot read: {e}"),
            Kind::Parse(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for YamlError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.0.kind {
            Kind::Read(e) => Some(e),
            Kind::Parse(e) => Some(e.as_ref()),
        }
    }
}

/// Read a YAML document into `T`. Prefer [`from_file`] or [`from_str_in`],
/// which put the file name into the error.
pub fn from_str<T: DeserializeOwned>(input: &str) -> Result<T, YamlError> {
    parse(input, None)
}

/// Read a YAML document that came from `file` (used only in errors) into `T`.
pub fn from_str_in<T: DeserializeOwned>(input: &str, file: &str) -> Result<T, YamlError> {
    parse(input, Some(file))
}

/// Read the YAML file at `path` into `T`.
pub fn from_file<T: DeserializeOwned>(path: &Path) -> Result<T, YamlError> {
    let file = path.display().to_string();
    let input = std::fs::read_to_string(path).map_err(|e| {
        YamlError(Box::new(Details {
            file: Some(file.clone()),
            field: None,
            unknown_key: None,
            kind: Kind::Read(e),
        }))
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

fn parse<T: DeserializeOwned>(input: &str, file: Option<&str>) -> Result<T, YamlError> {
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
    .map_err(|e| {
        YamlError(Box::new(Details {
            file: file.map(str::to_string),
            field,
            unknown_key: unknown_key(input, &e),
            kind: Kind::Parse(Box::new(e)),
        }))
    })
}

/// A key the receiving type does not know, and the line it is on.
#[derive(Debug)]
struct UnknownKey {
    line: u64,
    /// The parser's own message: "unknown field `k`, expected …".
    message: String,
}

/// For an "unknown field `k`" error, the line of the key `k` itself.
///
/// The parser can place such an error only at the start of the enclosing
/// entry, or nowhere: a tagged enum (`type:` / `kind:`) or a hand-written
/// `Deserialize` buffers its input first, which drops each key's position. So
/// the key is looked up in the text as a mapping key (`k:`), at or after the
/// line the parser reported. `None` when the error is about something else or
/// the key is not found; the parser's own message is then shown as it is.
fn unknown_key(input: &str, e: &serde_saphyr::Error) -> Option<UnknownKey> {
    const MARK: &str = "unknown field `";
    let text = e.to_string();
    let at = text.find(MARK)?;
    let message = text[at..].lines().next()?.trim_end().to_string();
    let key = text[at + MARK.len()..].split('`').next()?;
    let from = e.location().map(|l| l.line()).unwrap_or(1).max(1);
    let line = input
        .lines()
        .zip(1u64..)
        .skip_while(|(_, n)| *n < from)
        .find(|(l, _)| is_key_line(l, key))
        .map(|(_, n)| n)?;
    Some(UnknownKey { line, message })
}

/// `line` holds `key` as a mapping key: `key:`, `- key:`, or quoted.
fn is_key_line(line: &str, key: &str) -> bool {
    let mut rest = line.trim_start();
    while let Some(r) = rest.strip_prefix('-') {
        rest = r.trim_start();
    }
    [key.to_string(), format!("\"{key}\""), format!("'{key}'")]
        .iter()
        .any(|k| {
            rest.strip_prefix(k.as_str())
                .is_some_and(|r| r.trim_start().starts_with(':'))
        })
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

    /// Internally tagged, so the parser buffers each entry and loses the
    /// position of the keys inside it.
    #[derive(Debug, Deserialize)]
    #[serde(tag = "type")]
    #[allow(dead_code)]
    enum Tagged {
        #[serde(rename = "a")]
        A(Entry),
    }

    #[derive(Debug, Deserialize)]
    #[allow(dead_code)]
    struct TaggedDoc {
        adapters: Vec<Tagged>,
    }

    #[test]
    fn an_unknown_key_in_a_tagged_entry_is_shown_at_its_own_line() {
        let yaml = "adapters:\n  - type: a\n    id: x\n    priority: 1\n  - type: a\n    id: y\n    priority: 2\n    prioirty: 3\n";
        let err = from_str_in::<TaggedDoc>(yaml, "kulyrax.yaml").unwrap_err();
        let text = err.to_string();
        // The second entry starts on line 5; its misspelt key is on line 8.
        assert!(text.contains("line 8: unknown field `prioirty`"), "{text}");
    }

    #[test]
    fn an_unknown_key_may_be_quoted_or_open_a_list_entry() {
        assert!(is_key_line("  - \"prioirty\": 3", "prioirty"));
        assert!(is_key_line("  - - 'prioirty' : 3", "prioirty"));
        assert!(!is_key_line("  id: prioirty", "prioirty"));
        assert!(!is_key_line("  prioirtyx: 3", "prioirty"));
    }

    #[test]
    fn an_unknown_key_not_written_as_a_line_keeps_the_parsers_message() {
        // A flow mapping: the key is not at the start of a line.
        let err = from_str_in::<TaggedDoc>(
            "adapters: [{ type: a, id: x, priority: 1, bad: 2 }]\n",
            "kulyrax.yaml",
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("unknown field `bad`"), "{text}");
        assert!(text.contains("line 1"), "{text}");
    }
}
