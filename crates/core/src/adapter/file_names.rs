//! File names for values that come from events (an entity id, a key, a node
//! id), and the check for names that come from mappings (a namespace, a
//! collection, a label). Used by the file-backed stores and the DLQ.
//!
//! **Event values** go through [`stem`], which is injective and safe on every
//! file system Kulyrax supports:
//! - *Ordinary* characters (`a`–`z`, `0`–`9`, `_`, `-`, `@`, and `.` except as
//!   the first or last character) are kept, so most ids keep the file name
//!   they always had.
//! - Every other byte, uppercase letters included, becomes `^` and two
//!   lowercase hex digits (`K` → `^4b`). `^` is not ordinary, so an id that
//!   contains it is always escaped, and an escaped name can never equal an
//!   ordinary id's name. Names contain no uppercase letter, so two different
//!   ids never meet on a case-insensitive file system (NTFS, default macOS);
//!   and no byte above ASCII, so Unicode normalization (macOS) never merges
//!   two names either.
//! - A name whose part before the first `.` is a Windows device name (`con`,
//!   `nul`, `com1`, …) has its first character escaped.
//! - A name longer than [`MAX_STEM`] bytes is replaced by `^h` and the
//!   SHA-256 of the id in hex. `^h` cannot start an escape (`h` is not a hex
//!   digit). The store keeps the full id inside the guard file, so a hash
//!   collision is detected, not merged.
//!
//! **Mapping names** must pass [`check_config_name`]: a single, plain path
//! component. They are refused at start-up rather than encoded, so the
//! directories a store creates are the ones its mappings name.

use sha2::{Digest, Sha256};

/// Longest encoded name kept as is; leaves room under the 255-byte file-name
/// limit for a suffix (`.guard.json`) and the store's own temporary names.
pub(crate) const MAX_STEM: usize = 200;

/// Windows device names: reserved with any extension.
const DEVICE_NAMES: &[&str] = &[
    "con", "prn", "aux", "nul", "com1", "com2", "com3", "com4", "com5", "com6", "com7", "com8",
    "com9", "lpt1", "lpt2", "lpt3", "lpt4", "lpt5", "lpt6", "lpt7", "lpt8", "lpt9",
];

fn ordinary(c: u8) -> bool {
    matches!(c, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'-' | b'@' | b'.')
}

fn escape(out: &mut String, byte: u8) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push('^');
    out.push(HEX[(byte >> 4) as usize] as char);
    out.push(HEX[(byte & 0xf) as usize] as char);
}

/// The reversible encoding of `id` (see the module docs), before the length
/// limit.
pub(crate) fn encode(id: &str) -> String {
    let bytes = id.as_bytes();
    let mut out = String::with_capacity(bytes.len());
    let last = bytes.len().saturating_sub(1);
    let device = {
        let head = id.split('.').next().unwrap_or(id);
        DEVICE_NAMES.iter().any(|d| head.eq_ignore_ascii_case(d))
    };
    for (i, &b) in bytes.iter().enumerate() {
        let edge_dot = b == b'.' && (i == 0 || i == last);
        if !ordinary(b) || edge_dot || (i == 0 && device) {
            escape(&mut out, b);
        } else {
            out.push(b as char);
        }
    }
    out
}

/// The inverse of [`encode`]; `None` if `name` is not an output of it.
pub(crate) fn decode(name: &str) -> Option<String> {
    let bytes = name.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'^' {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            if hex.bytes().any(|c| c.is_ascii_uppercase()) {
                return None;
            }
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// The file name stem for `id`: [`encode`], or a hashed name past
/// [`MAX_STEM`] bytes.
pub(crate) fn stem(id: &str) -> String {
    let encoded = encode(id);
    if encoded.len() <= MAX_STEM {
        return encoded;
    }
    hashed(id)
}

pub(crate) fn hashed(id: &str) -> String {
    let digest = Sha256::digest(id.as_bytes());
    let mut out = String::from("^h");
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    out
}

/// Whether [`stem`] gave a hashed name: the store then keeps the full id in
/// the file.
pub(crate) fn is_hashed(stem: &str) -> bool {
    stem.starts_with("^h")
}

/// Whether `name` is a stem this scheme can produce: what a file written by
/// it is called. A file written under the old scheme from an id that
/// [`encode`] now escapes (an uppercase letter, a `/`) fails this.
pub(crate) fn is_stem(name: &str) -> bool {
    if is_hashed(name) {
        return name.len() == 66
            && name[2..]
                .bytes()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase());
    }
    decode(name).is_some_and(|id| stem(&id) == name)
}

/// A mapping's namespace, collection, label or edge type, as a directory
/// name: letters, digits, `_` and `-`, at most 64 bytes, not a Windows device
/// name. `Err` says why not.
pub(crate) fn check_config_name(what: &str, name: &str) -> Result<(), String> {
    let refuse = |why: &str| Err(format!("{what} {name:?} cannot name a directory: {why}"));
    if name.is_empty() {
        return refuse("it is empty");
    }
    if name.len() > 64 {
        return refuse("it is longer than 64 bytes");
    }
    if let Some(c) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-'))
    {
        return refuse(&format!(
            "it contains {c:?}; use letters, digits, `_` and `-`"
        ));
    }
    if DEVICE_NAMES.iter().any(|d| name.eq_ignore_ascii_case(d)) {
        return refuse("it is a Windows device name");
    }
    Ok(())
}

/// The field of a guard file that holds the full id when its name is hashed.
const ID_FIELD: &str = "__kulyrax_id";

/// `guard_json` as written under `stem`: with the full `id` inside when the
/// name is hashed, so a read can tell two ids that share a hash apart.
pub(crate) fn with_id(guard_json: String, stem: &str, id: &str) -> String {
    if !is_hashed(stem) {
        return guard_json;
    }
    match serde_json::from_str::<serde_json::Value>(&guard_json) {
        Ok(serde_json::Value::Object(mut fields)) => {
            fields.insert(ID_FIELD.into(), id.into());
            serde_json::Value::Object(fields).to_string()
        }
        _ => guard_json,
    }
}

/// `Err` when a guard file read under a hashed `stem` belongs to another id.
pub(crate) fn check_id(guard_json: &str, stem: &str, id: &str) -> Result<(), String> {
    if !is_hashed(stem) {
        return Ok(());
    }
    let stored = serde_json::from_str::<serde_json::Value>(guard_json)
        .ok()
        .and_then(|v| v.get(ID_FIELD).and_then(|s| s.as_str()).map(str::to_string));
    match stored {
        Some(stored) if stored == id => Ok(()),
        other => Err(format!(
            "guard file {stem} belongs to {:?}, not {id:?}: two ids share a hashed file name",
            other.unwrap_or_default()
        )),
    }
}

/// For an upgrade: every entry of `dir` that this scheme would not have
/// named, onto `refused`: a directory (an earlier version made one from an
/// id containing `/`), a file without `suffix`, or a file whose name before
/// `suffix` is not a [`stem`]. A missing `dir` has nothing to refuse.
pub(crate) fn refuse_foreign_names(
    dir: &std::path::Path,
    suffix: &str,
    refused: &mut Vec<String>,
) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // The store's own temporary files.
        if name.starts_with(".kulyrax-tmp") || name.ends_with(".tmp") {
            continue;
        }
        let ok = entry.file_type()?.is_file() && name.strip_suffix(suffix).is_some_and(is_stem);
        if !ok {
            refused.push(path.display().to_string());
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small deterministic generator: no extra test dependency.
    struct Rng(u64);
    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }
        fn id(&mut self) -> String {
            const PIECES: &[&str] = &[
                "a", "z", "K", "k", "0", "9", "_", "-", "@", ".", "..", "/", "\\", "^", "^4b", ":",
                "?", "#", " ", "\0", "\n", "é", "é", "ß", "😀", "con", "NUL", "com1", "%2e",
            ];
            let n = (self.next() % 8) as usize;
            (0..n)
                .map(|_| PIECES[(self.next() % PIECES.len() as u64) as usize])
                .collect()
        }
    }

    #[test]
    fn ordinary_ids_keep_their_names() {
        for id in ["u1", "user-42", "a_b", "x@y.com", "a..b", "1.0.3"] {
            assert_eq!(encode(id), id);
            assert!(is_stem(id), "{id}");
        }
    }

    #[test]
    fn escapes() {
        assert_eq!(encode("K"), "^4b");
        assert_eq!(encode("../x"), "^2e.^2fx");
        assert_eq!(encode("."), "^2e");
        assert_eq!(encode(".."), "^2e^2e");
        assert_eq!(encode("a."), "a^2e");
        assert_eq!(encode("con"), "^63on");
        assert_eq!(encode("Con.txt"), "^43on.txt");
        assert_eq!(encode("^"), "^5e");
        assert_eq!(encode("é"), "^c3^a9");
        assert_eq!(encode(""), "");
    }

    /// Property, over 200,000 generated ids: decoding inverts encoding; names
    /// use only `[a-z0-9_@.^-]`, never start or end with `.`, never stem a
    /// device name; an escaped name always contains `^` and an ordinary id
    /// never does; and no two different ids share a name, even compared
    /// without case.
    #[test]
    fn the_encoding_is_injective_and_case_safe() {
        let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
        let mut seen: std::collections::HashMap<String, String> = Default::default();
        for _ in 0..200_000 {
            let id = rng.id();
            let name = encode(&id);
            assert_eq!(decode(&name).as_deref(), Some(id.as_str()), "{id:?}");
            assert!(
                name.bytes().all(|c| ordinary(c) || c == b'^'),
                "{id:?} -> {name}"
            );
            assert!(!name.starts_with('.') && !name.ends_with('.'), "{name}");
            let head = name.split('.').next().unwrap_or("");
            assert!(!DEVICE_NAMES.contains(&head), "{id:?} -> {name}");
            // A name differs from its id exactly when something was escaped,
            // and every escape leaves a `^`; an id with a `^` is escaped.
            assert_eq!(name != id, name.contains('^'), "{id:?} -> {name}");
            if id.contains('^') {
                assert_ne!(name, id);
            }
            if let Some(other) = seen.insert(name.to_ascii_lowercase(), id.clone()) {
                assert_eq!(other, id, "two ids share the name {name}");
            }
        }
    }

    #[test]
    fn long_ids_get_hashed_names() {
        let long = "a".repeat(MAX_STEM + 1);
        let s = stem(&long);
        assert!(is_hashed(&s) && s.len() == 66 && is_stem(&s), "{s}");
        assert_eq!(stem(&"a".repeat(MAX_STEM)), "a".repeat(MAX_STEM));
        assert_ne!(stem(&format!("{long}b")), s);
        // An id escaping to more than MAX_STEM bytes is hashed too.
        assert!(is_hashed(&stem(&"K".repeat(70))));
    }

    #[test]
    fn old_names_from_escaped_ids_are_not_stems() {
        for name in ["K", "User1", "a.", ".x", "con", "x^", "^zz", "^4B"] {
            assert!(!is_stem(name), "{name}");
        }
    }

    #[test]
    fn config_names() {
        for ok in ["users", "Users", "user_events-2"] {
            assert!(check_config_name("namespace", ok).is_ok(), "{ok}");
        }
        for bad in [
            "", "../x", ".kulyrax", "a/b", "a:b", "a b", "CON", "lpt1", "a.b",
        ] {
            assert!(check_config_name("namespace", bad).is_err(), "{bad}");
        }
    }
}
