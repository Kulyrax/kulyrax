//! `.kulyrax/`: the one directory name Kulyrax keeps its own files under, and
//! every entry any part of Kulyrax puts there.
//!
//! Two owners can meet in one `.kulyrax/`. A file store keeps its internal
//! files in `<store root>/.kulyrax/`. The config folder holds
//! `<config folder>/.kulyrax/`, for `kulyrax reload`'s control files and, by
//! the scaffolded default, the source checkpoints. A store whose root *is* the
//! config folder shares the directory with them. That is safe because each
//! owner uses only the entries listed here, and the lists are disjoint
//! (checked by a test):
//!
//! | Owner | Entries |
//! |---|---|
//! | file stores ([`crate::adapter::file_store`]) | [`STORE_ENTRIES`] |
//! | `kulyrax reload` ([`crate::runtime::reload`]) | [`CONTROL`] |
//! | scaffolded checkpoints (`state_dir: ./.kulyrax/state`) | [`STATE`] |
//!
//! No check may ever ask whether `.kulyrax/` as a whole is empty: another owner's
//! entries may be there. Each owner looks at its own entries only.

/// The directory's name.
pub const DIR: &str = ".kulyrax";

/// A file store's batch logs.
pub const WAL: &str = "wal";
/// A file store's process lock.
pub const LOCK: &str = "lock";
/// A file store's layout markers, one per adapter kind.
pub const LAYOUT: &str = "layout";
/// The key-value store's guards.
pub const KV: &str = "kv";
/// The document store's guards.
pub const ENTITIES: &str = "entities";
/// The graph store's guards.
pub const GUARD: &str = "guard";
/// The graph store's incidence index.
pub const INCIDENT: &str = "incident";

/// Every entry a file store may create under `.kulyrax/`.
pub const STORE_ENTRIES: &[&str] = &[WAL, LOCK, LAYOUT, KV, ENTITIES, GUARD, INCIDENT];

/// `kulyrax reload`'s request and result files. Reserved: no store creates it.
pub const CONTROL: &str = "control";
/// The scaffolded `state_dir` for source checkpoints. Reserved: no store
/// creates it.
pub const STATE: &str = "state";

/// Entries owned by something other than a file store.
pub const RESERVED_ENTRIES: &[&str] = &[CONTROL, STATE];

/// Whether `path`, relative to a store root with `/` separators, is under
/// `.kulyrax/` in an entry a file store owns.
pub fn is_store_path(path: &str) -> bool {
    path.strip_prefix(DIR)
        .and_then(|rest| rest.strip_prefix('/'))
        .and_then(|rest| rest.split('/').next())
        .is_some_and(|entry| STORE_ENTRIES.contains(&entry))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owners_never_share_an_entry() {
        for e in RESERVED_ENTRIES {
            assert!(
                !STORE_ENTRIES.contains(e),
                "{e} is both reserved and a store entry"
            );
        }
        let mut all: Vec<&str> = STORE_ENTRIES
            .iter()
            .chain(RESERVED_ENTRIES)
            .copied()
            .collect();
        all.sort_unstable();
        all.dedup();
        assert_eq!(all.len(), STORE_ENTRIES.len() + RESERVED_ENTRIES.len());
    }

    #[test]
    fn store_paths() {
        assert!(is_store_path(".kulyrax/kv/users/u1.guard.json"));
        assert!(is_store_path(".kulyrax/wal/1.log"));
        assert!(!is_store_path(".kulyrax/state/events.json"));
        assert!(!is_store_path(".kulyrax/control/reload-request"));
        assert!(!is_store_path(".kulyrax"));
        assert!(!is_store_path(".kulyraxx/kv/a"));
        assert!(!is_store_path("users/u1.json"));
    }
}
