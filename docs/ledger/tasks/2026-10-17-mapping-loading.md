---
status: completed
---

# Load mappings from `.yml` files and subfolders

## Objective

Mapping files ending in `.yml` were ignored, and so was everything inside a subfolder of `mappings/<kind>/`. A user who named a file `orders.yml`, or grouped files into `mappings/sql/orders/`, got a startup error saying the mapping was missing even though the file was there. `.yml` and `.yaml` are the same format, and folder grouping is how larger projects stay organised. The owner asked for both to load.

Out of scope: how the storage kind is chosen (still the top-level folder), the mapping file format, and validation.

## Acceptance criteria

- [x] `.yaml` and `.yml` files load, in any letter case.
- [x] Files in nested subfolders of `mappings/<kind>/` load.
- [x] Hidden entries (names starting with `.`, such as `.gitkeep`) and files with other extensions are skipped.
- [x] Two files for the same event in one kind are still rejected, and the error names both files.
- [x] The four public `load_*_mappings` functions keep their signatures.
- [x] `docs/mapping-reference.md` describes the rules.
- [x] fmt, clippy `-D warnings` and `cargo test --workspace` pass.

## Constraints and assumptions

- Constraint: Zero Panic Rule in library code.
- Constraint: `kulyrax_core::adapter::<kind>::loader::load_*_mappings` are public, so they stay as wrappers.
- Verified: the only non-`.yaml` files inside any mapping folder in the repo are three `.gitkeep` files, and no mapping folder has subfolders. So recursive loading picks up nothing new in existing examples or fixtures.

## Decisions

- This is the trigger recorded in [the adapter restructure](2026-10-16-adapter-crate-restructure.md) for building the generic loader: loader behaviour had to change, and it would otherwise have meant the same edit in four files. The four loaders now call `adapter::loader::load_mapping_dir`.
- Read each mapping's event name through a closure argument, not a trait. Graph's mapping is an enum with an `event()` method, and the others have an `event` field. A closure covers both with no new public type.
- Symlinked directories are not followed, which rules out cycles. Symlinked files are still read, as before.
- Files are read in sorted path order, so a duplicate is always reported against the same pair of files.

## Verification

| Criterion | Check | Observed result |
|---|---|---|
| Before: `.yml` behaviour | quickstart copy with `user_email_changed.yaml` renamed to `.yml`, `kulyrax run --once` | exit 1 at startup: `Validation([MissingSqlMapping { event_type: "UserEmailChanged", … }])` |
| Before: control | same copy with all `.yaml`, schema created with `dbtool init` | 3 processed, 3 succeeded; matches `expected/users.txt` |
| Before: loader test coverage | grep for loader error strings outside `loader.rs` | no tests asserted on them |
| Loader rules | `cargo test -p kulyrax-core --lib adapter::loader` | 6 passed: yaml/yml/YAML; nested folders; hidden and other files skipped; duplicate names both files; missing folder names kind and path; invalid YAML names the file |
| End to end | quickstart copy with the mapping moved to `mappings/sql/users/user_email_changed.yml`, `kulyrax run --once` | exit 0; 3 processed, 3 succeeded; matches `expected/users.txt` |
| Gates | `cargo fmt --check`; clippy `-D warnings`; `cargo test --workspace --no-fail-fast` | clean; clean; 103 test binaries, 296 passed, 0 failed |
| Rustdoc | `RUSTDOCFLAGS="-D warnings" cargo doc -p kulyrax-core --no-deps`, with and without this change | fails both ways on the same 3 older warnings, in `adapter/mod.rs`, `runtime/config.rs` and `keyvalue/exec.rs`; none in the loader files. Not a CI gate |

## Risks and open questions

- A project that kept non-mapping YAML files (drafts, notes) in a subfolder of a mapping folder will now have them loaded. None exist in this repo; this is a behaviour change worth a line in the next release note.
- A file like `orders.yaml.bak` is still skipped, since its extension is `bak`.

## Outcome

Mappings now load from `.yaml` and `.yml` files anywhere under `mappings/<kind>/`. One generic loader replaces four copies; the public functions are unchanged.
