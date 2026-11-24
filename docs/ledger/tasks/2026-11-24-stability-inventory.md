---
status: in-progress
---

# 1.0 inventory: everything a 1.0 release would freeze

## Objective

Owner's 1.0 plan, step 1 (2026-11-24): a draft `docs/stability.md` listing every
compatibility promise 1.0.0 would make (public Rust API, YAML, CLI, proto,
persisted formats, every "conduit"), each marked freeze / change before 1.0 /
unstable. Documentation only, then stop for the owner's review.

## Acceptance criteria

- [x] Public Rust API of every library crate listed, one item per line
  (`docs/stability/api/*.txt`), summarized per module with a proposal.
- [x] Config, mapping and routing YAML; CLI commands, flags, exit codes and
  parseable output; `ingest.proto`; persisted formats.
- [x] Every "conduit" occurrence, grouped (persisted name / code identifier /
  user-visible / docs / legal).
- [x] A proposal per item, and the open questions for the owner.
- [ ] Owner's review (STOP).

## Verification

| Check | Command | Result |
|---|---|---|
| Tools | `cargo public-api --version`, `rustup toolchain list` | not installed at first; installed `nightly` (minimal profile) and `cargo-public-api` 0.52.0 (owner named the tool) |
| Public API | `cargo +nightly public-api -p <crate> --simplified` for the 11 library crates | all exit 0: core 5,449 lines; ingest 208; client 198; source-postgres 70; adapter-postgres 71; mysql 64; mongodb 42; neo4j 43; elasticsearch 42; redis 40; backends 2. No local paths in the output |
| "conduit" count | `grep -rio conduit crates` | 193 (the plan said 188): `conduit_projection_state` 121, `.conduit` 47 + `./.conduit` 2, `__ConduitGuard` 16, `CONDUIT_DIR` 4, `conduit` 3; source 80, tests 113 |
| Unknown keys | `grep deny_unknown_fields` in `runtime/config.rs`, `adapter/*/mapping.rs` | none: built-in configs and mappings ignore unknown keys |
| Error types | `grep '^impl core::error::Error for'` over the API lists | all but `DependencyOrderError` and `AdapterReportError` |
| **Finding F1** (key-value namespace collides with the file store's own names) | a throwaway test in `crates/core/tests` (deleted, not committed): write at sequence 5, restart, redeliver sequence 3 | namespace `users` → `Skipped(StaleSequence)`; namespace **`wal`** → **`Created`** (guards ignored after restart: `store_is_empty` skips `wal`); namespace **`lock`** → `Failed(WriteFailed("… (os error 183)"))` |
| Config `version` | `KulyraxConfig::validate` | already required, must be 1 |
| Gates (documentation only) | `cargo fmt --check`; `cargo build --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace --no-fail-fast` | all clean; 122 binaries, 420 passed, 0 failed |

## Risks and open questions

- F1 is a correctness bug in released 0.11 code for a key-value namespace named
  `wal` (and `lock`). Proposed fix in step 2.
- The six questions in `docs/stability.md` §9 need the owner's answers.

## Outcome

In progress: waiting for the owner's review.
