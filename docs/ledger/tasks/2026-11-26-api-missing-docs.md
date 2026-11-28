---
status: completed
---

# `#![deny(missing_docs)]` on every published crate; the docs written (step 4, D8)

## Objective

Runbook D8: "`#![deny(missing_docs)]` on published crates; write the docs."
This is tranche 4 of Phase D, the last.

At the tranche 1 report the count was 567 hand-written items (core 536), plus 6
generated `pb` items. The estimate was about a day.

## Acceptance criteria

- [x] `#![deny(missing_docs)]` in every published crate:
  - `kulyrax-core`, `-client`, `-ingest`, `-backends`, `-cli`;
  - the six adapter crates;
  - `kulyrax-source-postgres`.

  The generated `pb` code is `#[doc(hidden)]` (D3), which `missing_docs` skips:
  it builds with and without `unstable-pb`.
- [x] Every public item documented: 609 items, by kind:
  - 349 struct fields, 105 variants, 68 methods;
  - 26 structs, 25 modules, 17 associated functions, 16 enums;
  - 3 type aliases, 3 crate docs, 1 trait.

  Each was written against its code. Where a doc claimed behaviour, it was
  checked against the source (examples below).
- [x] Core's crate doc says what the crate is, before the public-path rules.
- [x] `cargo doc --workspace --no-deps` with `RUSTDOCFLAGS=-D warnings` is
  clean. 4 links that predate D8 were fixed: three to private items, one to a
  module that does not exist.
- [x] Gates: fmt, clippy `-D warnings` (also with `unstable-pb`), workspace
  tests, examples, every Docker backend suite.
- [x] PR #36 merged with the 12 required checks green (all 18 pass; merge commit e99a19c).

## Constraints and assumptions

- No behaviour change: doc comments only, plus four one-line variants split
  so that their fields can carry docs.
- Inserted by a script (`insert_docs.pl`, scratchpad) from per-file specs of
  `path|line|text`: bottom-up per file, above the item's attributes, refusing
  an already-documented item. The text is mine and was reviewed per item.

## Verification

| Check | Command | Result |
|---|---|---|
| Count | `RUSTFLAGS="-W missing_docs" cargo build --workspace` | 611 warnings: core 535, client 25, outbox 14, ingest 12, elasticsearch 7, the other adapters 3–4 each, cli 1. Two were in `build.rs` and do not count (609 items) |
| Claims checked before writing | the source | `failed` in a batch summary counts adapter writes, not events (doc fixed before insert). `InvalidArgument` is sent only for an empty position, not a bad payload (fixed). `decide` returns `Insert` for a non-permanent tombstone older than the event (wording fixed). `RuntimeError` is never returned anywhere (documented as such; reported). |
| Inserter, first runs | `insert_docs.pl` | It refused, and wrote nothing for that file, when: a spec line held a real newline from a `sed` edit; a field sat inside a one-line variant (`DeadLettered { count }`), now split by hand. |
| Encoding | `iconv -f UTF-8 -t UTF-8` on every changed file | valid. Twice a `\x{a7}` in a perl one-liner wrote a bare Latin-1 byte (`main.rs`, the D7 record); both were found and fixed |
| Build with deny | `cargo build --workspace`; client and ingest with `unstable-pb` | clean |
| rustdoc | `RUSTDOCFLAGS=-D warnings cargo doc --workspace --no-deps` | 4 warnings first, which predate D8; then 0 |
| fmt, clippy | `cargo fmt --check`; `cargo clippy --workspace --all-targets -- -D warnings`, and again with `unstable-pb` | clean |
| Workspace tests | `cargo test --workspace` | 592 pass, 0 fail |
| Examples | the 6 example manifests; `examples/embedded` run | 0 errors each |
| Docker suites (Neo4j on 7688) | `docker-suites.sh` | postgres 35, mysql 30, redis 19, mongodb 26, elasticsearch 16, neo4j 25, source-postgres 16, ingest 17, cli 37: all pass, 0 fail |

## Risks and open questions

- **`replay::EventsFromPath` exposes its iterator state** (variant fields like
  `reader`, `index`): an internal that D2 missed. Documented as "iterator
  state, not an interface"; making it opaque is an API change, so it is
  reported, not done here.
- **`runtime::error::RuntimeError` is never returned.** Documented as kept for
  compatibility, and reported: removing it before 1.0 is the owner's call.

## Outcome

Completed: merged in #36 (`e99a19c`). The two findings above stay open for the
owner.
