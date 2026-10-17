---
status: completed
---

# `kulyrax init`: a runnable starter project

## Objective

A newcomer had no way to start a project except copying an example, and easily missed that `mappings/sql/` and `mappings/document/` must both exist. The owner asked for an `init` command. Its output should run straight away with no setup.

## Acceptance criteria

- [x] `kulyrax init [dir]` writes config, routing, one mapping and sample events, creating `dir` if needed (default `.`).
- [x] `kulyrax run --once` from inside the new project succeeds with no further setup.
- [x] It never overwrites: if any target file exists, it lists them and writes nothing.
- [x] Every generated file is YAML except the events, and the files explain themselves in comments.
- [x] README, CLI README and CLAUDE.md mention it.
- [x] fmt, clippy `-D warnings` and `cargo test --workspace` pass.

## Constraints and assumptions

- Constraint: Thin CLI. The scaffold lives in `kulyrax_core::scaffold` and is reached through `kulyrax_core::pipeline::init_project`; the CLI only prints.
- Constraint: Kulyrax never creates a user's SQL tables, so a SQL starter could not run without a manual step.

## Decisions

- The starter projects into the file-backed document adapter, which needs no setup, so `init` then `run` works with nothing installed. The config carries a commented SQLite adapter with the steps to enable it.
- It is all-or-nothing: existence is checked for every file before any is written, so a refused `init` leaves the folder untouched.
- The starter's `.gitignore` covers `out/`, `.kulyrax/` and `app.db`, so a first run leaves `git status` clean.

## Verification

| Criterion | Check | Observed result |
|---|---|---|
| Runs with no setup | scratch folder: `kulyrax init myproject`, `cd myproject`, `kulyrax run --config config.yaml --mappings mappings --once` | init exit 0; run exit 0, 2 processed, 2 created; `out/users/u1.json` = `{"name": "Ada", "email": "ada@example.com"}` |
| Re-run is idempotent | second `run --once` | 0 processed (checkpoint remembers the position) |
| No overwrite | `kulyrax init myproject` again | exit 1: `error: not overwriting existing files, nothing was written:` plus the 6 paths |
| Tests | `cargo test -p kulyrax-core --test scaffold` | 3 passed: the starter loads, validates, and both sample events dry-run to `users` writes for `u1`, `u2`; an existing `config.yaml` blocks init, stays unchanged, and nothing else is written; init works in an existing empty folder |
| Gates | `cargo fmt --check`; clippy `-D warnings`; `cargo test --workspace --no-fail-fast` | clean; clean; 105 test binaries, 309 passed, 0 failed |

## Risks and open questions

- Adapter paths (`file.root`, `sqlite.path`) resolve against the folder `kulyrax` runs from, while sources and routing resolve against the config file's folder. The starter says to run from the project folder and states the rule in a comment. Making all paths config-relative is a behaviour change worth its own task.
- The file adapter writes its bookkeeping under `out/.conduit/`, a pre-rebrand name on disk (see the open storage-name follow-up).

## Outcome

`kulyrax init my-project` creates a commented starter that runs immediately and refuses to overwrite anything.
