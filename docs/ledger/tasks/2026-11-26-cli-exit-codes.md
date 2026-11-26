---
status: in-progress
---

# Exit codes 0/1/2/3/4, each tested through the CLI (step 4, D7)

## Objective

Runbook D7: "Exit codes: 0 ok, 1 internal, 2 usage/config/mapping/routing
invalid, 3 event failures (failed or dead-lettered in --once / replay /
rebuild), 4 refused by a safety check (lock held, rebuild refused, newer on-disk
format). A CLI test each." This is the last item of tranche 3.

It turned out bigger than planned, about a day instead of half: codes 3 and 4
needed typed signals. The owner approved the mapping on 2026-11-26: decision
[2026-11-26-exit-codes](../decisions/2026-11-26-exit-codes.md).

## Acceptance criteria

- [x] New typed signals:
  - `SourceError::Locked` and `SourceError::InvalidConfig`;
  - `rebuild::RebuildError` (`Invalid` / `Refused` / `EventsFailed` /
    `Failed`), held by `PipelineError::Rebuild`;
  - `ShadowRebuild` returns `RebuildError`;
  - `SqlError::RebuildRefused` for the exec layer's two refusals, in SQLite and
    Postgres.
- [x] The CLI maps every error and outcome to its code (`mod exit`, `main.rs`):
  - `run --event`, `run --once`, continuous `run`, `ingest`;
  - `replay`, `rebuild`, `dry-run`, `reload`, `init`.
- [x] `crates/cli/tests/exit_codes.rs` runs the binary for each code:
  - **0:** a clean `--once`.
  - **1:** `--event` naming a directory (an I/O error).
  - **2:**
    - clap;
    - a misspelt config key;
    - an unknown source type;
    - a rebuild of an unknown adapter.
  - **3:**
    - `--once` with no DLQ, and with one;
    - `run --event`;
    - `replay`;
    - `dry-run`;
    - `rebuild`.
  - **4:**
    - a held outbox lock (taken before connecting, so no database is needed);
    - a rebuild of a shadow-named table;
    - `init` over a starter file.
- [x] `RebuildError` is in the `Send + Sync + 'static` test, and a backend's
  refusal stays `Refused`.
- [x] Docs:
  - the `stability.md` §3 exit-code table;
  - the rc.1 notes, with an exit-codes section and script before/after;
  - the README reload line.
- [x] Gates: fmt, clippy `-D warnings`, workspace tests, examples, every Docker
  backend suite.
- [ ] PR merged with the 12 required checks green.

## Constraints and assumptions

- The thin-CLI rule holds: the mapping is a match over public error types
  (rendering), and every engine call still goes through `pipeline`.
- `decide()`, dispatch and the dependency graph are untouched.
- Step 5's newer-format refusal does not exist yet. It maps to 4 when it lands
  (decision, "Revisit if").

## Verification

| Check | Command | Result |
|---|---|---|
| CLI exit codes, first run | `cargo test -p kulyrax-cli --test exit_codes` | 2 pass, 3 fail. Causes: (a) the fixture never created the SQLite table, so even the good event failed (exit 3 where 0 was expected), and the rebuild found no table; fixed by creating it with rusqlite. (b) The lock case held `state/in.lock`, but directory sources take no lock; only the outbox source does (`lock_source`). Switched to an outbox source, which locks before connecting |
| CLI exit codes, after | same | 5 pass |
| clippy | `cargo clippy --workspace --all-targets -- -D warnings` | clean |
| fmt | `cargo fmt --check` | clean |
| Workspace tests | `cargo test --workspace` | 592 pass, 0 fail |
| Examples | the 6 example manifests; `examples/embedded` run | 0 errors each; `projected: users.u1.name = "Ada"` |
| Docker suites (Neo4j on 7688) | `docker-suites.sh` | postgres 35, mysql 30, redis 19, mongodb 26, elasticsearch 16, neo4j 25, source-postgres 16, ingest 17, cli 37: all pass, 0 fail |

## Risks and open questions

- **A held file-store lock exits 1, not 4.** It surfaces as an adapter prepare
  failure. Reported to the owner (decision, "Filled in here").
- **Scripts that tested `$? -eq 1` for "some event failed"** must test 3. This
  is in the rc.1 notes.

## Outcome

In progress.
