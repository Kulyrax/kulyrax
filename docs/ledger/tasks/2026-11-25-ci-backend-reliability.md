---
status: in-progress
---

# Backend CI suites pass on a clean run, every run

## Objective

Release follow-up (b), before 0.12.1 and the rename land: CI fully green.

- **MySQL.** Today the suite reuses one long-lived database. Table names
  restart at `t_0` every run, so a guard row from an earlier run makes
  `mysql_column_missing_rejected` see `Skipped(StaleSequence)`. That fails on
  `cda1862` too.
- **Elasticsearch.** In parallel, index creation on one node waits on
  cluster-state writes (measured at 31–36 s under load) until the 10 s client
  timeout fails tests at random.

## Acceptance criteria

- [x] Each MySQL test binary drops and recreates a database of its own, once per
  run (`tests/common/mod.rs`), so no run sees an earlier run's rows.
- [x] The Elasticsearch suite's adapter timeout is 60 s, and CI runs the suite
  with `--test-threads=1`.
- [x] Locally: each suite passes 3 runs in a row against long-lived containers
  (the case that failed).
- [ ] In CI: each suite green 3 times (re-runs) once the branch is pushed, after
  PR #12 is merged.
- [x] Four gates green.

## Constraints and assumptions

- Branch `ci/backend-reliability` from `origin/phase-3`. Not pushed until PR #12
  is merged (owner).
- Test code only, plus the CI workflow. No library change.
- Two concurrent runs of one MySQL test binary against one server would share
  its database. CI runs each binary once.

## Verification

| Check | Command | Result |
|---|---|---|
| Before the change | `cargo test -p kulyrax-adapter-mysql --test mysql_backend` at `cda1862` against a long-lived database | `mysql_column_missing_rejected` fails with `Skipped(StaleSequence)` (recorded in `2026-11-24-file-store-names`, and again on 2026-11-25) |
| MySQL, 3 runs | `cargo test -p kulyrax-adapter-mysql` ×3 against the long-lived `kx-mysql` container | runs 1–3: `mysql_backend` 12/12 (with `mysql_column_missing_rejected`), `mysql_binlog` 1/1, `mysql_guard_collation` 1/1, `mysql_guard_ddl` 1/1. Green on the first run |
| Elasticsearch, 3 runs | `cargo test -p kulyrax-adapter-elasticsearch --test es_backend -- --test-threads=1` ×3 | runs 1–3: 8/8 each (run 3 took 229 s while another build ran on the same machine) |
| Format | `cargo fmt --check` | first run: rustfmt reflowed the new MySQL test code (`tests/common/mod.rs`, two `let … else` lines). Ran `cargo fmt`; then clean |
| Lint | `cargo clippy --workspace --all-targets -- -D warnings` | clean |
| Build | `cargo build --workspace` | clean |
| Tests | `cargo test --workspace --no-fail-fast` | 420 passed, 0 failed (backend suites skip without their URLs; they ran above) |

## Risks and open questions

- The 3× CI proof needs the branch on GitHub, after PR #12.

## Outcome

Committed on `ci/backend-reliability`, not pushed. Pushed after PR #12 merges,
then each backend job is re-run 3 times in CI.
