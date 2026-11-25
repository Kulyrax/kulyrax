---
status: completed
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
- [x] In CI: each suite green 3 times (re-runs) once the branch is pushed, after
  PR #12 is merged.
- [x] A `cargo audit` CI job over the workspace and every example lockfile,
  failing on vulnerabilities and on unsound, yanked or unmaintained crates.
  Unmaintained crates with no fix are ignored in `.cargo/audit.toml`, each with
  a reason, what pulls it in, and a revisit date. It goes green once the deps
  and rusqlite PRs are in (this branch merges after them).
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
| Audit job, locally | the job's loop (`cargo audit --file <lock> --deny unsound --deny unmaintained --deny yanked` from the root, so `.cargo/audit.toml` applies) on the rusqlite branch with its updated lockfiles | the ignores apply to every lockfile, example ones included (checked: `embedded`'s unsafe-libyaml is ignored from the root, denied when run from the example's own directory). The remaining failures were exactly what the deps PR fixes |
| CI, 3 runs | PR #14 at `48c13bb` (rebased onto main after #12, #13, #15): run 45564731759, attempts 1, 2, 3 | all 13 jobs green on every attempt: test (ubuntu, macos, windows), postgres, postgres outbox source, mysql, redis, mongodb, neo4j, elasticsearch, examples, MSRV (1.89), cargo audit |


## Outcome

Pushed after PR #12 merged; CI green 3 times in a row; merged as PR #14.
