---
status: completed
---

# Rebuild refuses unsupported tables up front

## Objective

Owner's step 2 (2026-11-24): `kulyrax rebuild` found a table it could not swap only
at the swap, after replaying the whole history. Refuse those tables in
`ShadowRebuild::start_rebuild`, before anything is created, naming the table and
the object.

## Acceptance criteria

- [x] Postgres refused, with table and object named: a dependent view
  (`pg_depend` / `pg_rewrite`), a foreign key into or out of the table
  (`pg_constraint`), a column owned by a sequence (serial or identity).
- [x] SQLite refused: a view or trigger referencing the table, a foreign key
  pointing at it.
- [x] Errors reach the caller as `PipelineError::Rebuild`; no `unwrap` /
  `expect` / `panic!` in core.
- [x] A test per refusal case on SQLite and on Postgres, each asserting no shadow
  table and no shadow guard lane was created.
- [x] Decision record and README updated.
- [x] Gates green, commit, push.

## Constraints and assumptions

- `decide()`, dispatch and the dependency graph unchanged. The check is a new
  `SqlTxn::rebuild_blockers` (default: none), called by
  `exec::start_shadow_rebuild` for every table before the first shadow is
  created.

## Decisions

- [2026-11-24-shadow-rebuild](../decisions/2026-11-24-shadow-rebuild.md), updated:
  the limits move from "trade-offs" to "refused at preflight".

## Verification

| Check | Command | Result |
|---|---|---|
| SQLite | `cargo test -p kulyrax-core --test sql_rebuild` | 8 passed, first run: refused with `users: view users_active`, `users: trigger users_audit`, `users: foreign key from orders`, each leaving no `%__kulyrax_rebuild%` object, no guard lane outside `users`, rows and lanes unchanged; a view over `users_archive` is not a match (whole-word check), and that rebuild succeeds; the four earlier tests still pass |
| Postgres 16 (Docker, port 55432) | `KULYRAX_PG_URL=postgres://postgres:kulyrax@localhost:55432/kulyrax_ci cargo test -p kulyrax-adapter-postgres --test pg_backend` | 20 passed, first run: view, foreign key into, foreign key out of, `serial`, identity, each refused naming `<table>: …`, with no shadow table (`to_regclass`) and no shadow guard lanes |
| The view case is a real stop | reasoning, SQLite | without the preflight SQLite's `DROP TABLE users` succeeds with a view over it, so the rebuild would have swapped and left the view naming a replaced table; the test's error assertion would fail |
| Gates | `cargo fmt --check`; `cargo build --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo test --workspace --no-fail-fast` | fmt, build, clippy clean; 119 binaries, 408 passed, 0 failed |

## Outcome

Completed.
