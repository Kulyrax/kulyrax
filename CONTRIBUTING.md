# Contributing to Kulyrax

Thanks for looking. This covers day-to-day contribution — dev setup, the standards a
PR is held to, and how the DB-backed tests work. **Adding a new database backend has
its own dedicated guide: [`docs/writing-a-backend.md`](docs/writing-a-backend.md)** —
read that first if that's what brought you here.

## Getting set up

```sh
git clone https://github.com/Panchauly/kulyrax.git
cd kulyrax
cargo build --workspace
cargo test --workspace
```

That's the whole setup — no external services required for the default test run (see
[DB-backed tests](#db-backed-tests) below for the ones that need one).

## Before opening a PR

Four gates, all enforced in CI (`.github/workflows/ci.yml`) and expected to be clean
locally first:

```sh
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --check
```

And two standards specific to this codebase (see `CLAUDE.md` for the full list):

- **Zero-panic rule.** No `unwrap()` / `expect()` / `panic!()` / `unreachable!()` in
  `kulyrax-core` library code. Return an explicit error type — every existing adapter
  and the `runtime::error` module are the pattern to follow.
- **Every engine feature is backed by a test** under `crates/kulyrax-core/tests/`
  (fixtures in `tests/fixtures/`, shared helpers in `tests/common/`). A behavior
  change with no test to show it is not landing.

## DB-backed tests

Postgres, MySQL, Redis, MongoDB, and Neo4j each have their own integration suite
(`tests/pg_backend.rs`, `tests/mysql_backend.rs`, `tests/redis_backend.rs`,
`tests/mongo_backend.rs`, `tests/neo4j_backend.rs`). They're **skipped, not failed**,
when the matching connection env var isn't set — `cargo test --workspace` locally with
no databases running will report them as passed-and-skipped, not error:

| Suite | Env var |
|---|---|
| Postgres | `KULYRAX_POSTGRES_URL` |
| MySQL | `KULYRAX_MYSQL_URL` |
| Redis | `KULYRAX_REDIS_URL` |
| MongoDB | `KULYRAX_MONGO_URL` (+ optional `KULYRAX_MONGO_DB`) |
| Neo4j | `KULYRAX_NEO4J_URL`, `KULYRAX_NEO4J_USER`, `KULYRAX_NEO4J_PASSWORD` |

Point the relevant env var at a real instance (a local Docker container is the easiest
way — see `.github/workflows/ci.yml`'s `test-postgres` / `test-mysql` / `test-redis` /
`test-mongo` / `test-neo4j` jobs for exactly how CI starts each one, including Mongo's
replica-set initialization) to run that suite for real before touching backend code.
CI runs all five on every PR regardless of what you tested locally.

## Code style

- **Thin CLI pattern.** `kulyrax-cli` is argument parsing (`clap`) and output rendering
  only — every engine call goes through `kulyrax_core::pipeline`. Business logic never
  lives in `main.rs`.
- Match the surrounding code's idiom and comment density rather than introducing a new
  style in one file. Module-level and function-level doc comments explain *why* a piece
  is shaped the way it is — read those before changing it, and `architecture.md` for
  the system-wide contracts (the `decide()` gate, the guard model, adapter/source
  traits) that any change needs to preserve.
- A behavior change should come with a short explanation in the PR description of what
  changed and why, alongside the test that proves it.

## Reporting issues

Include: what you ran, what you expected, what happened, and your `kulyrax --version` /
Rust toolchain version. For a backend-specific bug, the failing mapping YAML and the
adapter's error output are usually enough to reproduce from.

## License

By contributing, you agree your contribution is licensed under the same dual
MIT / Apache-2.0 terms as the rest of the project (see [`LICENSE-MIT`](LICENSE-MIT) /
[`LICENSE-APACHE`](LICENSE-APACHE)).
