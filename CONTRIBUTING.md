# Contributing to Kulyrax

Thanks for looking. This covers day-to-day contribution — dev setup, the standards a
PR is held to, and how the DB-backed tests work. **Adding a new database backend has
its own dedicated guide: [`docs/writing-a-backend.md`](docs/writing-a-backend.md)** —
read that first if that's what brought you here.

## Getting set up

```sh
git clone https://github.com/Kulyrax/kulyrax.git
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
  and `source::SourceError` are the pattern to follow.
- **Every engine feature is backed by a test** under `crates/core/tests/`
  (fixtures in `tests/fixtures/`, shared helpers in `tests/common/`). A behavior
  change with no test to show it is not landing.

## DB-backed tests

Postgres, MySQL, Redis, MongoDB, Neo4j and Elasticsearch each have their own
integration suite (`tests/pg_backend.rs`, `tests/mysql_backend.rs`,
`tests/redis_backend.rs`, `tests/mongo_backend.rs`, `tests/neo4j_backend.rs`,
`tests/es_backend.rs`), and some CLI, ingest and outbox tests use them too. They're
**skipped, not failed**, when the matching connection env var isn't set —
`cargo test --workspace` locally with no databases running will report them as
passed-and-skipped, not error:

| Suite | Env var |
|---|---|
| Postgres | `KULYRAX_PG_URL` |
| MySQL | `KULYRAX_MYSQL_URL` |
| Redis | `KULYRAX_REDIS_URL` |
| MongoDB | `KULYRAX_MONGO_URL` |
| Neo4j | `KULYRAX_NEO4J_URI`, `KULYRAX_NEO4J_USER`, `KULYRAX_NEO4J_PASSWORD` |
| Elasticsearch | `KULYRAX_ELASTICSEARCH_URL` |

A test that skips prints `SKIP: <var> not set`; run with `-- --nocapture` to see
it. CI runs every suite on every PR regardless of what you tested locally.

### Containers

The same images CI uses (`.github/workflows/ci.yml`), each on its usual port:

| Backend | Start it | Env var value |
|---|---|---|
| Postgres | `docker run -d --name kx-pg -p 5432:5432 -e POSTGRES_PASSWORD=kulyrax -e POSTGRES_DB=kulyrax_ci postgres:16` | `postgres://postgres:kulyrax@localhost:5432/kulyrax_ci` |
| MySQL | `docker run -d --name kx-mysql -p 3306:3306 -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -e MYSQL_DATABASE=kulyrax_ci mysql:8` | `mysql://root@localhost:3306/kulyrax_ci` |
| Redis | `docker run -d --name kx-redis -p 6379:6379 redis:7` | `redis://localhost:6379` |
| MongoDB | `docker run -d --name kx-mongo -p 27017:27017 mongo:7 --replSet rs0`, then once `docker exec kx-mongo mongosh --eval 'rs.initiate()'` (the adapter uses transactions, which need a replica set) | `mongodb://localhost:27017/?directConnection=true` |
| Neo4j | `docker run -d --name kx-neo4j -p 7687:7687 -e NEO4J_AUTH=neo4j/kulyrax-test neo4j:5` | URI `bolt://localhost:7687`, user `neo4j`, password `kulyrax-test` |
| Elasticsearch | `docker run -d --name kx-es -p 9200:9200 -e discovery.type=single-node -e xpack.security.enabled=false -e ES_JAVA_OPTS="-Xms512m -Xmx512m" docker.elastic.co/elasticsearch/elasticsearch:8.15.0` | `http://localhost:9200` |

If a port is taken, map another (`-p 55432:5432`) and put that port in the URL.

### Running them

- **One backend:**
  `KULYRAX_PG_URL=… cargo test -p kulyrax-adapter-postgres -- --test-threads=1`.
- **The Elasticsearch and Neo4j suites need `--test-threads=1`:** on one node,
  parallel index creation queues behind cluster-state writes, and a Neo4j test
  owns the whole database (below).
- **The upgrade tests** (`cargo test -p kulyrax-cli --test rename_migration`)
  open stores a real Kulyrax 0.12.1 wrote. Build that once and point
  `KULYRAX_V0121_BIN` at it:
  ```sh
  git worktree add ../v0.12.1 v0.12.1
  cargo build --manifest-path ../v0.12.1/Cargo.toml -p kulyrax-cli \
    --features kulyrax-core/crash-points --target-dir ../v0.12.1-target
  ```
  Without it they print `SKIP`. Set every backend's env var too, as the
  `upgrade from 0.12.1` CI job does.
- **Crash tests** (`file_store_crash`, `dlq_crash`, the upgrade kill tests) kill
  the binary at named points. `kulyrax-cli`'s dev-dependencies turn on
  `kulyrax-core/crash-points` for them; a normal build has no crash points.

### Isolation

A test never reads or writes another run's data, or the shared database's own
guard table, collection or label:

- **Postgres, MySQL, MongoDB:** each test binary works in a schema (Postgres) or
  database (MySQL, MongoDB) of its own, named `kx_run_<binary>`. It is dropped and
  recreated when the binary starts, and the binary fails at once if it is not empty
  then. Tests that need more than one (the `*_rename` suites, the CLI upgrade tests)
  make one per test and drop it at the end.
- **Redis, Elasticsearch:** each test uses a namespace or index of its own, named
  after the process id and the run's start time: a process id alone repeats, since
  Windows reuses them. The CLI upgrade tests delete theirs at the end.
- **Format markers that are one per database or cluster.**
  - **Redis:** the marker is one key per database. `redis_format` and the
    CLI's `redis_eviction_notice` use databases 13 and 14, which no other
    suite uses, and flush them before and after.
  - **Elasticsearch:** the marker is one per cluster. `es_format` deletes it
    around each test and, in one test, sets it to a newer version, so **do not
    run it at the same time as another suite that starts an Elasticsearch
    adapter on the same cluster** (`kulyrax-ingest`, `kulyrax-cli`): their
    `prepare` would be refused meanwhile. One `cargo test` runs a crate's
    binaries one after another, and CI gives each job its own cluster.
- **Neo4j** has a single database, and the guard-label migration relabels every guard
  node in it, so a Neo4j test owns the whole database while it runs. It takes a
  process-wide lock and an OS file lock in `target/tmp`, then wipes everything: every
  node, and every constraint and index named `kulyrax_…` (or with 0.12's prefix), or
  on a guard label. It wipes again when it ends. A guard node left after the first wipe fails
  the test.

  **Locally, run one Neo4j container per branch**, and never point two branches at
  one Neo4j: a run on one branch wipes the other's data. Pick the port with
  `KULYRAX_NEO4J_URI`, for example `bolt://localhost:7687` for `main` and
  `bolt://localhost:7688` for a second branch:

  ```
  docker run -d --name kx-neo4j-7688 -p 7688:7687 -e NEO4J_AUTH=neo4j/kulyrax-test neo4j:5
  ```

  CI starts a fresh Neo4j for every job.

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

## Cutting a release

1. Set `version` under `[workspace.package]` in the root `Cargo.toml`.
2. Write the notes in `RELEASES/v<version>.md`.
3. Once that is merged into `main`, tag the merge commit and push the tag:

   ```sh
   git tag v<version>
   git push upstream v<version>
   ```

The `Release` workflow (`.github/workflows/release.yml`) refuses a tag that doesn't
match the workspace version. It builds `kulyrax` for Linux x86_64, macOS (Apple Silicon
and Intel) and Windows x86_64, and publishes a GitHub release with those archives,
their SHA-256 files, and the notes file.

## Reporting issues

Include: what you ran, what you expected, what happened, and your `kulyrax --version` /
Rust toolchain version. For a backend-specific bug, the failing mapping YAML and the
adapter's error output are usually enough to reproduce from.

## License

By contributing, you agree your contribution is licensed under the same dual
MIT / Apache-2.0 terms as the rest of the project (see [`LICENSE-MIT`](LICENSE-MIT) /
[`LICENSE-APACHE`](LICENSE-APACHE)).
