---
status: completed
---

# File stores name files by an injective, case-safe encoding (0.12.1)

## Objective

Fix GHSA-6chh-gx4c-448g before step 2 of the 1.0 plan, as its own release
(0.12.1; affected <= 0.12.0). The file-backed key-value, document and graph stores
built file names straight from event data, so an id could leave the store root,
merge with another id on a case-insensitive file system, or be impossible to write.
Mapping names reached the store's own files (`.conduit/wal`, `.conduit/lock`), and
dead-letter file names took producer-chosen positions as given. Owner's
requirements (2026-11-24):

- internal directories live where no namespace name can reach them; validation
  also refuses names that collide with reserved names;
- the encoding is injective, case-preserving and collision-free on
  case-insensitive file systems; the escape character is outside the ordinary
  set; property test;
- ids whose names would exceed 255 bytes get a hash-based name with the full id
  inside the file; tested;
- an existing store holding files the old scheme named from unsafe ids is
  refused at start-up, naming the paths and telling the operator to rebuild;
  never silently re-keyed;
- the real findings of the networked-backend audit are fixed too;
- regression test per case; `RELEASES/v0.12.1.md`; gates green; pushed only to the
  private advisory fork (`ghsa`), never to `origin` or upstream.

## Acceptance criteria

- [x] No event id writes outside its store root (KV key, document id, graph node
  id, edge endpoints), and each is idempotent across a restart.
- [x] `K` and `k` are two entities with two lanes across a restart, on NTFS.
- [x] Names longer than `MAX_STEM` (200) are `^h<sha256>`; no file name over 255
  bytes; a guard whose stored id differs fails the event.
- [x] Property test: decode∘encode = id, charset, no edge dots, no device stem,
  `name != id` iff the name contains `^`, injective after lowercasing.
- [x] KV namespaces `wal`, `lock`, `layout`, `kv`, … work across a restart (guards
  now under `.conduit/kv/`).
- [x] A 0.12.0 KV store (including namespace `wal`) opens with every lane; an
  older redelivery is stale.
- [x] A 0.12.0 store with `K.json`, a nested `a/b.json`, or a guard at both the old
  and new path is refused, with the paths named and "Rebuild the store"; nothing
  is moved.
- [x] Mapping names `../x`, `.conduit`, `a/b`, `con`, `a:b`, `a.b` refused before
  anything is written; `Users`+`users` and KV namespace = document collection on
  one root refused.
- [x] DLQ file for position `../../outside` stays inside `dlq_dir`.
- [x] SQL table/column names must be plain identifiers (SQLite, Postgres, MySQL);
  ES ids `.`/`..` fail the event; Redis namespace with `:`/`__kulyrax`/empty
  refused; Neo4j label with a backtick refused.
- [x] `decide()`, `dispatch.rs`, the dependency graph unchanged; no `unwrap`/
  `expect`/`panic!` added to core library code.
- [x] Version 0.12.1; `RELEASES/v0.12.1.md` with an upgrade section.
- [x] Four gates green; every Docker backend suite run.
- [x] Pushed to `ghsa` only.

## Constraints and assumptions

- Disclosure: nothing about this is pushed to `origin` or upstream (no commits,
  test names, ledger entries or messages that hint at it) until the owner
  publishes the advisory, which merges the fork.
- Branch `security/file-names` from `cda1862` (phase-3), per the owner.
- Uppercase ids are encoded (`K` → `^4b`), so stores holding them must be rebuilt;
  accepted by the owner.

## Decisions

- **Ordinary set** `[a-z0-9_@.-]`, `.` not first or last; everything else
  `^` + two lowercase hex digits. Lowercase-only ordinary characters and lowercase
  hex make the encoding injective when the file system ignores case; `^` is
  escaped itself, so no encoded name equals an ordinary id.
- **Windows device names** (`con`, `prn`, `aux`, `nul`, `com1`–`9`, `lpt1`–`9`)
  get their first character escaped, with or without an extension.
- **Hashed names** `^h` + 64 hex (66 bytes) above 200 bytes, leaving room for the
  longest suffix (`.guard.json`) and temp-file prefixes. The full id is stored as
  `__kulyrax_id` in the guard file and checked on every read.
- **Layout check** once per store, recorded in `.conduit/layout/<kind>`: claim the
  top-level directory names on the root (case-folded), run the kind's upgrade (KV:
  move guards to `.conduit/kv/<ns>/`), then refuse any name this version would not
  have written. Runs after the batch-log replay, so a crashed 0.12.0 batch is
  completed under its own names first, then checked.
- **Config names** for the file stores: 1–64 bytes of `[A-Za-z0-9_-]`, not a
  device name. Graph node files keep the label as given (`nodes/User/`); claims
  compare case-folded.
- **Networked backends**: each check in its own crate's `prepare` (SQL in
  `exec::check_identifiers`, shared by the three SQL backends), so there is no
  storage-kind special case in the engine.

## Verification

| Check | Command | Result |
|---|---|---|
| Encoding unit + property tests | `cargo test -p kulyrax-core --lib file_names` | 6 passed, first run (200,000 random ids) |
| Core suite after the layout change, first run | `cargo test -p kulyrax-core` | 4 failed: `kv_redelivery`, `kv_delete_lifecycle`, `kv_cross_adapter` read guards at `.conduit/<ns>/`; `file_store_batch` tree comparison saw `.conduit/layout`. Expected consequences; tests updated to `.conduit/kv/<ns>/` and to ignore the marker. Then 295 passed |
| Regression suite, first run | `cargo test -p kulyrax-core --test file_store_names` | 7 passed, 4 failed. **Code**: (1) the old-and-new-guard refusal printed `.conduit/kv\users\…` (mixed separators) under a header that only described renamed ids. Fixed: path built from components; entry reads "both X and Y"; header covers both cases. (2) `Root::claim` compared only against earlier adapters' claims, so `Users`+`users` in one adapter passed. Fixed: staged map checked name by name. **Test**: (3) `longer` contained `long`, so the tampering step edited the wrong guard; (4) a column name in YAML double quotes was invalid YAML. Then 11 passed |
| Backends, first run | `cargo test -p kulyrax-adapter-{elasticsearch,redis,neo4j,postgres,mysql,mongodb}` against kx-* containers | neo4j 13, postgres 21, mongodb 11 passed. redis `kv_cross_backend` read the guard at the old path (expected; updated). ES: new test read through a helper that only fetches fixed ids, and fetched the id unencoded (test bugs; fixed). ES also failed intermittently with `error sending request` and index-create 503 |
| ES intermittent failures | `_cat/indices`: 198 leftover `t<pid>-<n>` test indices on the local node; deleted. Suite run 3× parallel, 3× `--test-threads=1` | after cleanup, parallel 2 of 3 runs green (failures are request timeouts on plain ids like `u1`); serial 3 of 3 green, 9 passed. Base `cda1862` parallel 2 of 2 green before cleanup. The code change is a check made before any request is sent. Recorded as a load flake of the single local node under 10 s client timeouts, not a regression |
| MySQL `mysql_column_missing_rejected` | run on this branch and on `cda1862` (worktree) | fails on both with `Skipped(StaleSequence)`: `fresh_table()` restarts at `t_0` each run and the long-lived database keeps the guard row. Already failing on `cda1862`; not caused by this change; the new MySQL test passes |
| Workspace tests, first run | `cargo test --workspace` | CLI `file_store_crash::unsynced_files_lost_with_the_page_cache_are_restored` failed: the test truncates every file, including `.conduit/layout/*`, then compares contents. The marker is store metadata, fsynced when written, read only for existence; the test now leaves it out with the lock and batch logs |
| Workspace tests | `cargo test --workspace --no-fail-fast` | 442 passed, 0 failed (backend suites skip without their env vars; run above) |
| Clippy | `cargo clippy --workspace --all-targets -- -D warnings` | clean |
| Format | `cargo fmt --check` | clean |
| Version | `cargo run -p kulyrax-cli -- --version` | `kulyrax 0.12.1` |
| Examples | `cargo update -p kulyrax-core` / `-p kulyrax-client` in the two example lockfiles; `cargo run --manifest-path examples/embedded/Cargo.toml`; build `examples/grpc-producer/producer` | both build; embedded prints `status: Succeeded`, `projected: users.u1.name = "Ada"`. Embedded lockfile gains `sha2` and its dependencies; the workspace lockfile already had `sha2 0.10.9` |
| Engine untouched | `git diff --stat cda1862 -- crates/core/src/dispatch.rs crates/core/src/runtime/dependency_graph.rs`; `decide()` in `adapter/mod.rs` | no changes; `adapter/mod.rs` only gains `mod file_names` |
| Rebase onto main (2026-11-25) | `git rebase upstream/main` twice: onto `8b41a38` (after #12, #13, #15), then `cc86796` (after #14) | first rebase: one conflict, `examples/embedded/Cargo.lock` (this change's 0.12.1 and `sha2` vs main's rusqlite 0.43). Took main's, then `cargo update -p kulyrax-core` there. Second rebase clean. `cargo metadata --locked` consistent. Third rebase, onto `7e94a0d` (after #16): clean; main gained only `.github/workflows/audit.yml` and a ledger file, no code, so the gate and Docker results below still apply |
| Release notes | `RELEASES/v0.12.1.md` | added "Dependency updates": the RUSTSEC patches and rusqlite 0.43 that 0.12.1 includes, with the SQLite CVE's reachability |
| Gates on the rebased branch | build; clippy `-D warnings`; `fmt --check`; `cargo test --workspace --no-fail-fast` (`line-tables-only` debug info) | clean; clean; clean; 442 passed, 0 failed |
| Docker suites on the rebased branch (the fix's only CI: Actions is not available in the private fork) | `cargo test -p <backend>` against local kx-* containers after a Docker restart, one suite at a time, Elasticsearch with `--test-threads=1` | all green on the first run: Postgres 25/0, MySQL 19/0, Redis 12/0, MongoDB 15/0, Neo4j 17/0, Postgres outbox source 12/0, ingest 9/0, Elasticsearch 9/0 |
| F-03, reproduced first | `crates/core/tests/dlq_files.rs` against the old `dlq_write` (only `runner.rs` reverted) | 3 of 3 failed. Four events of one file with ids `a.b`, `a/b`, `a_b`, `A_B` left one DLQ file (`a_b.0001.ndjson.json`), so three events were lost. One event dead-lettered twice left one file. A 300-byte id with a 300-byte position failed the whole run with OS error 123 (invalid file name), a third problem found by the test |
| F-03, fixed | the same | 3 of 3 passed: four files; two files; names ≤ 255 bytes |
| F-04 | `crates/cli/tests/dlq_crash.rs`: kill at `dlq-before-rename:1`, `:2`, `dlq-written:1`, `:3`, then run again | passed first run. After each kill no checkpoint had been written. After the next run, every event had a complete DLQ file and at most one empty claimed file was left. A third run parked nothing new. A process kill cannot show the old missing fsync (the OS cache survives a kill); the fsync ordering is the power-loss fix, and the test proves no loss and no overwrite across kills |
| R0 / F-06, found | review of every URL/DSN echo; `kulyrax run` with `url: postgres://app:${KX_TEST_PW}@db.internal:54x32/app` | the Redis, Postgres, MySQL, MongoDB and Elasticsearch factories printed the expanded URL with its password (`does not parse: {url}`). Reported to the owner, nothing pushed. Clean: the outbox source, gRPC acks (no error text), `kulyrax sources`, and `reqwest` 0.12.28 errors (they omit userinfo; checked with a scratch program). The `Debug` of every config with a URL or password was derived and would print the password |
| R0 / F-06, Neo4j | `crates/adapters/neo4j/tests/redacted_config_errors.rs`, first run | failed: the Neo4j factory also printed the URI (``neo4j adapter "x": `uri` does not parse: bolt://app:<password>@…``). The R0 report had called Neo4j clean after reading only the inner check, which does not quote the URI, and missed the message that wraps it. Corrected: six adapters, not five |
| F-06, redaction helper | `cargo test -p kulyrax-core --lib redact` | first run 3/4: `password = 'value'` (libpq allows spaces around `=`) printed the quoted value. Second run 3/4: a malformed quoted value (`'it''s \' secret'`) still showed its tail. Fixed: a key may have spaces before `=`, and a value runs to the next whitespace outside quotes, so a malformed value is still taken whole. Then 4/4, including unterminated and mixed-case cases |
| F-06, every backend | `cargo test -p <crate> --test redacted_config_errors` for redis, postgres, mysql, mongodb, neo4j, elasticsearch, source-postgres | all 2/2 (after the Neo4j fix). Each test uses a URL with a password and a `password=`/`apiKey=`/`token=` parameter, made unparseable; the factory error (Display and Debug) and the config's Debug contain neither secret |
| F-06, through the binary | `crates/cli/tests/redacted_errors.rs`: `kulyrax run` with the password in `KX_TEST_DB_PASSWORD`; `kulyrax run` (continuous) switched to that URL, then `kulyrax reload` | first run 0/2. **Test setup**: validation refused the project first (Postgres route without a SQL mapping; a document mapping without a file adapter). Fixed the project, then 2/2: stderr, the reload reply and `reload-result` show `app:***@` and neither secret. On Unix `reload-result` is checked for mode 0600 (not exercised on this Windows machine; it runs on Linux in upstream CI once this is merged, since the private fork has no Actions) |
| Gates after A1b (F-03, F-04, F-06) | build; clippy `-D warnings`; `fmt --check`; `cargo test --workspace --no-fail-fast` | clean; clean; clean; 466 passed, 0 failed |
| Docker suites after A1b | `cargo test -p <backend>` against the local kx-* containers, one suite at a time, Elasticsearch with `--test-threads=1` | all green on the first run: Postgres 27/0, MySQL 21/0, Redis 14/0, MongoDB 17/0, Neo4j 19/0, Postgres outbox source 14/0, ingest 9/0, Elasticsearch 11/0 |

## Risks and open questions

- **Rebuild required** for stores holding ids outside the ordinary set (uppercase
  included). The refusal names the paths; the release notes give the steps.
- **Downgrade**: 0.12.0 on a store 0.12.1 opened finds no KV guards and re-applies
  events. Stated in the release notes with the manual undo.
- **DLQ names** for positions with special characters change. Scripts that build
  those names themselves must follow.
- The ES and MySQL test-hygiene problems (shared indices and tables on a
  long-lived node) are older than this change. Worth a separate task after the
  advisory is out.

## Outcome

Fixed on `security/file-names` and pushed to the private fork only. The advisory
text (Details / PoC / Impact) is drafted for the owner, who publishes the advisory
and the release together.
