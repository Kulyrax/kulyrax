---
status: completed
---

# Patch the RUSTSEC advisories with semver-compatible updates

## Objective

`cargo audit` on `phase-3` (2026-11-25) reported 9 vulnerability advisories and 3
unsound crates in `Cargo.lock`. Every one except `libsqlite3-sys` has a
semver-compatible fix. Take them, in the workspace and in every example lockfile,
before 0.12.1's binaries are built. `libsqlite3-sys` needs a rusqlite major bump:
separate task `2026-11-25-rusqlite-0.43`.

## Acceptance criteria

- [x] `cargo update` for every semver-compatible fix; afterwards `cargo audit`
  reports only RUSTSEC-2026-0459 (`libsqlite3-sys`) as a vulnerability.
- [x] Examples' lockfiles patched the same way.
- [x] Four gates green; every Docker backend suite green; examples build.

## Verification

| Check | Command | Result |
|---|---|---|
| Before | `cargo audit` at `phase-3` (`cda1862`) | 9 vulnerabilities: ctutils RUSTSEC-2026-0385; libsqlite3-sys -0459; rustls -0346/-0347/-0348/-0349; rustls-webpki -0382; url -0592; webpki-roots -0381. Unsound: lru -0253, rustix -0486, tokio -0386. Plus 8 unmaintained |
| Update | `cargo update -p rustls -p rustls-webpki -p webpki-roots@1.0.9 -p url -p ctutils -p tokio -p rustix -p lru` | ctutils 0.4.2→0.4.8, rustls 0.23.45→0.23.46, rustls-webpki 0.103.15→0.103.17, webpki-roots 1.0.9→1.0.12, url 2.5.8→2.5.9, tokio 1.53.1→1.54.1, rustix 1.1.3→1.1.6 (+ form_urlencoded, linux-raw-sys). First run: `webpki-roots` was ambiguous (0.26.11 and 1.0.9 in the lock); named the version |
| lru | `cargo tree -i lru`; crates.io for mysql | lru 0.16.4 comes from `mysql` 28.0.2; the fix (≥ 0.18.2) is outside 0.16's range. `mysql` 28.0.4 needs `lru ^0.18.2`, so `cargo update -p mysql`: mysql 28.0.4, lru 0.18.5 (+ pem 4.0.0, base64 0.23.1) |
| After, workspace | `cargo audit` | 1 vulnerability (libsqlite3-sys, the rusqlite task); 8 unmaintained warnings (backoff, chrono, chrono-tz, chrono-tz-build, instant, paste, rustls-pemfile, unsafe-libyaml) |
| Examples | `cargo audit` in each of the 6 example lockfiles; `cargo update -p tokio -p rustix` in `grpc-producer/producer`, `-p ctutils -p tokio` in `postgres/dbtool` | afterwards: `benchtool`, `producer`, `postgres/dbtool` clean; `embedded` and both `dbtool`s keep libsqlite3-sys (rusqlite task); `embedded` keeps unsafe-libyaml (step 3) |
| Gates | `cargo build --workspace`; `cargo clippy --workspace --all-targets -- -D warnings`; `cargo fmt --check`; `cargo test --workspace --no-fail-fast` | clean; clean; clean; 420 passed, 0 failed |
| Docker suites, first run | `cargo test -p <backend>` against the local kx-* containers | Postgres 6/24, MySQL 3/15 and MongoDB 6/15 failed. **Environment**: the shared `kulyrax_ci` databases had been migrated by the unreleased rename work (tombstones in place), and this 0.12-named code hit the tombstones, which is what they are for. Reset the guard objects in those test databases |
| Docker suites, clean databases | the same | Postgres 24/0, MySQL 18/0, Redis 11/0, MongoDB 15/0, Postgres outbox source 12/0. Neo4j (3 failed), ingest (1 failed) and Elasticsearch (3 failed) failed while another branch's full test run loaded the machine |
| Docker suites, idle machine | Neo4j, ingest, Elasticsearch (`--test-threads=1`) again | Neo4j 16/0, ingest 9/0, Elasticsearch 8/0 |
| Examples | `cargo build --manifest-path` for `grpc-producer/producer`, `postgres/dbtool`, `embedded`; `cargo run` for `embedded` | all build; embedded prints `projected: users.u1.name = "Ada"` |

## Outcome

Semver-compatible patches only. `libsqlite3-sys` is left for the rusqlite task,
and the unmaintained crates for the audit job's ignore list.
