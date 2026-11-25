---
status: completed
---

# Replace serde_yaml (1.0 plan, step 3)

## Objective

Replace the deprecated `serde_yaml` without changing how any existing config,
mapping or routing file is read. Decide first, with evidence on maintenance,
unsafe code, YAML 1.1 vs 1.2 parsing, line numbers in errors, and
`deny_unknown_fields` errors. `serde_yml` is excluded. Decision:
[2026-11-25-yaml-parser](../decisions/2026-11-25-yaml-parser.md).

## Acceptance criteria

- [x] Decision record comparing the maintained options against the owner's
  criteria, with evidence observed on 2026-11-25.
- [x] Owner's approval: accepted 2026-11-25 with four changes (the decision's
  "Owner's changes"; runbook C1).
- [x] C2: serde-saphyr 2.x behind `kulyrax_core::yaml`, with the strict options
  and `with_snippet: true`. Every error names the file, and the field path
  where available. A test checks that a wrong-type error in a mapping names the
  file, the line and the field path.
- [x] C3: `raw` is a `serde_json::Value` in `CustomAdapterConfig` and
  `CustomSourceConfig`, with before/after code in the release notes.
- [x] C4: the golden test compares serde_yaml 0.9 and `kulyrax_core::yaml` on
  every YAML file in the repo, every YAML string in Rust tests and examples, and
  every YAML code block in the docs. Both must give identical results, into the
  real structs and into `serde_json::Value`. It also holds the permanent refusal
  corpus, which both parsers must refuse.
- [x] C5: a CI grep allows only `crates/core/src/yaml.rs` and the golden test to
  name `serde_saphyr` or `serde_yaml`.
- [x] C6: serde-saphyr does not backport (its SECURITY.md). The move to 3.x is
  recorded: [2026-11-25-yaml-parser-3x](2026-11-25-yaml-parser-3x.md).
- [x] C7: serde_yaml removed, along with the `unsafe-libyaml` audit ignore.
- [x] Every difference goes in the release notes.
- [x] Four gates green; every Docker suite green; PR merged: #27, `3df837c` (CI 16/16).

## Constraints and assumptions

- Branch `docs/yaml-parser`, rebased onto main (`d04ca00`, after the rename).
- Kulyrax never writes YAML. The only `serde_yaml::to_value` calls build a `raw`
  config in tests.

## Verification

The harness is a scratch crate outside the repo
(`yaml-survey`, in the session scratchpad). It depends on every candidate at an
exact version and runs each on the same inputs.

| Check | Command / source | Result |
|---|---|---|
| Maintenance | crates.io API `GET /api/v1/crates/<name>` for each candidate | serde_yaml 0.9.34+deprecated 2024-03-25. yaml_serde 0.10.7 2026-08-18 (7 releases since 2026-01-24, repo github.com/yaml/yaml-serde). serde-saphyr 2.2.0 2026-11-18 plus 3.0.0-rc.1 (42 releases since 2025-09-27; 1.1 2026-08-15, 2.0 2026-10-13). serde_norway 0.9.42 2024-12-21. serde_yaml_ng 0.10.0 2024-05-26. serde_yaml_bw 2.5.8 2026-09-07. noyalib 0.0.64 2026-11-23 (61 releases since 2026-05-10) |
| Parsing behaviour | `cargo run` in the harness: 13 cases × 7 parsers, plus serde-saphyr with strict options | the decision record's Evidence §1 and §3. The four libyaml-based crates are identical on every case. serde-saphyr defaults read `yes`/`on` as `true` and expand merge keys; with `strict_booleans`, `MergeKeyPolicy::Error` and `leading_zero_numbers_as_strings` it matches serde_yaml on every case. noyalib takes the last of duplicate keys silently. serde_yaml_bw drops line numbers from unknown-field and type errors |
| Every YAML file in the repo | `git ls-files '*.yaml' '*.yml'` (41 files, `.github/` excluded) piped to the harness's `compare` binary: serde_yaml vs serde-saphyr (strict) vs yaml_serde into `serde_json::Value` | `same 41, differ 0` |
| 1.1-sensitive syntax in those files | `grep` for yes/no/on/off/y/n values, leading-zero numbers, `&`/`*`/`<<:`, `!!`, base-60 | none; the one `*` hit is a comment |
| Unsafe | `cargo tree -e normal -p <crate>`, then the `unsafe` keyword counted in each dependency's `src/` (comments included) | libyaml family: ~240 in the c2rust parser plus ~60 in the crate. serde-saphyr and granit-parser: `#![forbid(unsafe_code)]`. Numbers per crate in the decision record |
| Error traits | `fn needs<T: Error + Send + Sync + 'static>()` for each `Error` | serde_yaml, yaml_serde and serde_saphyr all satisfy it |
| Minimal features | scratch crate with `serde-saphyr = { version = "=2.2.0", default-features = false, features = ["deserialize"] }` and the strict options | builds and parses (`{"a":"yes","b":[1,2]}`) |
| MSRV and licence | each crate's `Cargo.toml` | serde-saphyr 1.89, edition 2024, MIT OR Apache-2.0. yaml_serde 1.82, edition 2021, MIT OR Apache-2.0; libyaml-rs MIT |
| Public API exposure | `git grep 'serde_yaml::Value'` in `crates/*/src`; `docs/stability/api/kulyrax-core.txt` | `CustomAdapterConfig.raw`, `CustomSourceConfig.raw` (`runtime/config.rs`) |
| Advisories | `cargo audit` (cargo-audit, advisory DB fetched 2026-11-25) on the harness; web search of RustSec | **RUSTSEC-2024-0449: `unsafe-libyaml` is unmaintained, no fixed upgrade.** That is today's serde_yaml parser, and serde_yaml_ng's. Nothing for yaml_serde/libyaml-rs, serde-saphyr/granit-parser, serde_norway or noyalib. Also RUSTSEC-2025-0068 (serde_yml, unsound and unmaintained) and RUSTSEC-2025-0067 (libyml) |

First-run failures: the harness's dispatch macro took the parser as a `path`, which
cannot take a turbofish (compile error; now an `ident`). The `compare` binary got
MSYS paths it could not open (now `pwd -W`). Both were harness bugs, not
findings.

Implementation (runbook C2–C7), on `docs/yaml-parser` from `d04ca00`:

| Check | Command | Result |
|---|---|---|
| serde-saphyr API | its 2.2.0 sources | `with_deserializer_from_str_with_options` hands a closure the real deserializer, so `serde_path_to_error` can wrap it; the closure's errors still get the snippet. Options exist for every recorded setting; duplicate keys default to `Error` |
| `kulyrax_core::yaml` unit tests, first run | `cargo test -p kulyrax-core --lib yaml::` | 4 of 5. **Test bug**: I expected a syntax error inside `adapters: [` to have no field, but it belongs to `adapters`, which is right. Test rewritten: it names the file and the line; an error outside any field has none. Then 5/5 |
| Loader test, first run | `cargo test -p kulyrax-core --lib` | `invalid_yaml_names_the_file` failed. **Expected**: it matched the old wording `Invalid YAML in`. Now `Invalid SQL mapping: <file>: …`; the test also checks the line. Then 103/103 |
| Every call site moved | `git grep 'serde_yaml::'` outside `yaml.rs` | 180 `from_str`, 13 `to_value`, 5 `Value` in 94 files, all moved to `kulyrax_core::yaml` / `serde_json`. `raw` is a `serde_json::Value` (C3) |
| Workspace on the new parser, first run | `cargo test --workspace --no-fail-fast` | 523 passed, 0 failed |
| Golden mode, run 1 | `cargo test --workspace --features kulyrax-core/yaml-golden` with `KULYRAX_YAML_GOLDEN_LOG` | 1,508 lines, 0 marked different, **but 164 malformed**: parallel processes appended to one file. Unusable. Fixed: one file per process and a mutex per write |
| Golden mode, run 2 | the same | 1,733 comparisons, 0 malformed, **4 different**, plus 2 golden-test failures. **Finding**: serde_yaml 0.9 refuses a duplicate key or `<<` only into a struct. Into a map or `Value`, the last duplicate wins and `<<` is an ordinary key; the recorded options refused both. Also two **test bugs**: an "untyped duplicate key" refusal case that serde_yaml never refused, and a docs-block minimum of 20 where there are 16 |
| `duplicate_keys` and `merge_keys` options | probe test, every case against serde_yaml | `PassThrough` + `AsOrdinary` match serde_yaml on all six cases (struct, map and `Value`, duplicate and merge). `LastWins` and `FirstWins` accept a struct duplicate, so they don't match. **Owner chose "Read exactly as 0.12"** (decision record) |
| Golden mode, run 3 | the same | **1,687 comparisons across 168 test processes: 1,687 same, 0 different, 0 malformed.** 529 passed, 0 failed, including `yaml_golden.rs`: every YAML file, the 16 docs blocks, the refusal corpus, and the owner's file/line/field test |
| Real errors from the binary | `kulyrax run` with a bad config and a bad mapping, 1.0 and 0.12.1 | 1.0 printed the error with `Debug`, so the snippet came out as `\n` escapes. Fixed: `main` prints with `Display`, exit code still 1. A config error inside an adapter entry points at the entry (`adapters[0]`, its first line), as 0.12's did (`adapters`, line 5). A mapping error points at the field and line. Both samples are in the release notes |
| Golden mode over every Docker suite | `cargo test -p <crate> --features kulyrax-core/yaml-golden -- --test-threads=1`, every backend URL set (Neo4j on this branch's 7688), and `KULYRAX_V0121_BIN` | postgres 30, mysql 25, redis 14, mongodb 21, elasticsearch 11, neo4j 20, source-postgres 14, ingest 14, cli 32, all passed. **1,342 comparisons across 150 processes: 1,342 same, 0 different, 0 malformed** |
| clippy, first run | `cargo clippy --workspace --all-targets -- -D warnings` | `result_large_err`: `YamlError` was 160 bytes. Fixed: the parser's error is boxed. Then clean, with and without `yaml-golden` |
| CI on PR #27 | the PR's checks | golden stage `b76405b` (serde_yaml still present, `yaml_golden.rs` with both halves): 15/15 green. Removal `0574fab`: 16/16, including the new `YAML parser only in kulyrax_core::yaml`. Merged as `3df837c` |
| C7: serde_yaml gone | `grep -c 'name = "serde_yaml"'` and `"unsafe-libyaml"` in every lockfile | 0 in `Cargo.lock` and each example's lockfile |
| C5 check, locally | the CI job's grep | no hits outside the allowlist |
| `cargo audit` without the `unsafe-libyaml` ignore | `cargo audit --file <lock> --deny unsound --deny unmaintained --deny yanked`, every lockfile | exit 0 for all 7 |
| Docker suites after C7, first run | `docker-suites.sh` | Redis **4 of 9 failed** (`redis_batch_*` ×3, `redis_facet_disjoint`); passed on rerun. **Test bug, pre-existing**: Redis namespaces were `t<pid>-<n>`, the tests never delete keys (691 left on the server), and Windows reuses process ids. A run that drew an earlier run's id met its guards, and its events were stale. Reproduced by pinning the id to a leftover one (14444): the same 4 failed, plus `redis_delete_lifecycle`. Fixed: Redis namespaces and Elasticsearch indexes add the run's start time (`t<pid>x<nanos>-<n>`). Elasticsearch had the same risk (`create_index` fails on an existing index). CONTRIBUTING updated. The other backends use per-binary databases recreated each run, or a wipe |
| C7 gates and Docker suites | fmt; clippy `-D warnings`; `cargo test --workspace`; `cargo run --manifest-path examples/embedded/Cargo.toml`; every Docker suite (Neo4j on 7688) | clean; clean; 526 passed, 0 failed; the example projects `users.u1`; postgres 30, mysql 25, mongodb 21, elasticsearch 11, neo4j 20, source-postgres 14, ingest 14, cli 32. Redis after the fix: 14/14 and Elasticsearch 11/11, rerun with `--nocapture`, no SKIP lines |
| Non-finite floats into `raw` | probe: `.inf` / `.nan` into `serde_json::Value` | new: refused, with field and line. serde_yaml into `serde_json::Value`: `null`. 0.12's `raw` held the YAML float |

## Risks and open questions

- serde-saphyr's API churn (three majors since August 2026). Contained by one
  module and a major pin; yaml_serde is the fallback.
- The YAML embedded in Rust tests and docs is not compared yet. The golden test
  covers it after approval.
- `raw: serde_json::Value` changes the API for third-party factories. The owner
  accepted it with the decision.
