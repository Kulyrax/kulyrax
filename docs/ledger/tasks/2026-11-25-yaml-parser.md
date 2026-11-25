---
status: in-progress
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
- [ ] C2: serde-saphyr 2.x behind `kulyrax_core::yaml`, with the strict options
  and `with_snippet: true`. Every error names the file, and the field path
  where available. A test checks that a wrong-type error in a mapping names the
  file, the line and the field path.
- [ ] C3: `raw` is a `serde_json::Value` in `CustomAdapterConfig` and
  `CustomSourceConfig`, with before/after code in the release notes.
- [ ] C4: the golden test compares serde_yaml 0.9 and `kulyrax_core::yaml` on
  every YAML file in the repo, every YAML string in Rust tests and examples, and
  every YAML code block in the docs. Both must give identical results, into the
  real structs and into `serde_json::Value`. It also holds the permanent refusal
  corpus, which both parsers must refuse.
- [ ] C5: a CI grep allows only `crates/core/src/yaml.rs` and the golden test to
  name `serde_saphyr` or `serde_yaml`.
- [x] C6: serde-saphyr does not backport (its SECURITY.md). The move to 3.x is
  recorded: [2026-11-25-yaml-parser-3x](2026-11-25-yaml-parser-3x.md).
- [ ] C7: serde_yaml removed, along with the `unsafe-libyaml` audit ignore.
- [ ] Every difference goes in the release notes.
- [ ] Four gates green; every Docker suite green; PR merged.

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

## Risks and open questions

- serde-saphyr's API churn (three majors since August 2026). Contained by one
  module and a major pin; yaml_serde is the fallback.
- The YAML embedded in Rust tests and docs is not compared yet. The golden test
  covers it after approval.
- `raw: serde_json::Value` changes the API for third-party factories. The owner
  accepted it with the decision.
