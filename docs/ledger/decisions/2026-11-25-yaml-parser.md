---
status: accepted
---

# Replace serde_yaml with serde-saphyr, behind one module, configured to read YAML as 0.12 did

Task: [2026-11-25-yaml-parser](../tasks/2026-11-25-yaml-parser.md)

## Context

`serde_yaml` is deprecated: 0.9.34+deprecated (2024-03-25) is its last release, and
it parses through `unsafe-libyaml`, a c2rust translation of libyaml. `cargo audit`
flags that parser: RUSTSEC-2024-0449, unmaintained, no fixed upgrade. Kulyrax reads
three kinds of YAML with it:

- the config;
- mappings (SQL, document, key-value, graph);
- routing.

Kulyrax never writes YAML. Two facts shape the choice:

- **The parser's types are in the public API.** `CustomAdapterConfig.raw` and
  `CustomSourceConfig.raw` are `serde_yaml::Value`, so third-party backends and
  sources deserialize their config from it. Whatever we pick there becomes a 1.0
  semver dependency.
- **Step 4 makes every built-in config, mapping and routing struct
  `deny_unknown_fields`**, with an error that names the key, the file, and the line.

Owner's criteria (1.0 plan, step 3): maintenance, unsafe code, YAML 1.1 vs 1.2
differences that would change how existing files parse (yes/no/on/off, octal,
anchors, merge keys), errors with line numbers, and how `deny_unknown_fields`
errors look. `serde_yml` is excluded.

## Options considered

| | Last release | Releases | Parser | `unsafe` in the YAML-specific tree | Reads files as serde_yaml 0.9 does? |
|---|---|---|---|---|---|
| **serde_yaml** 0.9.34 (today) | 2024-03-25, deprecated | 86 | `unsafe-libyaml` (c2rust) | ~300 (crate 60, parser 240) | n/a |
| **yaml_serde** 0.10.7 (YAML org fork) | 2026-08-18 | 7, first 2026-01-24 | `libyaml-rs` 0.3.0 (c2rust fork) | ~295 (crate 59, parser 236) | yes, identical in every case |
| **serde-saphyr** 2.2.0 | 2026-11-18 (3.0.0-rc.1 the same day) | 42, first 2025-09-27 | `granit-parser` (pure Rust, `forbid(unsafe_code)`) | crate `forbid(unsafe_code)`; parser none | **with four options set, yes, in every case**; not by default |
| serde_norway 0.9.42 | 2024-12-21 | 7 | `unsafe-libyaml-norway` (c2rust) | ~300 | yes |
| serde_yaml_ng 0.10.0 | 2024-05-26 | 3 | `unsafe-libyaml` (unmaintained) | ~300 | yes |
| serde_yaml_bw 2.5.8 | 2026-09-07 | 24 | `granit-parser` + `unsafe-libyaml-norway` + `regex` | ~350 | no: misspelt-key and type errors lose their line numbers |
| noyalib 0.0.64 | 2026-11-23 | 61 in 6 months, 0.0.x | own (`forbid(unsafe_code)`) | ~10 (doc text) | no: duplicate keys silently take the last value |

"`unsafe`" counts are occurrences of the keyword in each crate's sources, comments
included, so treat them as orders of magnitude. General-purpose crates (`smallvec`,
`encoding_rs`, `simdutf8`, `serde_core`) appear in several trees and are left out.

Ruled out:

- **serde_norway and serde_yaml_ng**: no release in two years, and the same c2rust
  parser.
- **serde_yaml_bw**: worse errors, and still carries the c2rust parser.
- **noyalib**: too young (0.0.x), and it accepts duplicate keys silently. Its
  author also wrote `serde_yml`, which is now a shim over noyalib; `serde_yml` was
  flagged unsound and unmaintained (RUSTSEC-2025-0068).

That leaves **yaml_serde** (lowest migration risk) and **serde-saphyr** (no
unsafe, active).

## Evidence

All observed on 2026-11-25. The harness is a scratch crate that ran every candidate
on the same inputs (kept outside the repo). Details are in the task's Verification.

**1. How each reads the cases that differ between YAML 1.1 and 1.2.** The four
libyaml-based crates agree on every row, so they share one column.

| Input | serde_yaml / yaml_serde / norway / ng | serde-saphyr, defaults | serde-saphyr, strict options |
|---|---|---|---|
| `flag: yes` into `bool` | error: string `"yes"`, expected a boolean | `true` | error (strict mode expects true/false) |
| `flag: on` into `bool` | error | `true` | error |
| `flag: no` into `String` | `"no"` | `"no"` | `"no"` |
| `mode: 0755` into `i64` | error (a string) | error | error |
| `mode: 0o755` into `i64` | 493 | 493 | 493 |
| `n: ~` into `Option` | `None` | `None` | `None` |
| anchor + alias | works | works | works |
| merge key `<<` | not expanded: `missing field x` | **expanded** | error: merge keys not allowed, line 3 |
| duplicate key | error | error, line 2 | error, line 2 |
| untyped `c: yes` (into `serde_json::Value`) | `"yes"` | **`true`** | `"yes"` |

The strict options are:

```rust
serde_saphyr::options! {
    strict_booleans: true,
    merge_keys: MergeKeyPolicy::Error,
    leading_zero_numbers_as_strings: true,
    with_snippet: false,
}
```

(The owner changed `with_snippet` to `true`; see "Owner's changes" below.)

With them, serde-saphyr agrees with serde_yaml 0.9 on every row. The only
difference is that a merge key fails with a clearer message, where serde_yaml
fails with a confusing one.

**2. Every YAML file in the repo.** All 41 tracked `.yaml` files (configs,
mappings, routing, fixtures, examples) parse to identical `serde_json::Value`s
with serde_yaml 0.9, yaml_serde and serde-saphyr (strict). None of them uses
yes/no/on/off, a leading-zero number, an anchor, a merge key or a tag.

The YAML embedded in Rust tests (197 `serde_yaml::` call sites) and in the docs'
code blocks is not covered yet. The golden test covers it after approval.

**3. Errors.**

| Case | serde_yaml / yaml_serde | serde-saphyr (strict, no snippet) |
|---|---|---|
| misspelt key, `deny_unknown_fields` | ``adapters[0]: unknown field `priorty`, expected one of `id`, `priority`, `config` at line 5 column 5`` | ``unknown field `priorty`, expected one of id, priority, config at line 5, column 5`` |
| wrong type | ``adapters[0].priority: invalid type: string "high", expected u32 at line 4 column 15`` | `invalid u32 at line 4, column 15` |
| duplicate key | `` duplicate field `a` `` (no line) | `duplicate mapping key: a … at line 2, column 1` |

Both give line and column. serde_yaml also names the field path and the bad
value; serde-saphyr does not. With `with_snippet: true` (its default),
serde-saphyr prints the offending lines with a caret instead, which says the same
to a person and more about where. None of them knows the file name: Kulyrax adds
it (step 4).

**4. Error types.** `serde_yaml::Error`, `yaml_serde::Error` and
`serde_saphyr::Error` are all `std::error::Error + Send + Sync + 'static`, which
step 4 requires for `source()` chains.

**5. Fit.**
- serde-saphyr: `rust-version = 1.89` (ours), edition 2024, MIT OR Apache-2.0.
  yaml_serde: 1.82, edition 2021.
- serde-saphyr only deserializes into Rust types. It has no `Value` tree of its
  own, but any `Deserialize` type works, including `serde_json::Value`.
- serde-saphyr has published 1.x, 2.x and a 3.0 release candidate since
  August 2026. Its API moves fast.

## Decision

1. **Parser: serde-saphyr 2.x**, with `default-features = false, features =
   ["deserialize"]` (Kulyrax never writes YAML), and the strict options above.
   Pinned to `2`; a major upgrade is our own change, made with the golden test.
2. **One module owns YAML**: `kulyrax_core::yaml`, with `from_str` / `from_file`.
   It applies the options, puts the file name into every error, and is the only
   place that names the crate. Every other crate, test and example calls it. No
   serde-saphyr type appears in Kulyrax's public API, so its majors do not
   become ours.
3. **`raw` becomes `serde_json::Value`** in `CustomAdapterConfig` and
   `CustomSourceConfig`: a type on a stable 1.x crate that third-party factories
   already know (`serde_json::from_value`). YAML-only shapes have no JSON
   equivalent:
   - non-string keys become strings (`1: x` → `"1"`; every parser does this
     already in the test above);
   - non-finite floats are refused.

   This is an API change. It goes in the release notes with before/after code.
4. **Golden test before removal.** Every YAML file in the repo, every YAML
   string in Rust tests and examples, and every YAML code block in the docs is
   parsed with serde_yaml 0.9 and with `kulyrax_core::yaml`, into the real
   Kulyrax structs (config, each mapping kind, routing) and into
   `serde_json::Value`. The results must be identical. serde_yaml is removed only
   when this passes. Every difference found goes in the release notes.
5. **Error format** in the release notes: what changes for a person reading them,
   namely the source snippet in place of the field path. Scripts that parsed
   serde_yaml's messages break; error text was never covered by semver (stability
   §3).

### Owner's changes

Accepted 2026-11-25 with four changes. The source is the owner's message,
quoted:

> Step 3 record: approved with four changes. Update it to accepted and implement after the CI branch is committed.
>
> 1. with_snippet: true. kulyrax_core::yaml adds the file name to every error, and the field path where available (serde_path_to_error or saphyr's own path). Add a test asserting that a wrong-type error in a mapping names the file, the line, and the field path.
> 2. Golden test covers refusals: every input serde_yaml 0.9 refuses (the table's error rows: yes/on into bool, 0755 into int, merge key, duplicate key, wrong type, unknown field under deny_unknown_fields) must also be refused by kulyrax_core::yaml. Keep that corpus permanently as a regression test, even after serde_yaml is removed.
> 3. CI grep: only crates/core/src/yaml.rs and the golden test may name serde_saphyr / serde_yaml. Same mechanism as the conduit check.
> 4. Find out whether serde-saphyr backports fixes to 2.x. If not, record a task: move to 3.x when it's final, gated by the golden test, before 1.0.0-rc.1.

What each means for the decision above:

1. **Errors.** The options use `with_snippet: true`. `kulyrax_core::yaml` puts
   the file name into every error, and the field path where one is available. A
   test checks that a wrong-type error in a mapping names the file, the line and
   the field path. This replaces "Error format" in item 5: errors keep the field
   path *and* gain the snippet.
2. **Refusal corpus.** The golden test also holds every input serde_yaml 0.9
   refuses:
   - `yes` and `on` into `bool`;
   - `0755` into an integer;
   - a merge key;
   - a duplicate key;
   - a wrong type;
   - an unknown field under `deny_unknown_fields`.

   `kulyrax_core::yaml` must refuse each one. The corpus stays as a permanent
   test after serde_yaml is removed.
3. **CI grep.** Only `crates/core/src/yaml.rs` and the golden test may name
   `serde_saphyr` or `serde_yaml`. This works like the old-name job. Cargo
   manifests and the lock file spell the crate `serde-saphyr`, and
   `docs/ledger/` and `RELEASES/` record history, so they are outside the check.
4. **Backports: none.** serde-saphyr's SECURITY.md (master, read 2026-11-25)
   says: "serde-saphyr supports only the latest released version. Security
   fixes are provided via new releases; we do not backport fixes to older
   versions." The latest stable release is 2.2.0; 3.0.0-rc.1 came out the same
   day, 2026-11-18. A task records the move to 3.x once 3.0 is final, gated by
   the golden test, before 1.0.0-rc.1:
   [2026-11-25-yaml-parser-3x](../tasks/2026-11-25-yaml-parser-3x.md).

Fallback: if the golden test finds a difference we cannot configure away, or
serde-saphyr's API churn makes the pin unworkable before 1.0, **yaml_serde** takes
its place behind the same module. It behaved identically to serde_yaml in every
case here, so only the unsafe-code criterion would be given up.

## Trade-offs accepted

- **A young, fast-moving dependency** (42 releases in 14 months, three majors
  since August) in exchange for a parser with no unsafe code. Contained by the
  single module and the pin.
- **Error messages lose the field path and the offending value**, and gain a
  source snippet.
- **`raw: serde_json::Value`** costs the few YAML shapes JSON cannot hold, and
  changes the API for third-party factories once, before 1.0.
- **serde-saphyr's YAML 1.1 leniencies are turned off** (yes/on as booleans, merge
  keys). They would have been new behaviour; configs relying on them never
  existed, because serde_yaml 0.9 refused them.

## Revisit if

- serde-saphyr goes six months without a release, or its maintainer stops
  answering issues: switch to yaml_serde behind the same module.
- yaml_serde or libyaml-rs replaces its c2rust core with safe Rust: it becomes the
  lower-risk choice.
- A user asks for merge keys: turn on `MergeKeyPolicy::Merge` in a minor release.
  That accepts input that is refused today, so nothing that worked breaks.
