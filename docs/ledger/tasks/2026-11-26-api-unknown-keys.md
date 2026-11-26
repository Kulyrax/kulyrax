---
status: completed
---

# Unknown keys refused in every built-in config, mapping and routing struct (step 4, D5)

## Objective

Runbook D5: "`deny_unknown_fields` on every built-in config/mapping/routing
struct; the error names the key, file and line. A test per file kind. Extension
configs document their own behaviour." (`stability.md` "Unknown keys".) In 0.12
a misspelt key was ignored, so `depends_om:` silently did nothing. This is
tranche 3 of Phase D (D5, D6, D7).

## Acceptance criteria

- [x] `#[serde(deny_unknown_fields)]` on:
  - `KulyraxConfig`, `RoutingConfig`;
  - the 4 built-in adapter entries and their `config:` structs;
  - `DirectorySourceConfig`, `StdinSourceConfig`;
  - `SqlMapping`, `DocumentMapping`, `KvMapping`, `GraphNodeMapping`,
    `GraphEdgeMapping`, and the subgraph's raw form;
  - the 6 networked backends' configs.

  `OutboxConfig` and `OutboxColumns` already had it.
- [x] Custom adapter and source entries refuse a top-level key core does not
  read, with serde's `unknown_field` message. The `config:` block stays the
  backend's.
- [x] The error names the key, the file, and the key's own line, for every file
  kind. Tagged and hand-parsed entries lose key positions, so
  `kulyrax_core::yaml` finds the key's line in the text.
- [x] A test per file kind (`crates/core/tests/unknown_keys.rs`, 11 cases):
  - config: top level, `routing:`, built-in entry, built-in block, custom
    entry, source entries;
  - mappings: SQL, document, key-value, graph node/edge, subgraph.

  The networked blocks are covered in each crate's `tests/from_config.rs`.
  There are also `yaml.rs` unit tests for the line lookup.
- [x] Docs:
  - `stability.md` "Unknown keys";
  - `writing-a-backend.md` (extension configs document their own rule);
  - the rc.1 notes (behaviour change, upgrade advice).
- [x] Gates: fmt, clippy `-D warnings`, workspace tests, examples, every Docker
  backend suite.
- [x] PR #33 merged with the 12 required checks green (all 18 pass; merge commit 463af2c).

## Constraints and assumptions

- `routing.yaml` has no fixed keys: each key is an event type. Its "file kind"
  test is the `routing:` block in `config.yaml`.
- A networked backend's `config:` block is parsed by its factory from JSON,
  after the file is read. Its error names the key and the adapter id, not the
  line. Recorded as a limit, not fixed: carrying source positions into
  `CustomAdapterConfig` would be new API.
- The line lookup is textual: the first `key:` at or after the line the parser
  reported. A key the parser cannot place and that is not written as a line
  (a flow mapping, `{ a: 1, bad: 2 }`) keeps the parser's own message.

## Verification

| Check | Command | Result |
|---|---|---|
| Existing YAML still reads | `cargo test -p kulyrax-core` (incl. `yaml_golden`, every YAML file and docs block in the repo) | 340 pass, 0 fail: no existing file had an unknown key |
| File-kind tests, first run | `cargo test -p kulyrax-core --test unknown_keys` | 7 pass, 4 fail. Cause: tagged / hand-parsed entries (adapters, sources, graph mappings) report the entry's first line or no line. Fixed by the line lookup in `yaml.rs`. Then 9 pass, 2 fail. Cause: wrong expected line numbers in the test (miscounted); corrected. Then 11 pass |
| Networked blocks, first run | the D5 test edit script | it stopped on MongoDB (rustfmt had split the `json!` line); fixed the pattern, then all 6 converted. 5 pass in each crate |
| `yaml.rs` unit tests | `cargo test -p kulyrax-core --lib yaml::` | 8 pass |
| clippy, first run | `cargo clippy --workspace --all-targets -- -D warnings` | 1 error: `collapsible_if` in `deny_unknown_keys` (cause: nested `if let`); rewritten as one expression, then clean |
| fmt | `cargo fmt --check` | clean |
| Workspace tests | `cargo test --workspace` | 578 pass, 0 fail |
| Examples | the 6 example manifests; `examples/embedded` run | 0 errors each; `projected: users.u1.name = "Ada"` |
| Docker suites (Neo4j on 7688) | `docker-suites.sh` | postgres 35, mysql 30, redis 19, mongodb 26, elasticsearch 16, neo4j 25, source-postgres 16, ingest 15, cli 32: all pass, 0 fail |

## Risks and open questions

- **A breaking change for any project whose files hold an ignored key.** It is
  documented in the rc.1 notes, with the upgrade advice: start once against a
  scratch copy.

## Outcome

Completed. Every built-in config, mapping and routing struct refuses an unknown key, naming the key, the file and its line.
