---
status: completed
---

# Routing is YAML; JSON routing files are rejected

## Objective

Every file a user writes for Kulyrax should use one format. Config and mappings were YAML, but routing was `routing.json`. The owner asked to remove JSON so the system has one systematic way of doing things. Routing stays a separate file, now `routing.yaml`. An old `routing.json` must fail with a message saying how to convert it, not a bare parse error.

Out of scope: events stay JSON (the producer wire contract, including the gRPC payload). Files Kulyrax writes for itself (checkpoints, guard sidecars, DLQ entries) and `--output json` are not user-written config, so they are unchanged.

## Acceptance criteria

- [x] `load_routing` parses YAML from `.yaml` / `.yml` files.
- [x] A `.json` routing file fails at startup with conversion guidance.
- [x] Any other extension fails with a message naming the rule.
- [x] Every `routing.json` in examples and test fixtures is converted, with entry order kept.
- [x] No reference to `routing.json` remains outside `docs/ledger/` and `RELEASES/`.
- [x] The routing file format is documented.
- [x] fmt, clippy `-D warnings` and `cargo test --workspace` pass.

## Constraints and assumptions

- Constraint: pre-1.0, so the break is acceptable (owner chose "remove, with a clear error"). It needs a release-note line.
- Verified: routing is the only JSON file a user writes. A survey of `serde_json::from_*` call sites shows the rest parse events, checkpoints, sidecars and adapter payloads.
- Many test configs set `file: "routing.json"` only as a placeholder; they pass routing rules in directly. They were renamed anyway so the repo has no stale name.

## Decisions

- Owner's choices (2026-10-17): routing stays a separate `routing.yaml` rather than a section of `config.yaml`, and JSON routing is removed with a guided error.
- The extension decides, not the content. YAML is a superset of JSON, so a `.yaml` file written in JSON syntax still loads. Policing the syntax would add parsing for no user benefit.
- The check lives in `load_routing`, so `run`, `dry-run`, `replay`, `explain` and `ingest` all get it through `load_project` / `explain` without separate wiring.

## Verification

| Criterion | Check | Observed result |
|---|---|---|
| Loader rules | `cargo test -p kulyrax-core --test routing` | 7 passed: 3 existing plus JSON rejected with guidance, `.txt` rejected, `.yml` loads, invalid YAML names the file |
| Conversion kept order | read the converted `crates/core/tests/fixtures/routing.yaml` and `examples/automotive-csv-producer/routing.yaml` | entries in the original order, one `Event: [ids]` line each |
| End to end, new format | quickstart copy, `dbtool init`, `kulyrax run --once` | exit 0; 3 processed, 3 succeeded; matches `expected/users.txt` |
| End to end, old project | quickstart copy with `routing.json` and `file: routing.json` | exit 1 at startup: `Routing("JSON routing files are no longer supported (routing.json). Rename it to routing.yaml, …")` |
| No stale references | `grep -rI routing\.json` excluding `target/`, `.git/`, `ledger/`, `RELEASES/` | none |
| Example outside the workspace | `cargo check --manifest-path examples/embedded/Cargo.toml` | exit 0 |
| Gates | `cargo fmt --check`; clippy `-D warnings`; `cargo test --workspace --no-fail-fast` | clean; clean; 103 test binaries, 300 passed, 0 failed |

Not run: `examples/automotive-csv-producer` under Docker Compose (needs Postgres, MongoDB and Elasticsearch). Its compose file now mounts `routing.yaml`; checked by reading the diff only.

## Risks and open questions

- Breaking change for anyone with a `routing.json`. Release-note line: "Routing is now YAML. Rename `routing.json` to `routing.yaml`, write each entry as `EventType: [adapter-id, ...]`, and update `routing: file:`."

## Outcome

Config, routing and mappings are all YAML. An old JSON routing file stops startup with instructions for converting it.
