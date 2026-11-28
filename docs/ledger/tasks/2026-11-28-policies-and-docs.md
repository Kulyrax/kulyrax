---
status: in-progress
---

# Step 6: policies and docs (F1-F4)

## Objective

Runbook Phase F, in one PR (the owner left one PR per step or one for all to
me; these are docs only):
- **F1:** `docs/stability.md` final: semver scope and exclusions (CLI human
  output, log text, error text, internal modules), deprecation policy (at
  least one minor release), MSRV policy (stated, CI-tested, a bump is a minor
  release), exit codes, `unstable-pb`. Regenerate the public-API snapshot for
  1.0.0-rc.1 into `docs/stability/api-1.0/` (cargo public-api, the 0.12.0
  method). Keep `docs/stability/api/` unchanged as history, and point
  stability.md at the 1.0 snapshot.
- **F2:** `SECURITY.md`: report through GitHub private advisories; supported
  versions.
- **F3:** README: drop "alpha", status "1.0 release candidate", known limits.
  The required-grants doc linked from the README (the owner's instruction).
- **F4:** CONTRIBUTING: the testing section (containers, ports, isolation
  rules).

## Acceptance criteria

- [x] F1, `stability.md`:
  - a new §0 "The promise": covered (API, YAML, CLI with exit codes and JSON
    fields, `.proto`, persisted formats) and not covered (human CLI output,
    log and error text, `doc(hidden)`/UNSTABLE items, `unstable-pb`,
    performance);
  - deprecation (at least one minor release, removal only in a major), MSRV
    (1.89, CI job `MSRV (1.89)`, a bump is a minor release), `unstable-pb`,
    how each surface is checked;
  - "draft" gone, "Proposal" columns now "Status", §8 marks steps 4-6.
- [x] F1, snapshot: `docs/stability/api-1.0/` for the 11 library crates; §1
  gives 0.12.0 and 1.0 sizes and points at it; `docs/stability/api/`
  unchanged.
- [x] F2: `SECURITY.md`. **Needs the owner**: private vulnerability reporting
  is disabled on the repository, so the link does not work for outsiders yet.
- [x] F3: README status and known limits; links SECURITY.md and
  `docs/required-grants.md` (also linked under "Learn more" since #37).
- [x] F4: CONTRIBUTING "DB-backed tests":
  - container commands with CI's images, ports and env values;
  - how to run a suite, the upgrade tests (`KULYRAX_V0121_BIN`) and the crash
    tests;
  - the format-marker isolation rules (Redis databases 13/14, the
    cluster-wide ES marker).
- [x] Checks: fmt; the old-name and YAML-parser greps (the snapshot must not
  trip them); rustdoc; links resolve.
- [ ] PR merged with every required check green.

## Constraints and assumptions

- Docs only, except the generated snapshot. No code changes.
- The snapshot is of main `56413bc`. G0's fixes and G1's version bump come
  later: G1 regenerates it and diffs it against this one, so the record stays
  true for rc.1.
- My wording, flagged for the owner, in SECURITY.md: acknowledgement within
  7 days, a fix aimed for within 90, 0.12.x supported until 1.0.0.

## Verification

| Check | Command / source | Result |
|---|---|---|
| Branch base | `git worktree add -b docs/f-policies … upstream/main` | `56413bc` (#38 merge) |
| Tools | `cargo public-api --version`; `rustup toolchain list` | 0.52.0 (as 0.12.0's); nightly installed |
| Snapshot | `cargo +nightly public-api -p <crate> --simplified > docs/stability/api-1.0/<crate>.txt` for the 11 crates of `docs/stability/api/` | all exit 0. Lines: core 3,972 (0.12.0: 5,449), client 175 (198), ingest 90 (208), source-postgres 79 (70), adapters 48-52 (40-71), backends 2 (2) |
| Snapshot vs the CI greps | the YAML-parser and old-name jobs' patterns over `docs/stability/api-1.0/` | no hits: the YAML job's allowlist (`docs/stability/api/`) needs no change |
| Snapshot holds no local paths | `grep` for `C:\`, `/c/Projects` and the user name | none |
| Item counts for §1 | `grep -c -E '^(#\[...\] )*pub (mod\|struct\|enum\|trait\|fn\|const\|type) '`, `^impl` in core's snapshot | 49 modules, 101 structs, 49 enums, 10 traits, 919 functions, 16 consts, 7 type aliases, 1,997 impls (a first count without the attribute prefix gave 27 structs, 0 enums: corrected) |
| Private vulnerability reporting | `gh api repos/Kulyrax/kulyrax/private-vulnerability-reporting` | `{"enabled":false}` |
| Linked advisories | `gh api repos/Kulyrax/kulyrax/security-advisories` | GHSA-6chh-gx4c-448g published (high), GHSA-x8vc-9h5c-gh55 published (low) |
| Outbox needs Postgres 13 | `grep` in `crates/sources/postgres/src/lib.rs` | `pg_current_snapshot`, `pg_snapshot_xmin`, `pg_snapshot_xmax`: functions added in Postgres 13 |
| MSRV | `Cargo.toml`; CI | `rust-version = "1.89"`; job `MSRV (1.89)` builds the workspace |
| CONTRIBUTING's claims | `ci.yml` images and env; `crates/cli/Cargo.toml` | postgres:16, mysql:8, redis:7, mongo:7 (`--replSet rs0`), neo4j:5, ES 8.15.0 with security off; `kulyrax-core` dev-dependency with `crash-points` |

## Risks and open questions

- For the owner:
  - enable private vulnerability reporting (Settings, Code security) before
    rc.1;
  - confirm or change SECURITY.md's timelines and supported versions.

## Outcome

In progress.
