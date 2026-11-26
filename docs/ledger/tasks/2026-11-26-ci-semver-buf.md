---
status: in-progress
---

# CI: buf breaking (required) and a semver report (step 4, D9)

## Objective

Runbook D9, as the owner set it on 2026-11-26:
- `buf breaking` against proto v1 is a **required** check from the start, because
  the wire must not break.
- `cargo-semver-checks` (stable) against **v0.12.2**, the latest release. During
  Phase D it runs as a **non-required** job in report mode and uploads the list
  of breaking changes as an artifact.
- After the 1.0.0-rc.1 tag, semver-checks becomes required and strict against
  that tag (runbook G4).

This is tranche 2 of Phase D's split (D3, D4, D9). D9 goes first, so `buf
breaking` guards the wire while D3 reworks the gRPC types.

## Acceptance criteria

- [x] Job `proto wire compatible with v0.12.2`: `buf breaking proto --against
  '.git#tag=v0.12.2,subdir=proto'`.
- [x] Job `API changes since v0.12.2 (report)`: `cargo semver-checks --workspace
  --baseline-rev v0.12.2`. It never fails, and uploads `semver-report.txt`.
- [x] Both run on the PR. The buf job passes on the current proto and fails on a
  deliberately breaking commit, which is then reverted.
- [ ] The buf job is made required in the ruleset (the owner's setting).
- [ ] PR merged.

## Constraints and assumptions

- In report mode, semver-checks runs without `--release-type major`. With
  `major` it would accept every breaking change and list none. The point of the
  report is the list, so the job runs the default check and ignores its exit
  code.
- buf has no config in the repo: `buf breaking` on a directory uses buf's
  defaults (rule set `FILE`).
- Local runs: downloading and running a buf binary was not allowed here, so buf
  is tested in CI only. `cargo-semver-checks` was installed locally to preview
  the report.

## Verification

| Check | Command | Result |
|---|---|---|
| CI on a60e909 (recorded afterwards) | PR #30 checks | 18/18 pass; `proto wire compatible with v0.12.2` passes on the current proto; `API changes since v0.12.2 (report)` passes and uploads `semver-report` with the D1/D2 breaking changes per crate |
| buf negative test | 65cbe73 changes `EventEnvelope.position` from field 6 to 9; CI | `proto wire compatible with v0.12.2` **fails** (exit 100): `ingest.proto:24:1: Previously present field "6" with name "position" on message "EventEnvelope" was deleted.` Expected failure |
| Revert | this commit restores field 6; CI | see the PR checks on this commit |
