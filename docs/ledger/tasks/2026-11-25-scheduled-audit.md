---
status: in-progress
---

# Run cargo audit daily, and open an issue when it fails

## Objective

The RustSec database changes without any change to this repository. A new
advisory against a locked dependency should surface within a day, not at the
next pull request. Owner, 2026-11-25: the audit runs daily on a schedule, opens
or updates an issue on failure, and is not a required check.

## Acceptance criteria

- [x] `.github/workflows/audit.yml`: daily schedule (06:17 UTC) and
  `workflow_dispatch`; the same loop and flags as the `cargo audit` job in
  `ci.yml`; on failure, a comment on the open issue titled "cargo audit:
  scheduled check failing" or a new one, with the report and the run link; the
  run itself fails too.
- [ ] After merge: a manual dispatch on `main` passes.

## Verification

| Check | Command | Result |
|---|---|---|
| Exit status of the audit step | the step's shell logic, alone, with an always-passing and an always-failing body | `step exit=0` and `step exit=1`. First draft: the loop ran in the pipe's subshell, so its status was lost and the step always passed. Fixed with an explicit subshell `exit $status` and `set -o pipefail` |

Not run: the issue-creation path needs a failing audit on `main`; it runs the first
time one occurs.
