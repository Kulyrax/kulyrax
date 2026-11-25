---
status: completed
---

# Run cargo audit daily, and open an issue when it fails

## Objective

The RustSec database changes without any change to this repository. A new
advisory against a locked dependency should surface within a day, not at the
next pull request. Owner, 2026-11-25: the audit runs daily on a schedule, opens
or updates an issue on failure, and is not a required check. Then (runbook A4)
the alert path is tested end to end: one labelled issue, updated rather than
duplicated, closed when the audit passes again.

## Acceptance criteria

- [x] `.github/workflows/audit.yml`: daily schedule (06:17 UTC) and
  `workflow_dispatch`; the same loop and flags as the `cargo audit` job in
  `ci.yml`; on failure the run itself fails too (#16).
- [x] After merge: a manual dispatch on `main` passes.
- [x] `force_fail` dispatch input (boolean, default false): the real audit runs,
  then the step fails (#19).
- [x] A failing run opens one issue (title, report, run link, label
  `cargo-audit`) or comments on the open one, never a duplicate; a passing run
  closes it with a comment linking the passing run (#19).
- [x] Tested on the PR branch: (a) `force_fail=true` opens the issue; (b) again
  updates the same issue; (c) `force_fail=false` passes and closes it.

## Verification

| Check | Command | Result |
|---|---|---|
| Exit status of the audit step | the step's shell logic, alone, with an always-passing and an always-failing body | `step exit=0` and `step exit=1`. First draft: the loop ran in the pipe's subshell, so its status was lost and the step always passed. Fixed with an explicit subshell `exit $status` and `set -o pipefail` |
| Manual run on main (#16) | `gh workflow run audit.yml --ref main`; run [45570846476](https://github.com/Kulyrax/kulyrax/actions/runs/45570846476) | success: audit step success, the two failure steps skipped |
| Where to test #19 | the branch was on `Panchauly/kulyrax` only; an upstream dispatch cannot see fork branches, and the fork has Issues off and no audit workflow on its `main` | the owner chose to push `ci/audit-alert-path` to `Kulyrax/kulyrax` for the three runs and delete it afterwards (done) |
| (a) `force_fail=true` | `gh workflow run audit.yml --ref ci/audit-alert-path -f force_fail=true`; run [45600489369](https://github.com/Kulyrax/kulyrax/actions/runs/45600489369) | run failed as intended; the issue step created label `cargo-audit` and opened issue #20, "cargo audit: scheduled check failing", with the report and the run link |
| (b) `force_fail=true` again | run [45600604362](https://github.com/Kulyrax/kulyrax/actions/runs/45600604362) | run failed as intended; #20 got 1 comment; no second issue (`gh issue list --label cargo-audit --state all`: #20 only) |
| (c) `force_fail=false` | run [45600691848](https://github.com/Kulyrax/kulyrax/actions/runs/45600691848) | success; the issue step skipped; #20 closed with the comment "The scheduled `cargo audit` passes again on `ci/audit-alert-path`: <run 45600691848>" |
| Cleanup | `git push upstream --delete ci/audit-alert-path` | the upstream branch is gone; #19's head stays on the fork |
