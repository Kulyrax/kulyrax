---
status: proposed
---

# Move the YAML parser to serde-saphyr 3.x before 1.0.0-rc.1

## Objective

serde-saphyr supports only its latest release and does not backport fixes
(its SECURITY.md, read 2026-11-25). Kulyrax ships on 2.x. Once 3.0 is final, 2.x
gets no fixes. So move `kulyrax_core::yaml` to 3.x when 3.0 is final, and
before 1.0.0-rc.1, gated by the golden test.

Decision: [2026-11-25-yaml-parser](../decisions/2026-11-25-yaml-parser.md),
owner's change 4.

## Acceptance criteria

- [ ] serde-saphyr 3.0.0 is released (not a release candidate). 3.0.0-rc.1 came
  out on 2026-11-18.
- [ ] `crates/core/src/yaml.rs` moves to 3.x with the same strict options. It
  stays the only file that names the crate.
- [ ] Gated by the golden test:
  - The refusal corpus passes unchanged.
  - The acceptance comparison against serde_yaml 0.9 is re-run. It is removed
    in step 3, so the move brings it back from git history for the run, with
    serde_yaml as a temporary dev-dependency. Then it is removed again.
  - Every difference goes in the release notes.
- [ ] Four gates green and every Docker suite green, before 1.0.0-rc.1.

## Risks and open questions

- If 3.0 is not final by rc.1, rc.1 ships on 2.x. Record that in the rc.1
  notes, and keep this task open for 1.0.0.
