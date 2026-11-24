---
status: proposed
---

# Rename conduit → kulyrax (1.0 plan, step 2)

## Objective

Every persisted, user-visible and code name `conduit` becomes `kulyrax`. Existing
stores migrate in place at start-up, once, idempotently and with every guard
lane kept. A migration killed halfway completes on the next start. Design:
[2026-11-25-rename-conduit](../decisions/2026-11-25-rename-conduit.md).

## Acceptance criteria

- [ ] Decision record approved by the owner.
- [ ] SQLite, Postgres, MySQL, MongoDB, Neo4j and the file stores migrate as the
  decision says; when both names exist, start-up is refused and both are named.
- [ ] Layout marker version 2 under `.kulyrax/layout/<kind>`.
- [ ] Per backend: a 0.12.1-shaped store opens with every lane intact; an older
  redelivery is `Skipped(StaleSequence)`; a second start is a no-op.
- [ ] Interrupted migration recovers (crash points: file stores, Neo4j batches,
  SQL and MongoDB around the rename).
- [ ] Code, docs, comments and test names renamed. `grep -ri conduit` hits only
  migration code and tests, the new release notes, `docs/ledger/` and older
  `RELEASES/`.
- [ ] Release notes: an upgrade section at the top, with the reverse rename per
  backend.
- [ ] `decide()`, `dispatch.rs` and the dependency graph unchanged; no panic in
  core; four gates green; every Docker suite green.

## Constraints and assumptions

- Builds on 0.12.1 (the layout marker, `.conduit/kv/`), which is private until
  GHSA-6chh-gx4c-448g is published. Until then this branch (`step2-rename`) stays
  local: nothing is pushed.

## Verification

| Check | Command | Result |
|---|---|---|
| Inventory of persisted names | `grep -rnoi conduit crates --include=*.rs` (library code); `docs/stability.md` §5–6 | as listed in the decision record's Evidence |

## Risks and open questions

- Tombstones: recommended. They need the owner's yes.
- Grep scope: excluding `docs/ledger/` and old `RELEASES/` needs the owner's yes.
- `.kulyrax/` shared with `control/` and `state/` when a store root is the project
  folder.
- SQLite autoindex naming after `RENAME`: unknown until tested.
