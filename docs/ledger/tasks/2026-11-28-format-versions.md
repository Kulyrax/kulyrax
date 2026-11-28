---
status: blocked
---

# A format version in every persisted format (step 5, E1 + E2)

## Objective

Runbook E1: a format version in every persisted format (checkpoints, batch log
header, sidecars and guards, outbox state, layout marker, DLQ naming if
relevant). A newer version is refused with exit 4 and a clear error. An older
one is migrated or refused, never misread. The config keeps `version: 1`
required.

E2: tests that a future version is refused, that each format round-trips, and
that each migrates from 0.12.1.

Blocked on the owner's choice of design: decision
[2026-11-28-format-versions](../decisions/2026-11-28-format-versions.md)
(proposed).

## Acceptance criteria

- [x] Inventory of every persisted format, read in source (decision record,
  Context).
- [ ] The owner approves the design (decision record).
- [ ] Each format in the inventory records its version as decided.
- [ ] A newer version is refused before anything is read, replayed or written.
  The error names the file or store and both versions, and the CLI exits 4
  (a CLI test).
- [ ] An unversioned (0.12.x) file or store is read as format 1 and stamped.
- [ ] The newest batch log with a newer magic is refused, not deleted.
- [ ] The layout marker's content is read; new stores and migrated stores write
  the same version.
- [ ] E2 tests: future version refused, round-trip, and migration from 0.12.1
  (the `upgrade from 0.12.1` CI job), for each format and backend.
- [ ] `stability.md`: the formats, their versions, and what is out of scope.
  rc.1 notes: the change.
- [ ] Gates: fmt, clippy `-D warnings`, workspace tests, examples, every Docker
  backend suite.
- [ ] PR merged with the 12 required checks green.

## Constraints and assumptions

- `decide()`, `dispatch.rs` and the dependency graph stay unchanged. Version
  checks live in each backend's `prepare` and in the source's checkpoint read.
- Zero panic in kulyrax-core; thin CLI (the exit-4 mapping is a match on the
  typed error).
- Assumed, to be verified by the upgrade job: no persisted shape changed
  between 0.12.1 and main except the rename step 2 already migrates. R4
  recorded none for Phase R; Phase D changed APIs, not serde shapes.

## Verification

| Check | Command / source | Result |
|---|---|---|
| Disk before work | `df -h /c` | 203 GB free |
| Branch base | `git log formats/e1-format-versions` | `e99a19c` = `upstream/main` (#36 merge), no commits yet |
| Layout marker content | `file_store.rs:903` vs `legacy.rs:191`, `LAYOUT_VERSION` at `file_store.rs:35` | new stores write `1\n`; migrated stores write `2\n`; content never read (only `marker.exists()`, `file_store.rs:868`) |
| 0.12.1 marker | `kx-0121` `crates/core/src/adapter/file_store.rs:920` | `1\n` under `.conduit/` |
| Batch log with unknown magic | `file_store.rs:159-163`, `752-760` | `decode_log` returns `None`; the newest such log is deleted as torn |
| Replay before layout check | `file_store.rs:938-947` | `recover` runs before `check_layout` |
| Exit 4 reserved | decision `2026-11-26-exit-codes`, line 77 | "Step 5 adds the newer-format refusal: map it to 4 here, with a CLI test." |

## Risks and open questions

- The marker objects in networked backends (the decision lists them) are the
  owner's call: they add one create right per backend.
- Estimate: about 2 days.
  - Core: typed error, checkpoint, outbox state, file stores, exit 4 (~0.75 day).
  - Six networked backends plus SQLite, with tests (~1 day).
  - Upgrade-job cases and docs (~0.25 day).

## Outcome

Pending: blocked on the design approval.
