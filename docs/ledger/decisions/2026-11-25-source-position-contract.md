---
status: proposed
---

# The source-position contract: opaque, ordered by the source that made it

Task: [2026-11-25-source-position-contract](../tasks/2026-11-25-source-position-contract.md)

## Context

Review finding F-05. `SourcePosition` is a `String` newtype that derives `Ord`.
Its doc comment says positions compare as text, "within a single source".
Three places rely on that:

- **The run loop.** It commits each batch at the text-wise **maximum** of the
  batch's positions (`source/runner.rs`), and reports the minimum as
  `position_from`.
- **The directory source.** It ignores a commit that is `<=` the last one as
  text ("never moves backward").
- **The stdin source.** The same check.

Text order is not delivery order for numeric offsets (`"9" > "10"`) or
structured ones. Built-in sources work around this by zero-padding:
- stdin: `<line:012>`;
- directory: `<file>#<index:012>`;
- outbox: `<after:020>.<late:010>`.

The padding is not enough for the directory source. A split file's positions
are `<file>#<index>`, and `#` is 0x23. A later file whose name continues the
split file's name with a space, `!` or `"` is listed after the split file but
sorts before its positions as text:
- its commit is ignored as "backward";
- it is delivered again on every poll;
- a `--once` run never ends.

gRPC positions are the producer's own. Since R2 (#23), a stream's ack carries
its own last position in send order, so the ack no longer compares positions.
The run loop still computes the text-wise maximum for every gRPC batch. That
value becomes the source's in-memory committed position and the batch summary's
`position_to`.

## Options considered

- **A. Opaque positions; the core never compares them (recommended).**
  - The run loop commits each batch at its **last event in delivery order**.
    `position_from` and `position_to` are the batch's first and last positions.
  - Only the source that made a position orders it, with its own rule, inside
    its own code. Directory compares `(file, index)`; stdin compares the line
    number; the outbox already looks commits up exactly.
  - No trait method and no protocol field.
  - gRPC positions stay the producer's token. Kulyrax never orders them and
    echoes back each stream's last one (R2).
- **B. A typed position:** an enum such as `Offset(u64)`, `Text(String)`,
  `Composite(..)`, with a matching proto field so gRPC producers declare
  theirs.
  - The core could then compare safely.
  - Cost: new API and a new wire field.
  - Once the run loop uses delivery order, the core has nothing left to
    compare.
- **C. A comparator per source:** `EventSource::cmp_positions`, used by the
  run loop for the maximum, and a gRPC "position kind" (text or numeric).
  - It keeps a comparison the core does not need.
  - It adds trait surface and a protocol setting that producers can get wrong.
- **D. Keep text order and fix only the separator:** split positions use a
  byte below every legal file-name character.
  - No such byte exists: Linux allows 0x01–0x1F in names.
  - It also changes a frozen format and keeps the fragile rule.

## Evidence

Run on main (`7ff9391`), 2026-11-25.

| Check | Result |
|---|---|
| `crates/core/tests/source_position_order.rs`: `a.ndjson` split at `--max-batch 2`, then `a.ndjson b.ndjson` (space) or `a.ndjson!b.ndjson` | Both fail. The second file is delivered 4 times in 6 polls, and the committed position stays at `a.ndjson#000000000002`. The control, `a.ndjson-b.ndjson`, passes |
| The binary: quickstart plus `events/events.ndjson 2.ndjson`, `kulyrax run --once --max-batch 2` under `timeout 30` | Killed by the timeout (exit 124), with no report printed. The checkpoint is stuck at `events.ndjson#000000000002` |
| `grep` for comparisons of positions in `crates/` (outside tests) | Four places: `runner.rs` (`max`, `min`), `directory.rs` and `stdin.rs` (`<=` on commit). The outbox commit looks the position up in the last poll's set (no ordering) |
| Delivery order versus maximum | For every built-in source, the last event in delivery order is the maximum today, except in the directory case above. So A changes no correct result |

## Decision (proposed)

A. The contract, for `docs/producer-contract.md` (a new §, "Positions") and
`docs/stability.md`:

1. **What a position is.** An opaque resume token made by one source, for that
   source. The core stores it, echoes it, and names DLQ files after it. It
   never parses or compares it.
2. **Order.** Only the source that made a position may order it, by its own
   rule. Positions of different sources (or of different gRPC streams) are
   never compared. `SourcePosition`'s `Ord` is not part of the contract.
3. **Who constructs them.** Sources do, from what they read. A gRPC position is
   the producer's `EventEnvelope.position`, taken as is.
4. **What `commit(p)` means.** `p` is the position of the batch's last event in
   delivery order. Committing it means every event the source delivered up to
   and including that event is resolved: projected, skipped, or dead-lettered.
   A source resumes after it and must never deliver those events again, except
   after a crash before the commit.
5. **What gRPC producers send.** Any token they can resume from. Kulyrax does
   not order or compare it, and acks each stream's last committed one (R2).
   Several events may share a position. Kulyrax cannot ack an empty position
   usefully, since there is nothing to resume from: see the open question.

**Enforcement:**
- **Run loop:** first and last position in delivery order; no `max`/`min`.
- **Directory:** commit and resume compare `(file, index)` parsed from the
  position, not the text. The format `<file>#<index:012>` is unchanged, so
  0.12.1 checkpoints read the same and B1 needs no new source version.
- **Stdin:** compares line numbers. A checkpoint it cannot parse is an error,
  not line 0 as today.
- **Tests:** each built-in source (directory, stdin, outbox, gRPC) gets a test
  that it commits at every batch's last position, moves forward, and resumes
  after it. For directory this includes the space, `!` and `"` names above. The
  `--once` case also runs through the CLI.
- **Docs:** `SourcePosition`'s doc comment is rewritten to this contract.
- **Release:** the 0.12.2 notes record the fix.

**Unchanged:** `decide()`, `dispatch.rs`, the dependency graph, the proto wire,
and every persisted format.

## For the owner

1. **Approve A?**
2. **Empty gRPC positions.** Today they are accepted, and `pb::EventEnvelope::from(&Event)`
   sends one. Recommended: refuse an empty position with `InvalidArgument`
   from 1.0.0-rc.1, announced in the 0.12.2 notes. The alternative is to
   accept it and document that such a producer cannot resume. Either way,
   0.12.2 keeps accepting it.
3. **`Ord` on `SourcePosition`.** `stability.md` freezes the type as it is. To
   make rule 2 a compile-time guarantee, remove `PartialOrd`/`Ord` in step 4.
   This is a breaking API change, allowed before 1.0 but not in 0.12.2. Or keep
   `Ord` and state that it is not delivery order. Recommended: remove it in
   step 4.

## Trade-offs accepted

- A source with a broken order can no longer be caught by the run loop's
  maximum. It could not be before either: the maximum hid the problem rather
  than detecting it. The per-source tests are the check.
- An out-of-tree `EventSource` that hands out a batch whose last event is not
  its furthest position must change. No such case is known; the rule is
  documented.

## Revisit if

- A source needs the core to merge or compare positions, for example
  partitioned sources committed together. Then option B, typed positions, as
  an additive change.
