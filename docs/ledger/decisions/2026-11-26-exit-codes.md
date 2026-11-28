---
status: accepted
---

# Exit codes: what each failure maps to, and the typed signals behind them (step 4, D7)

Task: [2026-11-26-cli-exit-codes](../tasks/2026-11-26-cli-exit-codes.md)

## Context

`stability.md` §9.5 decided the categories:
- `0` ok;
- `1` internal;
- `2` usage / config / mapping / routing invalid;
- `3` event failures (failed or dead-lettered in `--once` / `replay` /
  `rebuild`);
- `4` refused by a safety check (a lock held, a rebuild refused, a newer
  on-disk format).

They freeze at 1.0. Two of them had no typed signal to map from:
- a held source lock was `SourceError::Unavailable(text)`;
- a refused rebuild preflight or swap reached the CLI only as
  `AdapterError::WriteFailed` text, through the `ShadowRebuild` trait.

## Options considered

The owner was asked on 2026-11-26, with the full mapping. The other options
moved `init` to 2, or exited 0 for a `--once` run whose failures were all
dead-lettered.

## Decision

The owner approved the mapping as proposed:

| Code | Cases |
|---|---|
| 0 | ok |
| 1 | I/O, unexpected errors, an adapter prepare failure |
| 2 | clap usage; config, mapping, routing or validation invalid; a bad `--event` / `--events` file; an adapter or source factory refusing its config; a rebuild of an unknown, non-SQL or non-rebuildable adapter; a `reload` refused because the new config is invalid |
| 3 | events failed or dead-lettered in `run --event` / `--once`, `replay`, `rebuild` (not swapped), `dry-run` with a failed adapter |
| 4 | a source lock held; a rebuild refused before it starts or at the swap; `init` would overwrite; a newer on-disk format (step 5) |

The signals behind it:
1. **`SourceError::Locked`**, from `lock_source`. Its `Display` is unchanged.
2. **`SourceError::InvalidConfig`**: an unknown source `type:`, or an outbox
   `config:` block or `state_dir` that is missing or wrong. Before, these were
   `Unavailable` and would have exited 1, while the same mistake on an adapter
   exits 2.
3. **`rebuild::RebuildError { Invalid, Refused, EventsFailed, Failed }`**, held
   by `PipelineError::Rebuild`. `ShadowRebuild` returns it.
   `SqlError::RebuildRefused` marks the exec layer's two refusals (blockers, a
   changed fingerprint), and `From<SqlError>` keeps them `Refused`.
4. **The policy lives in the CLI** (`mod exit` in `main.rs`): a match over the
   public error types, which is rendering, not an engine call. A kind it does
   not know (the enums are `#[non_exhaustive]`) is 1.

Filled in here; these were not in the asked mapping:
- **`reload` that no process answered: 1** (it was 2). 2 now means invalid
  input, and nothing in the request was invalid.
- **Continuous `run` and `ingest`: 3 only for failures not dead-lettered**, or a
  retry-exhausted halt. A long-running process that dead-letters is working as
  designed. The `--once` rule ("dead-lettered is 3") is the owner's.
- **A held file-store lock stays 1.** It surfaces as an adapter prepare failure
  (`AdapterPrepare`), and the approved mapping named the source lock only.
  Reported to the owner as a possible follow-up. **Resolved (E2b, 2026-11-28):**
  the owner, on the D8 report: "A held file-store lock exits 4, per D7; a CLI
  test proving it." It is now 4: `AdapterError::Locked` marks
  `PrepareFailure::refused` (the typed refusal step 5 added for formats).
  Test: `exit_codes.rs`, `exit_4_when_a_safety_check_refuses`.

## Trade-offs accepted

- **`ShadowRebuild`'s error type changed**, from `AdapterError` to
  `RebuildError`. Only SQLite and Postgres implement it.
- **The CLI's match grows with the error types.** A new variant falls back to
  1 until it is mapped.

## Revisit if

- ~~A held file-store lock should be 4: give `PrepareFailure` a typed refusal.~~
  Done in E2b (above).
- ~~Step 5 adds the newer-format refusal: map it to 4 here, with a CLI test.~~
  Done in step 5 (decision `2026-11-28-format-versions`, #37).
