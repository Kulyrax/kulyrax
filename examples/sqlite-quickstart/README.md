# sqlite-quickstart

The 60-second Kulyrax example: three events in, a projected SQLite table out.

```sh
bash run.sh
```

`run.sh` does exactly what a real deployment does:

1. Creates an empty `users` table in `app.db` (the target read model — Kulyrax never
   creates *your* tables, only its own `conduit_projection_state` guard table).
2. Runs `kulyrax run --config config.yaml --mappings mappings --once`, which drains the
   `events/events.ndjson` directory source once and exits (see `config.yaml`).
3. Dumps `app.db`'s `users` table and diffs it against [`expected/users.txt`](expected/users.txt)
   — this file doubles as a golden test, and CI runs it on every push.

## What happens

`events/events.ndjson` has three events:

| # | event | entity | sequence |
|---|---|---|---|
| 1 | `UserRegistered` | `u1` | 1 |
| 2 | `UserRegistered` | `u2` | 1 |
| 3 | `UserEmailChanged` | `u1` | 2 |

[`mappings/sql/user_registered.yaml`](mappings/sql/user_registered.yaml) inserts a row.
[`mappings/sql/user_email_changed.yaml`](mappings/sql/user_email_changed.yaml) uses
`on_existing: replace` (sequence-gated — a redelivery or a stale event is a clean skip, not
a duplicate or a corruption) to overwrite it. Notice that mapping still lists **every**
column, not just `email` — a full replace always writes the whole row. That's exactly the
limitation [facets](../../docs/concepts.md#facets) exist to lift.

Run it twice: the second run is a no-op (every event is `Skipped(AlreadyProjected)` /
`Skipped(StaleSequence)`) because the source's checkpoint (`.conduit/state/`) remembers
what it already delivered.

## Next

- [`docs/concepts.md`](../../docs/concepts.md) — the mental model: `sequence` vs
  `position`, the guard, facets, `decide()`.
- [`docs/mapping-reference.md`](../../docs/mapping-reference.md) — every mapping field.
- [`examples/postgres/`](../postgres/) — the same shape against Postgres.
- [`examples/embedded/`](../embedded/) — calling `kulyrax-core` as a library, no CLI.
- [`examples/grpc-producer/`](../grpc-producer/) — streaming these same events over gRPC
  instead of a directory.
