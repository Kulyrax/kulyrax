# postgres

The facet showcase, against a real Postgres backend.

```sh
export DATABASE_URL=postgres://user:pass@localhost:5432/dbname   # or accept the default
bash run.sh
```

This mirrors the scenarios in `crates/kulyrax-core/tests/pg_backend.rs`, and the shared
`exec::project` write path, which behaves identically on SQLite and Postgres by
construction. CI's `test-postgres` job (`.github/workflows/ci.yml`) runs this against a
real Postgres service container on every push.

## What happens

Three events project one `users` row split into two independent facets:

| # | event | facet | column | sequence |
|---|---|---|---|---|
| 1 | `UserRegistered` | *(default)* | `name` | 1 |
| 2 | `StatsRecomputed` | `stats` | `followers` | 6 |
| 3 | `ProfileUpdated` | `profile` | `bio` | 5 |

`profile` and `stats` are disjoint sequence lanes on the same row — see
[`mappings/sql/profile_updated.yaml`](mappings/sql/profile_updated.yaml) and
[`mappings/sql/stats_recomputed.yaml`](mappings/sql/stats_recomputed.yaml). Swap the
last two lines of [`events/events.ndjson`](events/events.ndjson) and the result is
identical — that's the guarantee under test: a facet update never depends on another
facet having landed first, or last.

Postgres is also the first Kulyrax adapter with real multi-writer safety
(`SELECT ... FOR UPDATE` on the guard row) — two `kulyrax` processes racing the same
row converge to one write and one clean skip, never a lost update or a raw constraint
error.

## Next

- [`docs/concepts.md#facets`](../../docs/concepts.md#facets) — the mental model and a
  second worked example.
- [`examples/sqlite-quickstart/`](../sqlite-quickstart/) — the simpler whole-row
  `on_existing: replace` case this contrasts with.
