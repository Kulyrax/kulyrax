---
status: completed
---

# `kulyrax-core` without network drivers

## Objective

The owner's first item for 2026-11-24 ("driver split"): `kulyrax-core` should not
pull in the MySQL, Redis, MongoDB or Neo4j drivers, so an application embedding it
for SQLite or the file stores builds no database client it doesn't use. The roadmap
still listed this as in progress ("embedding `kulyrax-core` still pulls in
MySQL/Redis/MongoDB/Neo4j transitively").

## Acceptance criteria

- [x] `kulyrax-core` has no networked database driver and no tokio among its
  dependencies, direct or transitive.
- [x] Every networked backend is reachable through its own crate and through
  `kulyrax-backends`, one Cargo feature each.
- [x] The roadmap no longer lists the item as open.
- [x] v0.11.0's release notes tell anyone upgrading from 0.8 or earlier what to
  depend on (owner's choice, 2026-11-24: a note labelled as a v0.9 change, since
  0.10 → 0.11 changes nothing here).

## Constraints and assumptions

- No behaviour change: the work was already done; this task verifies it and
  corrects the docs.

## Verification

| Check | Command / source | Result |
|---|---|---|
| Core's direct dependencies | `cargo tree -p kulyrax-core -e normal --depth 1` | `crc32fast v1.5.2`, `once_cell v1.21.3`, `rusqlite v0.32.1`, `serde v1.0.228`, `serde_json v1.0.149`, `serde_yaml v0.9.34+deprecated` |
| No driver or tokio anywhere below core | `cargo tree -p kulyrax-core -e normal --prefix none \| sort -u \| grep -c -i -E '^(mysql\|redis\|mongodb\|neo4rs\|tokio\|postgres\|reqwest) '` | `0` |
| When the split happened | `git show a0dccc0:crates/core/Cargo.toml` (v0.9.0), `git show cfa4953:crates/core/Cargo.toml` (v0.10.0); `RELEASES/v0.9.0.md` | both list serde, serde_json, serde_yaml, once_cell, rusqlite only. v0.9.0's notes: "`kulyrax-core` no longer depends on any database driver or on tokio". **The split shipped in v0.9.0**; the roadmap entry was stale |
| Backends as crates | `crates/adapters/{postgres,mysql,redis,mongodb,neo4j,elasticsearch}`, `crates/backends/Cargo.toml` features `postgres`, `elasticsearch`, `mysql`, `redis`, `mongodb`, `neo4j`, `all` (default) | present |
| Docs | `ROADMAP.md` (commit `4884c76`), `RELEASES/v0.11.0.md` "Coming from 0.8 or earlier" with `Cargo.toml` before / after | updated |

## Outcome

Completed. No code change was needed: the split shipped in v0.9.0. This task
removed the stale roadmap entry and added the upgrade note for 0.8 users to the
v0.11.0 release notes.
