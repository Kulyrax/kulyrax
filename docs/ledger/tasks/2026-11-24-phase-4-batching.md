---
status: in-progress
---

# Phase 4: batch the remaining networked backends

## Objective

Give MySQL, Redis, MongoDB and Neo4j one commit per batch, and Elasticsearch a
`_bulk` write per batch, as SQLite and Postgres have had since v0.10. Owner's
order (2026-11-24): driver split → Phase 4 → faster rebuilds → multi-record
graph events → Postgres-outbox source. No benchmarking for now.

## Acceptance criteria

- [x] MySQL: one transaction per batch, guard lanes loaded up front; batch
  outcomes and final state equal one event at a time; rollback leaves nothing.
- [ ] Elasticsearch: one `_bulk` request per batch, same equivalence.
- [ ] Redis: one round trip (pipeline / `MULTI`) per batch, same equivalence.
- [ ] MongoDB: one transaction / bulk write per batch, same equivalence.
- [ ] Neo4j: one transaction per batch, same equivalence.
- [ ] Each backend's test suite passes against a real server (Docker), and CI's
  service jobs cover the new tests.
- [ ] Gates green before every push.

## Constraints and assumptions

- `decide()`, `dispatch.rs`, `batch.rs` and the dependency graph unchanged; no
  storage-kind special case outside the backend crates.
- A backend that cannot batch keeps the default trait methods, which stay
  correct: the batch is re-delivered after a rollback and guards skip what
  already landed.
- Local verification uses Docker Desktop containers; CI runs the same suites
  against service containers.

## Decisions

- [2026-11-24-mysql-binary-guard-keys](../decisions/2026-11-24-mysql-binary-guard-keys.md):
  binary-collated guard keys, so a batch's exact-string guard cache matches
  MySQL's lookups.

## Verification

| Check | Command | Result |
|---|---|---|
| MySQL suite on MySQL 8 (Docker), fresh database each run | `KULYRAX_MYSQL_URL=mysql://root@localhost:3306/kulyrax_ci cargo test -p kulyrax-adapter-mysql`, ×5 | 18 tests in 5 binaries, all passed, 5 of 5 runs |
| First runs | same | `mysql_batch_redelivery_is_skipped` failed with ERROR 1213, 5 of 5. Two causes. (1) The batch's guard read matched `entity_key IN (…)`, a prefix of the guard key; fixed to `(entity_key, facet) IN (…)`, a unique probe, like the per-event read. Still 1213. (2) The InnoDB status had no deadlock: it was a metadata-lock deadlock. The general log showed `mysql_failed_guard_write_leaves_neither_row` running `ALTER TABLE conduit_projection_state ADD/DROP CHECK` alongside the batch tests. Moved to its own process (`mysql_guard_ddl.rs`), as `mysql_binlog.rs` already is |
| The equivalence test catches case-variant keys | Mutation: old case-insensitive guard keys | `mysql_batch_matches_one_event_at_a_time` fails: the batch `Created` `k`, one-at-a-time `Skipped(StaleSequence)`; reverted |
| Read plan | `EXPLAIN … WHERE target_table = ? AND (entity_key, facet) IN ((…),(…)) FOR UPDATE` | `type: range`, `key: PRIMARY`, `rows: 3` for 3 lanes |

## Risks and open questions

- The MySQL suite assumes a fresh database: test table names come from a
  per-process counter, so a rerun against a reused database can collide with an
  earlier run's guard rows (seen locally before resetting it). CI always starts
  fresh. Pre-existing, not changed here.

## Outcome

In progress.
