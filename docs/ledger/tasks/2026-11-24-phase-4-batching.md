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
- [x] Elasticsearch: one `_mget` and one `_bulk` request per batch, same
  equivalence; a concurrent writer's change fails the commit (409) instead of
  being overwritten.
- [x] Redis: guards `WATCH`ed and loaded with `MGET` at `begin_batch`, one
  `MULTI`/`EXEC` per batch, same equivalence; a concurrent writer aborts the
  whole batch, nothing of it lands.
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
| Elasticsearch suite on 8.15.0 (Docker, single node, 512 MB heap) | `KULYRAX_ELASTICSEARCH_URL=http://localhost:9200 cargo test -p kulyrax-adapter-elasticsearch`, ×5 | 8 passed, 5 of 5 runs: batch = one at a time (31 events, one `_mget`, one `_bulk`, nothing visible before commit), rollback sends nothing (and `_mget` reads a missing index as empty), a concurrent write fails the commit with `version_conflict` and the per-event fallback applies on top |
| First runs | same | 4 of 8 failed, and on the **unchanged** code 3 of 5: `503` on GET and request timeouts while the tests' indexes were auto-created all at once. Fixed in the tests: each creates its index first, waiting for its shard. Also a wrong assertion of mine (`read` calls): `exec::project` times every guard read, so the `_bulk` is timed as `commit` |
| CI | `.github/workflows/ci.yml` | new job `test (elasticsearch)`; there was none, so this suite never ran in CI |
| Redis suite on Redis 7 (Docker), same server each run | `KULYRAX_REDIS_URL=redis://localhost:6379 cargo test -p kulyrax-adapter-redis`, ×3 | 11 passed (3 unit, 8 integration), 3 of 3 runs: batch = one at a time (31 events, one `MULTI`/`EXEC`, nothing visible before commit), rollback leaves nothing, a concurrent write aborts the batch and the fallback applies on top |
| First reruns | same, against the same server | 4–6 of 8 failed, existing tests included: namespaces `t0`, `t1`, … repeat per process, so a rerun met the last run's guards. Namespaces now carry the process id, as the Elasticsearch tests' indexes do |

## Risks and open questions

- Elasticsearch, pre-existing: a GET on an index whose shards are still
  starting (just auto-created by another writer) returns 503, which the
  adapter reports as a failed event, not a retryable conflict. Retry and
  dead-letter handle it; not changed here.

- The MySQL suite assumes a fresh database: test table names come from a
  per-process counter, so a rerun against a reused database can collide with an
  earlier run's guard rows (seen locally before resetting it). CI always starts
  fresh. Pre-existing, not changed here.

## Outcome

In progress.
