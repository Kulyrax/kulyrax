# Required grants

What the user Kulyrax connects as must be allowed to do, per backend, to start
and to project. When Kulyrax cannot create its format marker, it refuses to
start (exit code `4`) and names the store and the grant it needs; this page
lists the rest.

Every store holds one small Kulyrax object besides its guards: the **format
marker**, which records the version of Kulyrax's own data there. It is
created on the first start, if absent. See [stability §5](stability.md#5-persisted-formats).

`kulyrax rebuild` needs more than this page lists: it creates, swaps and drops
tables.

## File stores (document, key-value, graph)

- Read, write, create, rename and delete files and directories under the
  store's `root`.

## SQLite

- Write access to the database file **and its directory**: SQLite keeps its
  write-ahead log and shared-memory files beside the database.

## Postgres

In the schema the connection uses (the first one on its `search_path` that
exists):

- `USAGE` and `CREATE` on the schema, **at every start**. Kulyrax runs
  `CREATE TABLE IF NOT EXISTS` for its guard table `kulyrax_projection_state`
  and its marker table `kulyrax_format`, and Postgres checks the `CREATE`
  right even when the table already exists. So the tables cannot be
  pre-created by an administrator for a user without it.
- `SELECT`, `INSERT`, `UPDATE`, `DELETE` on the tables your mappings project
  into. Kulyrax owns the two tables it creates.

The Postgres outbox source (`type: postgres_outbox`) only reads: `SELECT` on
the outbox table.

## MySQL

In the database named in the URL:

- `CREATE` on the database, **at every start**, for the same reason as
  Postgres: `CREATE TABLE IF NOT EXISTS` on `kulyrax_projection_state` and
  `kulyrax_format` checks it even when the table exists.
- `SELECT`, `INSERT`, `UPDATE`, `DELETE` on the tables your mappings project
  into, and on those two tables.
- `ALTER` on `kulyrax_projection_state`, once, if an older Kulyrax created
  its key columns with the server's default collation instead of a binary one.

## MongoDB

In the database named in `database:`:

- The built-in `readWrite` role on that database. It covers what Kulyrax
  does: `find`, `insert`, `update`, `remove`, `createCollection`,
  `createIndex` on the mapped collections, the guard collection
  `kulyrax_projection_state` and the marker collection `kulyrax_format`, and
  the one-time rename of a 0.12 guard collection.
- Transactions, so a replica set or a sharded cluster (not a standalone
  server).

## Redis

With Redis ACLs, the user needs these keys and commands:

- Keys: `~<namespace>:*` for each mapping's namespace, and `~__kulyrax:*` (the
  guards `__kulyrax:guard:…` and the marker `__kulyrax:format`).
- Commands: `+get +set +del +mget +watch +unwatch +multi +exec` (projecting),
  `+hsetnx +hget` (the marker).
- `+config` is optional. Kulyrax reads `CONFIG GET maxmemory-policy` once per
  adapter start to check the eviction policy (below). When it cannot, it says
  so in one line and carries on.

**Eviction.** Kulyrax's Redis store requires `maxmemory-policy noeviction`, or
a `volatile-*` policy with no TTL on any Kulyrax key. Kulyrax sets no TTLs.
An evicted guard or format marker silently loses data: an evicted guard lets
an old event overwrite newer data, or a deleted entity come back. When the
policy is an `allkeys-*` one, Kulyrax prints a warning naming it at start; it
does not refuse to start.

## Elasticsearch

With security enabled, index privileges:

- On the indexes your mappings project into: `create_index` (Kulyrax lets
  the first write create an index), `read`, `write`.
- On the index `kulyrax_format`, one per cluster: `create_index`, `write`,
  `read`.

## Neo4j

Neo4j Community has no role-based access control: every user can do
everything. With Neo4j Enterprise, the user needs:

- To create constraints: `kulyrax_guard_key` on `:__KulyraxGuard` and
  `kulyrax_format_component` on `:__KulyraxFormat` (`IF NOT EXISTS`, at every
  start).
- To read and write nodes and relationships with the labels and relationship
  types your mappings use, and the labels `__KulyraxGuard` and
  `__KulyraxFormat`.
