# Writing a Backend

Kulyrax projects each event into a storage model exactly once, with the same
idempotency/ordering/delete/facet semantics regardless of which database sits
behind an adapter. That uniformity comes from four small traits — one per
storage kind — that do the actual reading and writing while `kulyrax-core`
keeps doing the one thing every backend must get right identically: deciding,
from the event's sequence and the entity's persisted guard state, whether to
insert, update, delete, or skip.

Implement one of these traits, register a factory, and every mapping feature
— facets, deletes, tombstones, resurrection, out-of-order redelivery — works
for free. This is Kulyrax's public extension point: you do not fork
`kulyrax-core` to add a backend it doesn't ship.

## Which trait, for which storage kind

| Your target looks like… | Implement | Orchestration function | Built-in examples |
|---|---|---|---|
| A SQL table | [`SqlTxn`](../crates/kulyrax-core/src/adapter/sql/exec.rs) | `sql::exec::project` | SQLite, Postgres, MySQL |
| A key-value store (one blob per key) | [`KvBackend`](../crates/kulyrax-core/src/adapter/keyvalue/exec.rs) | `keyvalue::exec::project` | file-backed, Redis |
| A document store (one document per entity, optional named facets) | [`DocumentBackend`](../crates/kulyrax-core/src/adapter/document/exec.rs) | `document::exec::project` | file-backed, MongoDB |
| A graph store (nodes + edges) | [`GraphBackend`](../crates/kulyrax-core/src/adapter/graph/exec.rs) | `graph::exec::project_node` / `project_edge` | file-backed, Neo4j |

Each trait is small — five to eight methods — because it only owns what a
driver genuinely must: opening a transaction/session, running a read or
write, and reporting the outcome. The orchestration function (already
written, in `kulyrax-core`) owns everything else: reading the guard, calling
`decide()`, choosing insert vs. update vs. delete vs. skip, and writing the
updated guard back. You never call `decide()` yourself and you never see
`WriteDecision` in your own code — the trait's methods are the entire
contract.

Read the trait you're implementing; its doc comment states the exact
per-method contract. This guide covers what's true across all four.

## The atomicity contract

Every trait's methods run inside one call to its orchestration function. That
function reads the guard, decides, and writes the result **as if it were one
operation** — your implementation is what makes that true or false. Two
shapes satisfy it, and every built-in backend uses one of them:

- **A real transaction or session**, spanning the guard read through both
  writes (the target row/document/node and the guard row/document/node).
  SQLite and Postgres do this with a database transaction; MongoDB does it
  with a client-session transaction. If your store has ACID transactions,
  use one — this is the simplest shape and needs no retry logic.
- **A native compare-and-swap plus a retry loop.** If your store can't hold a
  transaction open across a read and a later write cleanly (or you'd rather
  not), have your write path fail with a "someone else changed this"
  condition, catch that in your `StorageAdapter::handle()`, and retry the
  whole read-decide-write sequence from scratch in a fresh transaction,
  bounded (5 attempts is the convention every built-in optimistic-retry
  backend uses). Redis (`KvBackend`) does this with `WATCH`/`MULTI`/`EXEC`;
  Neo4j (`GraphBackend`) does it with a `WHERE`-conditioned `SET` that
  affects zero rows on conflict. **`SqlTxn` has no such retry path** — a SQL
  backend must use a real pessimistic lock (`SELECT … FOR UPDATE` or
  equivalent) reachable from a client transaction, or rely on a genuine
  single-writer guarantee the way SQLite does.

Whichever shape you pick, get it right: a backend that merely "usually" makes
the guard-and-write pair atomic will occasionally lose or duplicate an
update, exactly the bug class idempotency guards exist to prevent.

## The guard: a model, not a requirement

Every backend has to answer one question before writing: "what sequence,
delete, and permanence state did this entity's `(target, entity_key, facet)`
lane last see?" `kulyrax-core` gives you a canonical answer for that
question, [`GuardState`](../crates/kulyrax-core/src/adapter/mod.rs):

```rust
pub struct GuardState {
    pub last_sequence: u64,
    pub deleted: bool,
    pub permanent: bool,
}
```

Your trait's read method returns `Option<GuardState>` (or your kind's own
richer guard type, which exposes a `GuardState` per lane — see
`ProjectionGuard::lane_state` for the document kind's version). How you
*store* that state is entirely up to you; `GuardState` is the shape
`kulyrax-core` reads and writes, not a schema you must replicate. Two shapes
recur across the built-ins, and picking between them is mostly about what
your store's own concurrency control needs:

- **Nested** — one record per entity, with named-facet lanes nested inside
  it (a `facets: BTreeMap<String, FacetGuard>` field, or equivalent). Used by
  every file-backed adapter and by Redis and MongoDB, because a whole-entity
  read/write is natural for those stores and a single document/key can hold
  arbitrary nested structure.
- **Flat** — one row/node per `(target, entity_key, facet)` lane, all in a
  `conduit_projection_state`-shaped table (or a `__ConduitGuard`-shaped node
  label). SQL uses this because a relational table naturally has one row per
  key. Neo4j uses this too, but for a different, load-bearing reason: Cypher
  cannot compare a value *inside* a nested/blob property for a CAS check —
  the property being compared and the property being set must be a direct
  node property, so the guard has to be its own node.

If your store can do a compare-and-set against a value nested inside a
larger record, nested is simpler. If it can only CAS a top-level column or
property, flat is not optional — it's what makes the atomicity contract
above achievable at all.

## Registering your backend

Once you have a `StorageAdapter` implementation (built directly, or via one
of the four traits above plus its orchestration function), make it available
under a `type:` name in the YAML config:

```rust
use kulyrax_core::adapter::StorageAdapter;
use kulyrax_core::runtime::AdapterBuildContext;
use kulyrax_core::runtime::config::{ConfigError, CustomAdapterConfig};
use kulyrax_core::KulyraxRuntime;

pub fn factory(
    entry: &CustomAdapterConfig,
    ctx: &AdapterBuildContext,
) -> Result<Box<dyn StorageAdapter>, ConfigError> {
    // `entry.id` / `entry.priority` are already parsed; `backend_config`
    // deserializes your own `config:` block with a uniform error message.
    let cfg: MyDbConfig = entry.backend_config()?;
    Ok(Box::new(MyDbAdapter::new(
        entry.id.clone(),
        entry.priority,
        &cfg,
        ctx.mappings.document.clone(), // the mapping table for your storage kind
        ctx.upcasters.clone(),
        ctx.migration_policy,
    )))
}

let mut runtime = KulyraxRuntime::load(&config_path, &mappings_dir)?;
runtime.register_adapter_factory("mydb", factory);
```

The [`kulyrax-adapter-postgres`](../crates/adapters/kulyrax-adapter-postgres/src/lib.rs)
and [`kulyrax-adapter-elasticsearch`](../crates/adapters/kulyrax-adapter-elasticsearch/src/lib.rs)
factories are complete real examples.

Call this **before** the runtime resolves adapters (i.e. before the first
`run_once`/`dry_run_once`/`replay`/`run_sources` call) — typically right after
`KulyraxRuntime::load` in your embedding application's `main`, if you're
using Kulyrax as a library (the "embedded crate" deployment mode; see
`architecture.md` §1.3). From that point on, `type: mydb` in the YAML config
resolves through your factory exactly like `type: postgres` resolves through
a built-in one: same `id`/`priority`/`capabilities`/`depends_on` handling,
same dependency-order and routing treatment, same dispatch. Nothing in
`dispatch.rs` or `runtime/dependency_graph.rs` distinguishes a registered
adapter from a built-in one.

The registry is owned by the `KulyraxRuntime` you register on, not a
process-wide global, so two runtimes in one process never see each other's
registrations.

If `type:` names something nobody registered, that adapter becomes an
always-fails placeholder rather than crashing config loading — every other
adapter in the config still builds and runs, and the missing registration
shows up the first time an event actually routes to it. This is deliberate:
`build_adapters_from_config` has an infallible signature and the "zero
panic" rule in `kulyrax-core` means registration typos surface as a normal
per-event failure, not a startup crash.

There is no dynamic loading here — no `dlopen`, no FFI. This is Rust's
ordinary compile-time extension model: implement the trait, depend on
`kulyrax-core`, call `register_adapter_factory` on your runtime.
`kulyrax-cli` as shipped has no community backends registered; getting one
into a binary means building a small custom binary that builds its own
`KulyraxRuntime` — a few lines beyond what `kulyrax-cli`'s own `main.rs`
already does, consistent with the Thin CLI Pattern.

## Worked example: the MySQL backend

[`kulyrax-adapter-mysql`](../crates/adapters/kulyrax-adapter-mysql/src/lib.rs)
implements `SqlTxn` for MySQL in its own crate, exactly as an out-of-tree
backend would:

- **Which trait:** `SqlTxn`, because the target is a SQL table.
- **Atomicity contract:** the pessimistic-lock shape — a `REPEATABLE READ`
  transaction with `SELECT … FOR UPDATE` on the guard row, the same shape
  Postgres uses (`InnoDB` supports the same row lock).
- **The stress test the trait had to survive:** MySQL's upsert syntax is
  `INSERT … ON DUPLICATE KEY UPDATE`, not `ON CONFLICT … DO UPDATE` — a
  real, structural difference from both SQLite and Postgres. The
  `Placeholders` enum in `sql/exec.rs` grew a `MySql` variant precisely to
  let `SqlWrite::render` emit either shape from the one neutral write
  description; nothing about `SqlWrite`'s own shape had to change. That's
  the point of designing an extension surface after multiple different
  targets prove it, not after just one.
- **A deliberate simplification, documented where it's made:** unlike
  Postgres, this backend does not introspect column types before binding —
  MySQL's own implicit string/numeric coercion is looser than Postgres's
  strict typed protocol, so JSON scalars bind by their own Rust shape
  (`bool` → `Value::Int(0/1)`, a JSON number → `Value::Int`/`Value::Double`,
  a JSON string → `Value::Bytes`). This is a real trade-off, not an
  oversight — see the module's doc comment for the reasoning, and
  `tests/mysql_backend.rs`'s `mysql_type_coercion` test for exactly what it
  does and doesn't handle (a temporal column needs MySQL's own
  `YYYY-MM-DD HH:MM:SS` form, not RFC 3339).
- **Test coverage:** `crates/adapters/kulyrax-adapter-mysql/tests/mysql_backend.rs` runs the
  same scenario suite as `pg_backend.rs` — out-of-order upsert, delete
  lifecycle, disjoint facets, a missing-column rejection, type coercion,
  cross-backend convergence with SQLite, and concurrent writers under the
  `FOR UPDATE` lock.

A community backend for a store Kulyrax doesn't ship (DynamoDB, …) follows
the same pattern from its own crate: implement the trait for your storage
kind, satisfy the atomicity contract, choose a guard shape, expose a
`factory`, and have the embedding application register it on its
`KulyraxRuntime`.
