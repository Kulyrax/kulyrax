# embedded

The "embedded crate" deployment mode (`architecture.md` §1.3): link `kulyrax-core`
straight into a host service and call `execute_event` — no CLI, no config file on
disk beyond a routing table, no gRPC.

```sh
cargo run --manifest-path examples/embedded/Cargo.toml
```

```
status: Succeeded
  sql-primary -> Created
projected: users.u1.name = "Ada"
```

## Routing

`execute_event` takes routing rules as an explicit `&HashMap<String, Vec<AdapterId>>`
argument — no config file, env var, or global state involved. This example builds
the map in-process; a real host would more likely load it once at startup via
[`kulyrax_core::routing::load_routing`](../../crates/core/src/routing.rs)
and reuse it across calls.

## Next

- [`docs/concepts.md`](../../docs/concepts.md) for the mental model.
- [`examples/sqlite-quickstart/`](../sqlite-quickstart/) for the same projection through
  the `kulyrax` CLI and a real event source instead of one direct call.
