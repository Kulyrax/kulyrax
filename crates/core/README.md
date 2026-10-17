# kulyrax-core

The engine crate of [Kulyrax](https://github.com/Panchauly/kulyrax): a deterministic,
projection-only engine for CQRS / event-sourcing read models. It takes one [`Event`] at
a time and projects it into SQL (SQLite, Postgres), a document store, a key-value store,
or a graph — through one shared, sequence-gated `decide()` core, with no engine-level
special-casing per storage kind.

```rust,no_run
use kulyrax_core::{execute_event, runtime::config::KulyraxConfig};
use kulyrax_core::event::Event;
use std::collections::HashMap;

# fn example(config: &KulyraxConfig, event: Event) {
let report = execute_event(
    config,
    HashMap::new(), // sql_mappings
    HashMap::new(), // document_mappings
    HashMap::new(), // kv_mappings
    HashMap::new(), // graph_mappings
    event,
);
# }
```

Most applications use this crate through `kulyrax-cli` (`kulyrax run` / `kulyrax ingest`)
rather than calling it directly; the embedded mode above is for services that want
projection in-process.

- **Concepts & mapping reference:** [`docs/concepts.md`](https://github.com/Panchauly/kulyrax/blob/master/docs/concepts.md), [`docs/mapping-reference.md`](https://github.com/Panchauly/kulyrax/blob/master/docs/mapping-reference.md)
- **Architecture:** [`architecture.md`](https://github.com/Panchauly/kulyrax/blob/master/architecture.md)
- **Adding a backend:** [`docs/writing-a-backend.md`](https://github.com/Panchauly/kulyrax/blob/master/docs/writing-a-backend.md)
- **Producer assumptions:** [`docs/producer-contract.md`](https://github.com/Panchauly/kulyrax/blob/master/docs/producer-contract.md)

Licensed under either of [MIT](https://github.com/Panchauly/kulyrax/blob/master/LICENSE-MIT)
or [Apache-2.0](https://github.com/Panchauly/kulyrax/blob/master/LICENSE-APACHE) at your option.
