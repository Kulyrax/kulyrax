# kulyrax-cli

The command-line binary for [Kulyrax](https://github.com/Kulyrax/kulyrax) — a thin
wrapper (argument parsing + rendering only) over `kulyrax-core`.

```sh
cargo install kulyrax-cli

kulyrax init my-project                                         # runnable starter project
kulyrax run --config config.yaml --mappings mappings/ --once   # drain configured sources once
kulyrax sources --config config.yaml                            # list sources + committed positions
kulyrax ingest --config config.yaml --mappings mappings/ --listen tcp://0.0.0.0:50051
kulyrax explain --config config.yaml --event event.json         # routing/dependency preview, no writes
kulyrax dry-run  --config config.yaml --mappings mappings/ --event event.json   # writes it would make, no writes
```

See the root [README](https://github.com/Kulyrax/kulyrax#readme) for a full quickstart
and [`docs/concepts.md`](https://github.com/Kulyrax/kulyrax/blob/main/docs/concepts.md)
for the mental model.

Licensed under either of [MIT](https://github.com/Kulyrax/kulyrax/blob/main/LICENSE-MIT)
or [Apache-2.0](https://github.com/Kulyrax/kulyrax/blob/main/LICENSE-APACHE) at your option.
