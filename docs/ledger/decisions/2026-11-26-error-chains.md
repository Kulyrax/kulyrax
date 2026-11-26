---
status: accepted
---

# Error chains: typed safe causes, driver errors stay redacted strings (step 4, D6)

Task: [2026-11-26-api-errors](../tasks/2026-11-26-api-errors.md)

## Context

`stability.md` §9: "Errors: `source()` chains in step 4. `DependencyOrderError`
implements `std::error::Error`. `AdapterReportError` is report data and is
renamed to a name that does not end in `Error`." Finding F4: the error types
flatten their causes into strings, and a chain means changing those variants.

Measured on 2026-11-26:
- the adapters' `WriteFailed` / `ExecutionFailed` / `BuildFailed` variants
  have about 250 construction sites in 10 adapters;
- many of those strings hold a driver error. A driver's `Display` can print the
  connection URL, and A1b (F-06) redacted it out of exactly these strings;
- the YAML, JSON and I/O causes sit in a few places in core.

## Options considered

The owner was asked on 2026-11-26, with three options:
- **chain the safe causes** (recommended, about 0.5 day);
- chain everything, driver errors included (about 1.5 days, plus a fresh F-06
  check of each driver's error text);
- minimal: no variant changes.

## Decision

The owner chose **chain the safe causes**:

1. **Every public error is `std::error::Error + Send + Sync + 'static`.** A
   compile-time test lists each one, so a new field that is not `Send` fails
   the build.
2. **`source()` returns the typed cause where it is an I/O, YAML, JSON, source
   or pipeline error:**
   - `PipelineError::ConfigParse` carries the `YamlError`, and
     `PipelineError::EventParse` the `serde_json::Error`;
   - `PipelineError::Mapping` and `PipelineError::Routing` carry a new
     `pipeline::LoadError`: a parse error (`YamlError`), a read error
     (`io::Error`), or a plain message (a duplicate mapping, a wrong
     extension);
   - `ReplayLoadError::Json` carries the `serde_json::Error` and its location;
   - `InitError::Io`, `IngestError::Runtime` and the three `ServeError`
     variants return what they hold.

   Each `Display` is unchanged, so the CLI prints the same text.
3. **Driver and database errors stay redacted strings**:
   - the adapters' `WriteFailed` / `ExecutionFailed` / `BuildFailed`;
   - `ConfigError::FactoryConfigInvalid`;
   - `SourceError::Unavailable`.

   Their text went through `redact`, and a raw driver error in a `source()`
   chain would bring the secret back for any caller that prints the chain.
4. **`SourceError`'s `Parse` / `Checkpoint` / `Unavailable(String)` stay.**
   They are how a custom source reports its own failures, in its own words.
   `SourceError::Io` already chains.
5. **`DependencyOrderError`** implements `Display` and `Error`.
6. **`AdapterReportError` is renamed `AdapterFailure`.** It is the report's
   record of an adapter failure (with `AdapterBuildFailure` and
   `PrepareFailure`), not an error value.

## Trade-offs accepted

- **A caller cannot downcast to a driver error** (to retry on a Postgres
  serialization failure, say). It reads the message. Revisit with a vetted,
  redacting wrapper if a real need appears.
- **`LoadError` is a new public type.** It is `#[non_exhaustive]`, so a later
  cause is not a breaking change.

## Revisit if

- A user needs to tell driver failures apart in code: add a typed, redacted
  cause per backend, after an F-06-style review of its text.
