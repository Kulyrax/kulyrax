---
status: accepted
---

# Which public types are `#[non_exhaustive]` (step 4, D1)

Task: [2026-11-26-api-non-exhaustive](../tasks/2026-11-26-api-non-exhaustive.md)

## Context

Runbook D1: "`non_exhaustive` on every public enum and growable pub struct". The
binding decisions name `Event` (stability §9.6) and the adapter configs (§9.4, D4).
Making a type `#[non_exhaustive]` stops every other crate from building it with a
literal, and from matching it exhaustively. That includes Kulyrax's own adapter
crates, tests and examples. So "growable" needs a rule, and every type that code
outside its crate builds needs a constructor.

In the published crates, outside the generated `pb` code, there are 48 public
enums and 81 public structs with public fields. With all 129 marked, the
workspace had 293 errors before the adapter crates even compiled:
- `Event` 117;
- `KulyraxConfig` and `RoutingConfig` 24 each;
- the SQLite adapter configs 18;
- `SourceRunOptions` 17;
- `AdapterExecutionMeta` 11;
- then smaller numbers.

## Options considered

- **Mark everything (129 types).** This makes internal plumbing (`Layout`,
  `AdapterExecutionMeta`, stage timings) awkward for no semver gain, since D2
  hides those anyway. It also does D4's adapter configs twice.
- **Mark only `Event` and the error enums.** Every report, outcome and guard
  type would then stay frozen at its current fields. Adding a field after 1.0
  would be a breaking change.
- **A rule by stability class (chosen).**

## Decision

1. **Every public enum: `#[non_exhaustive]`** (48), as D1 says.
2. **A public struct with public fields is `#[non_exhaustive]` if it is in the
   frozen API** (stability §1): reports, outcomes, guards, plans, mappings,
   projections, `Event`, `SourcedEvent`, `KulyraxConfig`, `RoutingConfig`, the
   run and replay options, the registry build contexts, `ServeOptions`,
   `GrpcServer`, `Checkpoint`. That is 54 structs.
3. **Code outside the defining crate builds them through constructors**, not
   literals:
   - `Event::new(event_id, event_type, sequence, payload)` with
     `with_metadata` / `with_version` (§9.6);
   - `SourcedEvent::new`, `GuardState::new`, `GraphFacetGuard::new`,
     `GuardFingerprint::new`, `ShadowTable::new`;
   - `AdapterBuildContext::new`, `SourceBuildContext::new`;
   - `KulyraxConfig::new(routing_file, adapters)` with `with_failure_policy` /
     `with_migration_policy` / `with_sources`, and `RoutingConfig::new`;
   - `From<DocumentProjection> for DocumentPlan`, and
     `From<KvProjection> for KvPlan`;
   - the option structs that already derive `Default` (`SourceRunOptions`,
     `ReplayRunOptions`, `MappingSet`, `ServeOptions`): `Default`, then assign
     the public fields. That adds no API.
4. **Not marked, and why:**
   - **Adapter and source configs:** the 6 networked adapters' configs; the
     built-in `Sqlite`/`File`/`KeyValue`/`Graph`/`Custom` adapter configs; the
     `Directory`/`Stdin`/`Custom` source configs; the outbox configs. D4 makes
     them `#[non_exhaustive]` with builders, as §9.4 decides for adapters. The
     source configs follow the same rule there.
   - **Internals that stability §1 marks UNSTABLE**, which D2 hides: `Layout`,
     `AdapterExecutionMeta`, `ExecutionResult`, `AdapterBuildFailure`,
     `StageTimings`, `StageTotal`, `Reloaded`.
5. **Matching outside the crate.** A `match` over a Kulyrax enum in another
   crate gets a wildcard arm that fails safe:
   - an unknown outcome is a write failure, never a silent success;
   - an unknown `MigrationPolicy` is treated as `Strict`;
   - an unknown status exits 1;
   - an unknown variant prints as "unknown".

## Trade-offs accepted

- **The API grows by a dozen constructors.** Each takes the fields the struct
  has today. A later field gets a `with_*` method or a default, never a new
  required argument.
- **Tests and examples say `Event::new(...)` instead of a literal**: 142
  rewritten mechanically. Literals that changed `..base` became a
  `let mut o = base; o.field = …` block.

## Revisit if

- A type outside this list turns out to be built by third-party code: add a
  constructor, in a minor release.
