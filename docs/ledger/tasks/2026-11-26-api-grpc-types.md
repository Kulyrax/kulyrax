---
status: completed
---

# The gRPC crates use Kulyrax-owned types; raw pb behind `unstable-pb` (step 4, D3)

## Objective

Runbook D3: "pb behind Kulyrax-owned types; raw pb only under the `unstable-pb`
feature, `#[doc(hidden)]`." The frozen contract is
`proto/kulyrax/v1/ingest.proto` (guarded by D9's `buf breaking`). The
tonic/prost code generated from it must not be public API. Otherwise a tonic or
prost upgrade would force a major release. This is tranche 2 of Phase D (D3, D4,
D9).

It also corrects D2: the storage kinds' facade re-exports are removed, not
hidden ([decision](../decisions/2026-11-26-canonical-paths.md), item 3).

## Acceptance criteria

- [x] `kulyrax_client::pb` and `kulyrax_ingest::pb` are private by default.
  With the `unstable-pb` feature they are `#[doc(hidden)] pub`.
- [x] `kulyrax_client` has owned types:
  - `#[non_exhaustive] EventEnvelope`, with `new(event_type, payload, sequence,
    position)`, `with_metadata`, `with_version` and `with_event_id`;
  - `#[non_exhaustive] Ack`, plus `AckOutcome`;
  - `StatusCode` (the 17 gRPC codes), `RpcError { code, message }`;
  - `ClientError::{Connect, Rpc, Closed}`, which implements `Error` with
    `source()`.

  `Producer` keeps its methods. No tonic or prost type appears in a public
  signature.
- [x] `pb::EventEnvelope::from_event` (ingest) exists only in tests and under
  `unstable-pb`.
- [x] The ingest tests, the gRPC example and `docs/producer-contract.md` §9 use
  `EventEnvelope::new`. The rc.1 notes have before/after code.
- [x] The D2 correction:
  - the four `pub use exec::{…}` facades are removed from
    `adapter::{sql,document,graph,keyvalue}`;
  - the Elasticsearch adapter imports from `document::exec`;
  - the decision record and the rc.1 notes are updated.
- [x] Gates: fmt, clippy `-D warnings` (default features, and with
  `unstable-pb`), workspace tests, examples build, every Docker backend suite.
- [x] The semver report has no `trait_newly_sealed` finding.
- [x] PR #31 merged with the 12 required checks green (all 18 checks pass; merge commit 3243836).

## Constraints and assumptions

- No new features. The wire format is unchanged, and no `.proto` edit.
- `StatusCode` mirrors tonic's `Code` one to one. `from_tonic` is private, so
  tonic stays out of the public API.
- `ClientError::Connect` holds the transport error's text rather than the
  error itself. The tonic transport error type would otherwise be public.

## Verification

| Check | Command | Result |
|---|---|---|
| Local semver report, before the facade fix | `cargo semver-checks -p kulyrax-core --baseline-rev v0.12.2` | `trait_newly_sealed` for `SqlTxn`, `DocumentBackend`, `GraphBackend` (first-run failure, cause: D2's `#[doc(hidden)]` facades; un-hiding the document facade alone cleared `DocumentBackend`) |
| Local semver report, after | same | no `trait_newly_sealed`; 14 major findings remain, all expected D1/D2 changes |
| fmt | `cargo fmt --check` | clean |
| clippy | `cargo clippy --workspace --all-targets -- -D warnings`; again for client + ingest with `unstable-pb` | clean, both |
| Workspace tests | `cargo test --workspace` | 529 pass, 0 fail |
| Examples | `cargo build` for each of the 6 example manifests; `cargo run` of `examples/embedded` | 0 errors each; embedded prints `projected: users.u1.name = "Ada"` |
| Docker suites (Neo4j on 7688) | `docker-suites.sh` | postgres 30, mysql 25, redis 14, mongodb 21, elasticsearch 11, neo4j 20, source-postgres 14, ingest 15, cli 32: all pass, 0 fail |

## Risks and open questions

- Code written against 0.12's `adapter::<kind>::<Name>` paths stops compiling,
  so it must import from `exec`. This is documented in the rc.1 notes.

## Outcome

Completed. The gRPC crates expose only Kulyrax-owned types; generated code is behind `unstable-pb`. The storage-kind facades are removed, and the semver report no longer reads the backend traits as sealed.
