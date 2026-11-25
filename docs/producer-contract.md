# The Producer Contract

Kulyrax is projection-only ([`architecture.md` §1.1](../architecture.md)). It consumes an event log it does not own, and its determinism / idempotency / replay guarantees hold **only if the events it receives satisfy the assumptions below**. Kulyrax sees one event at a time and cannot verify any of them — they are the application's and transport's responsibility.

---

## 1. `Event.sequence` — monotonic per entity

**Assumption.** For successive events about the *same* entity, `sequence` strictly increases. A per-aggregate version or a global monotonic log offset (Kafka offset, commit position) both satisfy this. Different entities have independent sequences.

- Gaps are fine (`1, 2, 7, 50`) — the gate needs ordering, not contiguity.
- It is **not** the event timestamp — wall-clock time is not monotonic under clock skew and can collide.
- It is unrelated to `version` (payload *schema* version, for upcasting). An entity on schema `version 3` may be at `sequence 47`.

**If violated.** Two distinct events sharing a sequence → the later-delivered one is judged stale and dropped (silent loss). A decreasing sequence → the "newer" update is rejected as stale.

---

## 2. Entity id — stable, canonical, scalar

**Assumption.** The `key` / `id` / `primary_key` path resolves to a JSON scalar, and the producer emits it in **one canonical textual form** for a given entity across its whole lifetime.

**If violated.** `550e8400-…` in one event and `{550E8400-…}` in another are two different `entity_key`s → two guard lanes → the "update" inserts a second entity instead of updating the first. Silent.

---

## 3. Per-entity delivery order

**Assumption.** Each entity's events reach Kulyrax in sequence order.

- Kulyrax dispatches events in the order the source delivers them, within a batch and across batches alike, and never re-sorts them by `sequence`: sequences are per entity (§1), so a sort would reorder *different* entities against each other (an edge before the nodes it points at, §7; a child row before its parent). The outcome therefore does not depend on where batches split. Replay follows the same rule: sorted file names, then line order.
- `sequence` gates each entity's own lane (`decide()`). That absorbs redeliveries and, under `on_existing: replace`, out-of-order updates: the highest sequence wins.
- It does **not** repair: an event permanently dropped; or, under `on_existing: ignore`, a lower-sequence event arriving *after* that entity's lane has already advanced (the first-delivered event wins, so a late "create" is skipped and its data never lands). This holds within one batch as much as across batches.
- `on_existing: replace` and facet lanes are order-independent *within a lane* — highest sequence wins — but a facet update delivered before the entity's create is skipped (`EntityAbsent`) in a live feed.

**If violated.** Lost updates (`ignore` mode) or skipped facet writes (out-of-order create).

---

## 4. One entity, one source

**Assumption.** An entity's events all arrive through a single `EventSource`. There is no ordering guarantee *across* sources.

**If violated.** Two sources feeding the same entity can interleave arbitrarily; the sequence gate still prevents corruption but convergence is not guaranteed if the sources' positions advance independently.

---

## 5. Delete route ⊇ create route

**Assumption.** A `delete` event (e.g. `OrderCancelled`) routes to *at least* the same adapters its `create` event (`OrderCreated`) routed to.

**If violated.** The entity is removed from some stores and left in others. Kulyrax routes by `event_type` and cannot compare the two routes.

---

## 6. Fat events, not deltas

**Assumption.** Every mapped value is fully resolved from the event payload / metadata. An event that logically increments a value carries the *computed result* (`new_balance: 150`), never the delta (`amount: +50`).

**If violated.** Kulyrax has no read-modify-write path; a delta would be written literally as the new value.

---

## 7. Graph endpoint order

**Assumption.** If edges reference nodes, the producer emits the node before edges that point at it — *when the target graph backend enforces endpoint existence*. The file-backed graph adapter is permissive about this (node ids are opaque strings); a backend that does enforce it (or a future one that would) responds with `Skipped(EndpointAbsent)` instead.

**If violated (permissive backend):** nothing — the edge is written referencing a node that doesn't exist yet, and resolves once the node arrives. **If violated (enforcing backend):** the edge write is skipped until the node exists.

---

## 8. Atomic file delivery (directory source)

**Assumption.** A producer writing event files into a watched directory writes to a temp name and `rename`s the finished file in — never appends to a file Kulyrax may already be reading.

**If violated.** The directory source reads a whole file at once; a half-written trailing NDJSON line is a parse error → that line is sent to the dead-letter queue, and the rest of the file continues.

---

## 9. Positions

A **position** is an opaque resume token. One source makes it, for itself: a file name, a line number, an outbox cursor, or a gRPC producer's own `EventEnvelope.position`. Unlike the rules above, most of this section is Kulyrax's side of the contract.

- **Kulyrax never parses or compares positions.** It stores them, echoes them back, and names dead-letter files after them. Only the source that made a position may order it, by its own rule:

  | Source | Position | Ordered by |
  |---|---|---|
  | `directory` | `<file>` or `<file>#<index:012>` | file name, then index within the file |
  | `stdin` | the line number, zero-padded | the number |
  | `postgres_outbox` | `<after:020>.<late:010>` | exact lookup among the positions it handed out |
  | gRPC | the producer's `position`, as sent | never ordered: each stream's last committed one is echoed back |

  Positions of different sources, or of different gRPC streams, are never compared. Text order is not delivery order (`"9"` sorts after `"10"`).
- **A batch is committed at its last event in delivery order.** Committing it means every event the source delivered up to and including that event is resolved: projected, skipped, or dead-lettered. After a restart the source resumes after it. It delivers those events again only if Kulyrax crashed before the commit.
- **What a gRPC producer sends.** Any token it can resume from. Kulyrax does not require it to increase, or to compare as text. Several events may share one. Each stream's `Ack.position` is the last committed position that stream sent, in its own send order.
- **The position must not be empty.** A stream that sends an empty position gets empty positions back, and has nothing to resume from. In 0.12.x Kulyrax accepts it and logs a warning once per stream. **From 1.0 the stream is refused with `InvalidArgument`.**
- **`SourcePosition` has no order.** In 0.12.x the Rust type still derives `PartialOrd`/`Ord` (text order, not delivery order). Both are removed in 1.0; `Eq` stays.

**If violated.** A custom `EventSource` whose batch's last event is not its furthest position is committed at the wrong place. It resumes too early (redelivery, absorbed by the guards) or too late (events skipped).

## Kafka-specific (once a native Kafka source exists)

**Assumption.** A producer partitions by entity id, so one entity's events land in one partition and Kafka's own per-partition ordering guarantee satisfies rule 3 above. A single consumer-group member reads the whole topic — splitting partitions across multiple Kulyrax instances can move an entity's stream mid-flight and break ordering for it.
