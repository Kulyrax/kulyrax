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

- Batch dispatch tolerates *local* reordering within a batch — it sorts by `sequence` before dispatch.
- It does **not** tolerate: an event permanently dropped; or, under `on_existing: ignore`, a lower-sequence event arriving *after* that entity's lane has already advanced (the first-delivered event wins, so a late "create" is skipped and its data never lands).
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

## Kafka-specific (once a native Kafka source exists)

**Assumption.** A producer partitions by entity id, so one entity's events land in one partition and Kafka's own per-partition ordering guarantee satisfies rule 3 above. A single consumer-group member reads the whole topic — splitting partitions across multiple Kulyrax instances can move an entity's stream mid-flight and break ordering for it.
