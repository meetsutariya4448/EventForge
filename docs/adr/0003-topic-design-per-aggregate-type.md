# ADR-0003: Topic design — topic-per-aggregate-type, partition key = order_id

## Context

Kafka only guarantees ordering within a single partition. EventForge's entire ordering story rests
on one invariant: everything that happens to a given order must be observable in the order it
happened, across every service that touches that order. There is no requirement anywhere in this
project for ordering *across* different orders.

## Options considered

**Topic-per-event-type** (e.g. separate topics `order-created`, `payment-authorized`,
`inventory-reserved`, ...). Common in some event-driven designs, and it lets a consumer subscribe
to exactly the event types it cares about. But it scatters a single order's causally-related events
across multiple topics with independent partition assignments — there is no ordering guarantee
between `order-created` on one topic and `payment-authorized` on another, even if both are
partitioned by `order_id`, because Kafka's ordering guarantee is per-partition-per-topic, not
per-key-across-topics.

**Topic-per-aggregate-type** (e.g. `orders.events`, `payments.events`, `inventory.events`), keyed
by `order_id`. Every event about a given order — regardless of which service produced it or what
type of event it is — lands in the same partition of the same topic (once partition count and
hashing are held constant), because they share the same key. That's the ordering guarantee this
project actually needs, and it's the one Kafka actually provides.

## Decision

Topic-per-aggregate-type, partition key = `order_id`. Concretely: `orders.events`,
`payments.events`, `inventory.events` (and `notifications.events` if notification-service ever
publishes). Every producer sets the Kafka record key to the order's aggregate id, never to
anything else — this is what keeps a single order's causal chain in one partition.

**EventForge relies on per-order ordering exclusively. It never relies on, and must never be
designed to require, ordering between different orders' events, or global ordering across the
cluster.** This is a hard boundary, not a simplification to revisit later.

## Consequences

- Consumers that only care about one event type still receive every event type for that
  aggregate type and must filter by `eventType` in the envelope — a minor tax in exchange for the
  ordering guarantee.
- Partition count for each topic is a throughput/parallelism knob, not an ordering knob — adding
  partitions changes which orders land on which partition (via key hashing) but never breaks
  per-order ordering, since a given `order_id` always hashes to the same partition as long as
  partition count is stable.
- Any future consumer-group parallelism work (M6, KEDA scaling) scales by adding consumers up to
  the partition count, not by reasoning about cross-partition ordering.

## Revisit if

A workflow emerges that genuinely needs ordering across aggregates (none is anticipated), or
partition-count changes become frequent enough that key-hash stability needs its own tooling.
