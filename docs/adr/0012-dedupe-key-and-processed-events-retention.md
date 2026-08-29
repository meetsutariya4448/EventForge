# ADR-0012: Dedupe key design and the `processed_events` retention window

## Context

M2 needs a dedupe mechanism every consumer uses identically (constitution Part 2's consumer
transaction invariant): `INSERT processed_event(consumer_group, event_id)` inside the same
transaction as the business mutation, unique-violation (here, zero rows affected via
`ON CONFLICT DO NOTHING`) means already handled. Two decisions need stating plainly: what the key
actually is, and what happens to this protection over time, since `processed_events` is not
retained forever.

## The key: `(consumer_group, event_id)`, never `event_id` alone

`event_id` alone would conflate two different questions: "has this event been processed at all"
and "has this event been processed by *this* consumer." Two different consumer groups
(`payment-service`, `notification-service`) must each process the same `OrderCreated` exactly
once, **independently** — `notification-service` marking an event processed must never prevent
`payment-service` from processing that same event, and vice versa. A composite key scoped to
`(consumer_group, event_id)` is what makes that independence possible; `event_id` alone would make
the second group to see any given event always find it "already processed" by the first.

This is not a hypothetical concern: `IndependentConsumerGroupOffsetsIntegrationTest` (item 6)
depends on it directly — two groups consuming the same topic, each with entirely separate offset
positions *and* entirely separate dedupe scopes, is the actual claim notification-service exists
to prove.

## `processed_events` is not retained forever, and here's what breaks when it isn't

No archival or deletion logic exists yet (M0's `idx_processed_events_processed_at` index reserved
the capability; nothing implements it — this is still the case, per ADR-0004's T3 posture). But
the retention question isn't "if," it's "when, and what happens then" — pretending otherwise would
be exactly the kind of unmeasured optimism the constitution warns against.

**What specifically breaks:** if a `(consumer_group, event_id)` row is ever purged from
`processed_events` — by a future archival job, by a manual `DELETE`, by any mechanism — and a copy
of that same event is redelivered *after* the row is gone, the dedupe check finds nothing, returns
"first time," and the business mutation **runs again**. This is not a vague "duplicates may occur"
risk; it is a specific, mechanical failure mode: the exact same crash-after-broker-ack window
`RelayProducedDuplicateEndToEndTest` proves is absorbed today would **not** be absorbed once the
relevant `processed_events` row has aged out, because the mechanism that absorbs it is that row's
continued existence, not something more durable.

**Why this matters differently per consumer.** For `payment-service` and `inventory-service`, a
duplicate that slips past an expired dedupe row hits a **second, local safety net**: the `payments`
and `inventory_order_events` tables both carry their own `UNIQUE (order_id)` constraint, so a
second attempt to write the same business row fails at the database level even if
`processed_events` no longer remembers it — degraded, not silent, and recoverable (the failed
`INSERT` just needs handling, not a corrupted ledger). For `notification-service`, no such second
constraint exists — `sent_notifications` deliberately has none (see its Javadoc) — because there is
nothing local to constrain: the effect already happened externally the first time, and an expired
dedupe row followed by redelivery means a second real notification goes out, with no database
constraint anywhere capable of stopping it.

## Decision

Composite key `(consumer_group, event_id)`, matching `processed_events`' primary key from ADR-0004.
No retention/archival policy is implemented this milestone — this ADR states the failure mode
honestly rather than implementing a fix or deferring the question silently. Any future retention
window must be chosen long enough that it exceeds the maximum realistic redelivery delay (broker
outage duration, consumer downtime, replay windows like item 6's) for every consumer group reading
that service's topics — a number this project has not measured and should not guess at.

## Consequences

- Local-effect consumers (payment, inventory) are protected twice: `processed_events` first, a
  business-table uniqueness constraint second. Deliberate defense in depth, not redundancy for its
  own sake — the second constraint is what turns "silent duplicate" into "loud, recoverable
  conflict" once the first layer's protection window has passed.
- `notification-service` is protected once. This was a deliberate design choice for this service
  specifically (see its Javadoc) precisely so this gap would be visible and testable rather than
  papered over by an easy second constraint that a real external side effect doesn't actually have
  an equivalent for.

## Revisit if

An actual retention/archival implementation is built (T3, still open) — at that point this ADR's
"no policy implemented" statement becomes false and needs updating alongside the code, and the
chosen retention window's reasoning belongs here, not just in the migration that adds it.
