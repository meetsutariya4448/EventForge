# ADR-0016: Compensation idempotency — three layers, not one

## Context

Constitution item 4: "Compensations are the place where teams forget their own idempotency rules."
A naive implementation protects a compensation command against the one redelivery shape M2 already
solved (the same Kafka message redelivered) and stops there — missing that a *retried* compensation
command (dispatched again by the orchestrator itself, under a brand new `event_id`, because the
first attempt's response never arrived) is a different shape of duplicate that plain
`(consumer_group, event_id)` dedupe cannot see at all, since the event_id genuinely differs.

## Decision

`RefundPayment` — and the orchestrator's own handling of the facts that answer it — is protected at
three independent layers, each catching a duplicate shape the others cannot:

**1. Event-level dedupe** (`payment-service`, `processed_events`, unmodified from M2). The exact
same `event_id` delivered twice — a genuine Kafka-level redelivery — is a clean no-op, exactly like
every other M2 consumer. This is what constitution item 4's literal required test exercises:
`DuplicateRefundCommandIntegrationTest` calls `handleRefundPayment` twice with an identical
envelope and asserts one refund, one `PaymentRefunded` outbox row.

**2. Business-level safety net** (`payment-service`, checking `Payment.status`). A *different*
`event_id` for an order whose payment is already `REFUNDED` — the shape a sweep-triggered
`RefundPayment` redispatch actually produces, since each redispatch is a new, legitimate command
from the orchestrator's point of view, not a redelivery — does not refund a second time, but the
`PaymentRefunded` fact is still re-announced (a fresh outbox write) so the orchestrator can
converge if the *original* announcement, not just the original command, is what never arrived. This
mirrors the same "second local safety net" pattern ADR-0012 established for ordinary duplicate
authorization, applied to the compensation side.

**3. State-guard idempotency** (`order-service`, the orchestrator). A `PaymentRefunded` fact
arriving when the saga is no longer `AWAITING_REFUND` — for instance, a second fact answering a
sweep-redispatched command, after the first fact already moved the saga to `COMPENSATED` — is
ignored as a clean no-op, logged but not acted on. This is the same discipline every
`SagaOrchestrator` handler follows for every fact type (`handlePaymentAuthorized`,
`handleInventoryReserved`, `handleInventoryReservationFailed` all check the expected state first),
not something invented specifically for refunds — but it is the layer that specifically closes the
gap layers 1 and 2 cannot: neither payment-service's event-level nor business-level checks know or
care whether the *orchestrator* has already consumed a `PaymentRefunded` fact for this saga.

## Why not just one layer

Layer 1 alone misses redispatch-shaped duplicates (different `event_id`, same logical command).
Layer 2 alone would refund correctly but could leave the orchestrator's saga stuck if the
*original* fact-announcement is what was lost, not the command. Layer 3 alone would still let
payment-service actually charge/refund twice if it, not the orchestrator, is what re-processes a
duplicate. Each layer's job is narrow and the three together are cheap — none of this required new
infrastructure, only reusing the dedupe primitive (ADR-0012), the same "second local safety net"
philosophy (ADR-0012 again), and the same state-machine-guard shape already used everywhere else in
`SagaOrchestrator`.

## The saga's own identifier, restated

`saga_instance.saga_id` is the saga's key. `correlation_id` (the root `OrderCreated` event's own
`event_id`) is stored on `saga_instance` too, but purely so every dispatch this saga makes —
including timeout-triggered ones, which have no inbound envelope to read a correlation id from —
can carry the correct root value; it is never used to look up or identify a saga. `order_id`
(unique per saga) is what every lookup (`findByOrderId`) actually keys on, since it is also the
`aggregate_id` on every event in the whole exchange. Neither `correlationId` nor `order_id` is
overloaded as the saga's primary identity — `saga_id` is, and it appears nowhere in any event
payload, purely a `saga_instance`/`saga_step` implementation detail.

## Consequences

- No dedicated test exercises layer 2 in isolation (the business-level safety net's re-announce
  path) beyond code-level reasoning — none of constitution item 8's required tests reach it, since
  every required scenario either never redispatches, or redispatches against a dead consumer that
  never processes any attempt. Stated here rather than left implicit: this is a documented,
  deliberately-scoped gap in test coverage, not a gap in the production code path.

## Revisit if

A real payment provider is ever integrated (see ADR-0013/T4) — layer 2's "already refunded, don't
double-charge" check becomes load-bearing for real money, not just this project's local ledger, and
should be re-verified against whatever idempotency guarantee the provider itself offers.
