# ADR-0013: The effectively-once boundary (trap T4)

## Context

Every M2 test that proves "exactly one business effect" — `DuplicateDeliveryIntegrationTest`,
`ConcurrentDuplicateDeliveryIntegrationTest`, `RelayProducedDuplicateEndToEndTest` — proves it for
`payment-service` authorizing a payment as a **local Postgres row**, written inside the exact same
transaction as the dedupe check. That guarantee is real, and it is also narrower than it sounds if
read as "EventForge does effectively-once payment processing." This ADR states the boundary
precisely, because trap T4 exists specifically to stop that overclaim from happening by accident.

## The one sentence

**The effectively-once guarantee holds here because "authorizing a payment" means writing a row to
a database we control, inside the same transaction as the dedupe check — the moment a real payment
provider is involved, the actual charge happens over the network, outside that transaction, and the
guarantee this project can make stops being "we processed this exactly once" and becomes "we called
a provider whose own idempotency key determined whether they charged the card exactly once," which
is a guarantee that belongs to them, not to us.**

## Why this is true, mechanically

`PaymentAuthorizationService.handleOrderCreated` does three things inside one `@Transactional`
method: the dedupe insert, the `payments` row write, and the next outbox write. All three commit
together or none do (constitution Part 2's invariant). A duplicate delivery is caught by the
dedupe insert *before* the payment row is written — so from Postgres's point of view, "the payment
was authorized" and "we will never authorize it again for this event" are a single atomic fact.
That's what every duplicate test in this milestone actually exercises.

A real payment provider call (Stripe, Adyen, a card network) cannot be inside that transaction.
Network calls cannot participate in a Postgres transaction's atomicity — there's no way to make
"charge the card" and "commit this row" a single all-or-nothing unit. The call has to happen either
before the transaction (in which case a transaction rollback after a successful charge leaves you
with a charge and no record of it) or after (in which case a crash between the charge succeeding
and the transaction committing leaves the identical ambiguity `RelayProducedDuplicateEndToEndTest`
proves the *relay* side of this project already has to live with — except now it's real money, not
a Kafka record).

The way real integrations solve this is the provider's own idempotency key: the caller generates a
key (naturally, `event_id` or a value derived from it), sends it with every attempt including
retries, and the *provider* guarantees that N calls with the same key produce one real charge. That
guarantee is enforced by their systems, under their control, not EventForge's. Once that's the
mechanism, "did the customer get charged exactly once" is a question this project can help ask
correctly (by always sending the same idempotency key for the same logical event) but cannot answer
on its own — the answer lives with the provider.

## Decision

State this boundary here rather than let the M2 test suite's "exactly one business effect" language
imply something broader. No code changes accompany this ADR — `PaymentAuthorizationService` still
models payment as a local ledger row, deliberately (constitution: "domain is scaffolding, do not
enrich it"). This ADR exists so the boundary is documented before anyone — including a future
milestone — mistakes "the tests pass" for "this handles real payments correctly."

## Consequences

- Nothing in this project currently calls an external payment provider, so nothing here is broken
  today — this ADR is preemptive documentation of a boundary, not a bug report.
- If a future milestone ever does integrate a real provider, the idempotency key it sends must be
  derived from `event_id` (the same identifier `processed_events` already dedupes on), and the
  local `payments` row should record that provider idempotency key/reference — without that,
  there's no way to reconcile "we think we called them once" against "they think they charged the
  card N times."

## Revisit if

A real payment provider integration is ever built — at that point this ADR's boundary becomes the
starting design constraint for that work, not an afterthought bolted on once something's already
gone wrong in production.
