# ADR-0014: The saga orchestrator lives inside order-service, not as a fifth service

## Context

M3 needs an orchestrated saga (constitution item 1: explicitly dispatched commands, persisted
state, not choreographed, not in-memory). Item 2 asks the question directly: does the orchestrator
get its own module/service, or does it live inside an existing one — with an explicit bias toward
not adding a fifth service.

## Decision

The orchestrator (`SagaOrchestrator`, `SagaEventListener`, `SagaTimeoutSweeper`, and the
`saga_instance`/`saga_step` tables) lives inside **order-service**, in its own `com.eventforge.order.saga`
package, using order-service's existing Postgres database and Kafka consumer/producer
infrastructure. No new deployable, no new database, no new topic-per-service beyond what
participants already needed.

## Why order-service specifically

The saga's first step (`AuthorizePayment`) must be dispatched atomically with `OrderCreated` — both
inside the exact same Postgres transaction that writes the `orders` row, or the outbox pattern's
whole point (constitution's dual-write guarantee) breaks at the very first hop. Concretely,
`OrderService.createOrder` now does, in one `@Transactional` method: write the order row, write
`OrderCreated` (sequence 1), write `saga_instance` (state `AWAITING_PAYMENT`), write
`AuthorizePayment` (sequence 2) — four writes, one transaction, one commit.

A separate orchestrator service could not offer this. It would need either its own database (in
which case starting a saga in response to `OrderCreated` becomes an asynchronous, eventually-consistent
step, reintroducing the exact dual-write gap the outbox exists to close — "order created" and "saga
started" could observably disagree) or a shared database with order-service (which breaks "every
service owns its own Postgres database, no shared schema, ever" — an existing, load-bearing rule
in this project, not new for M3). Colocating the orchestrator with the aggregate whose lifecycle it
drives is what makes the atomicity claim provable at all, not just convenient.

## Consequences

- order-service now has two responsibilities: Order CRUD (its original M1 scope) and orchestrating
  the whole order lifecycle saga. This is a real coupling, not a free lunch — see "Revisit if" below.
- order-service becomes a Kafka consumer for the first time (`payments.events`, `inventory.events`,
  via `SagaEventListener`), on top of already being the producer of `orders.events`. It picks up the
  exact same manual-ack, dedupe-on-`(consumer_group, event_id)` discipline M2 established for every
  other consumer in this project — nothing new invented, same mechanism reused (constitution item 4).
- `payments.events`/`inventory.events` are topics order-service consumes but does not own or
  produce to; `SagaTopicsConfiguration` provisions them explicitly (see the class Javadoc) rather
  than relying on broker-side auto-creation, which this project's own test suite demonstrated is
  fragile: without it, `SagaEventListener`'s subscription raced the topics' existence and the saga
  never advanced.
- Every participant's own saga-facing writes (`AuthorizePayment`/`RefundPayment` dispatched by the
  orchestrator; `PaymentAuthorized`/`PaymentRefunded`/`InventoryReserved`/`InventoryReservationFailed`
  published by payment-service/inventory-service) all go through each service's own existing
  `OutboxWriter` → single-topic relay — no new relay infrastructure, no multi-topic routing added to
  `OutboxRelayWorker`. Every saga event just multiplexes onto whichever topic that service already
  owned, differentiated by `event_type` — the identical filtering pattern M2 already established for
  `OrderCreated`.

## Revisit if

A second, unrelated saga type needs orchestrating (e.g. a returns/refund saga not rooted at
`OrderCreated`), or the orchestration logic's size/complexity starts to dominate order-service's
own Order-CRUD concerns rather than sitting alongside them. At that point, extracting a dedicated
orchestrator module — first as a Gradle module boundary within the same deployable, before ever
considering a network boundary — is the natural next step, and this ADR's atomicity argument would
need to be re-examined for whatever new saga root event is involved.
