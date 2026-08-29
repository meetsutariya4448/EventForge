# ADR-0015: Saga timeouts, bounded compensation retry, and the hard case

## Context

Constitution item 5 requires a persisted timeout source of truth, proven restart-safe. Item 6
requires an explicit answer to "compensation itself fails permanently" — terminal state, alert,
manual remediation — with "we retry" named directly as not an acceptable answer.

## Decision: persisted deadline, not an in-memory timer

`saga_instance.deadline_at` is the only source of truth for "has this step gone unanswered too
long." It is set whenever a step is dispatched (`AWAITING_PAYMENT`/`AWAITING_INVENTORY`/`AWAITING_REFUND`),
computed as `clock.instant() + <step timeout>`, and cleared (`NULL`) once the saga reaches a
terminal state. `SagaTimeoutSweeper` (`@Scheduled`) periodically asks `SagaOrchestrator` to find and
resolve every saga whose deadline has passed, evaluated against the same injected `Clock` every
other time-sensitive mechanism in this project uses (`OutboxRelayWorker`'s retry backoff, going
back to M1) — never `Instant.now()` called directly, so tests can move time forward deterministically
instead of sleeping.

Because the deadline lives in the database, not in the process that dispatched the command,
resolving a stranded saga does not depend on which process instance eventually notices — a fresh
`SagaTimeoutSweeper`/`SagaOrchestrator` pair, reading only from `saga_instance`, produces the
identical outcome a long-running process would have. `SagaTimeoutIntegrationTest` proves this
directly by calling the sweep method on demand rather than waiting on the real scheduled trigger,
exactly the way M1's crash-window tests proved the outbox relay's own restart-safety.

## Decision: bounded compensation retry, then a genuinely terminal state

`AWAITING_REFUND` timing out redispatches `RefundPayment` — but only up to
`eventforge.saga.max-compensation-attempts` (default 3, not tuned) times. The attempt after that
limit does not retry; it transitions the saga to **`COMPENSATION_FAILED`**, a state distinct from
`COMPENSATED` specifically so it is never confused with an ordinary successful cancellation.

**The hard case, stated plainly (constitution item 6):** if payment-service never answers any
`RefundPayment` attempt — a dead consumer, a permanently broken downstream, whatever the cause —
this project does not retry forever. It gives up after a bounded number of attempts, marks the
order `CANCELLATION_FAILED` (a status distinct from `CANCELLED`, queryable directly:
`SELECT * FROM orders WHERE status = 'CANCELLATION_FAILED'`), increments a Micrometer counter
(`saga.compensation.failed`) an operator's alerting would page on in a real deployment, and stops.
The manual remediation path is documented in `docs/runbooks/compensation-failure.md` — not
implemented as an automatic action, deliberately: at this point money may have been taken and not
refunded, and reversing that automatically without a human confirming the provider's actual state
is a worse failure mode than a clearly-flagged, paused saga.

`SagaTimeoutIntegrationTest#permanentCompensationFailureReachesATerminalStateWithAnAlertAndNoInfiniteLoop`
proves this end to end: exactly `max-compensation-attempts` `RefundPayment` dispatches happen (not
more — checked directly by sweeping once more afterward and confirming no 4th attempt), the
terminal state and order status are both correct, and the counter increments exactly once.

## Two bugs this milestone's own tests caught before they shipped

Both are worth naming because they are exactly the kind of trap a saga implementation invites, and
both were caught by writing the tests this ADR describes, not by inspection.

**1. `merge()` vs `persist()` silently drops mutations on new, explicitly-ID'd entities.**
`SagaInstance` (and `Payment`, `InventoryReservation`, `SagaStep`) use client-assigned UUID
primary keys, not `@GeneratedValue`. Spring Data JPA's default `save()` on such an entity treats a
non-null ID as "not new" and calls `entityManager.merge()`, which copies state onto a *different*,
newly-managed instance and returns that instance — the original object passed to `save()` stays
unmanaged. `SagaOrchestrator.startSaga` originally called `sagaInstanceRepository.save(saga)` and
then kept mutating the original `saga` reference (`allocateNextSequence()`, dispatching
`AuthorizePayment`) — a mutation Hibernate never saw, so `next_sequence` never advanced past its
initial value in the database. Every saga's `ReserveInventory` dispatch then collided with
`AuthorizePayment`'s already-claimed `aggregate_sequence`, caught immediately by
`SagaOrchestrationIntegrationTest`'s happy-path test as a `DuplicateKeyException`. Fixed by
reassigning `saga = sagaInstanceRepository.save(saga)` and using the returned reference — the
standard, easy-to-forget Spring Data JPA idiom for exactly this situation.

**2. Self-invocation silently disables `@Transactional`.** `SagaTimeoutSweeper` calls
`SagaOrchestrator.sweepTimedOutSagas()` from a different bean, correctly going through Spring's AOP
proxy — but `sweepTimedOutSagas()` itself originally called `this.handleTimeout(sagaId)` directly,
a same-object method call that never passes through the proxy at all. `@Transactional` on
`handleTimeout` was silently a no-op: each repository call inside it opened and closed its own
short-lived transaction instead of the method running as one atomic unit, which reintroduced
exactly the same stale-`next_sequence` bug above through a second door.
`SagaTimeoutIntegrationTest`'s hard-case test caught this as a second `DuplicateKeyException`, one
call into the retry loop. Fixed by demarcating the transaction programmatically
(`TransactionTemplate`, injected via `PlatformTransactionManager`) instead of relying on the
annotation for this specific call path, which has no proxy to bypass.

Both bugs are recorded here rather than only in commit history because they are structural traps
any saga implementation using explicit-ID entities and internal scheduled-task loops will hit
again if the pattern is copied without this context.

## Revisit if

`max-compensation-attempts`/the per-step timeout values are ever measured against real
payment-provider latency (M7-style measurement) rather than left as engineering defaults — none of
the numbers here are claimed to be tuned (R2).
