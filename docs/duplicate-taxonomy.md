# Duplicate taxonomy

Every mechanism in EventForge that can cause the same logical event to be observed more than
once, downstream of where it originates, which layer is responsible for absorbing it, and the test
that proves the mechanism is real (not hypothetical). M2's idempotent-consumer tests must map onto
an entry here — if a test can't point at a row in this table, either the table is missing a
mechanism or the test isn't testing a real one.

A duplicate, for this document, means: the same `event_id` is observed more than once by something
downstream (a Kafka consumer, or a count on the topic). Every mechanism below produces this exact
shape of problem; only the layer responsible for collapsing it back to one logical effect differs.

**Status column.** Every row is one of:
- **Proven** — a test in this repo demonstrates the mechanism and its absorption, referenced by name.
- **Predicted** — the mechanism is identified and its absorbing layer is designed for, but no test
  in this repo exercises it yet. Expected to flip to Proven when the milestone that builds the
  absorbing layer lands, with the test reference filled in at that point — not before.
- **N/A** — does not apply to EventForge's current configuration; recorded so it isn't rediscovered
  as a surprise if that configuration changes.

This document is a completion check for whichever milestone is current, not just a reference: if a
row relevant to that milestone's scope is still **Predicted** when the milestone's work ends, the
milestone's stop-and-report must say so explicitly rather than let the table's silence stand in for
"done."

## Why `enable.idempotence=true` does not cover most of this document

It's tempting to read "the producer is idempotent" as "publishing is exactly-once." It isn't, and
the reason matters for reading the rest of this document correctly.

Kafka's idempotent producer works by requesting a Producer ID (PID) from the broker, then tagging
every message the producer sends with `(PID, partition, sequence number)`. The broker remembers the
last sequence numbers it accepted per `(PID, partition)` and rejects/dedupes a message that's a
**retry of an already-accepted sequence number**. This protects exactly one thing: the producer
client's own internal retry loop for **one logical `send()` call** — network blips, a leader
election mid-request, a broker briefly returning `NOT_LEADER_FOR_PARTITION` — where the client
resends the *same* request and the broker recognizes it as a duplicate of a sequence number it
already has.

It does **not** protect against:

- **A new producer session.** A restarted process (crash, redeploy) gets a **new PID** by default —
  we don't configure `transactional.id`, so there's no session to resume. The broker has no way to
  know "this new producer's send is a duplicate of an old producer's already-accepted send"; PID
  scoping is per-session by design, not durable across restarts.
- **The application deciding to send again.** When `OutboxRelayWorker` treats an attempt as failed
  and later calls `kafkaTemplate.send()` again for what it believes is the same logical event, that
  is — from the Kafka client's perspective — a **brand new, independent send request**, which gets
  the *next* sequence number in the producer's local counter, not a retry of the old one. Nothing in
  the producer protocol correlates "this new message" with "the one I gave up on earlier"; they're
  two unrelated messages that happen to carry the same JSON payload and `event_id`.

Both of the mechanisms below that matter most in M1 — relay-restart duplication and
application-level retry duplication — are exactly this second case: our own code, not Kafka's
retry machinery, is the thing issuing the second send. Idempotence has nothing to dedupe there
because, as far as the broker can tell, no retry happened at all — just two ordinary, independent
publishes.

## The mechanisms

| # | Mechanism | Where it originates | Absorbed by | Status | Proven by / expected proof |
|---|-----------|---------------------|-------------|--------|------------------------------|
| 1 | **Producer-internal retry** (network blip, transient leader unavailability, within one `send()` call's own retry budget) | Kafka client library, inside `delivery.timeout.ms` | Nothing needs to — this is the one case `enable.idempotence=true` actually prevents from becoming a duplicate at all. | Proven (upstream) | Not separately tested in this project; this is Kafka's own guarantee, verified by upstream Kafka's own test suite, not ours. `ProducerIdempotenceConfigTest` only asserts the setting is actually configured (trap T2). |
| 2 | **Crash between Kafka ack and outbox commit** — the broker has accepted the message; the relay process dies (or is interrupted) before the `published_at` `UPDATE` commits. The row is retried, unmodified, on the next attempt. | `OutboxRelayWorker.relayNextEvent()`, the `AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED` seam | M2's idempotent consumers (`processed_events`, unique constraint on `(consumer_group, event_id)`) | Proven | `OutboxRelayCrashWindowIntegrationTest#aCrashAfterKafkaAckBeforeCommitRepublishesOnRestartAndTheDuplicateIsCorrect` (real fault injection, asserts the Kafka duplicate directly, same `event_id` both copies) |
| 3 | **Client-side send timeout racing actual delivery** — the relay gives up waiting on the send `Future` (treats it as failed, records the attempt), but the underlying send was not cancelled and completes successfully on the broker later; the relay's own retry then sends an independent second copy. | `OutboxRelayWorker.relayNextEvent()`'s `kafkaTemplate.send(record).get(timeout)` call | M2's idempotent consumers (same mechanism as #2 downstream) | Proven | `OutboxRelayCrashWindowIntegrationTest#ordersKeepCommittingWhileBrokerIsDownAndAllPublishOnceItRecovers` (scenario a) — observed directly during M1 addendum verification: two Kafka records, same `event_id`, ~9s apart |
| 4 | **General "recorded as failed, but the broker actually has it" ambiguity** — the umbrella case #2 and #3 are both instances of: any time "no confirmed success within our timeout" is treated as failure-and-retry, a duplicate is possible if the original attempt actually lands. This is inherent to at-least-once delivery under ambiguous outcomes, not a bug in either #2 or #3 specifically. | Same as #2/#3 | Same as #2/#3 | Proven | Same tests as #2/#3 — this row exists to name the general case explicitly, not to claim a separate test |
| 5 | **Multi-worker concurrent claim overlap** (trap T1) — two relay workers running concurrently both attempt to claim work for the same aggregate. | Would originate in `OutboxRelayWorker`'s claim query if it were unsafe under concurrency | **Nothing needs to absorb this — it does not happen.** Verified, not assumed: the head-row-only claim restriction (only an aggregate's lowest unpublished `aggregate_sequence` is ever an eligible claim target) combined with `FOR UPDATE SKIP LOCKED` means a second worker contending for a locked aggregate finds no fallback row for that aggregate and simply moves on. See ADR-0010's "T1 is solved" amendment. | Proven | `MultiWorkerRelayOrderingIntegrationTest` — 10 concurrent workers (verified up to 20 during development) against 6 aggregates, oversubscribed deliberately; 7 consecutive clean runs, zero double-claims, zero order violations. Negative control run separately: reverting the claim query to naive `ORDER BY aggregate_id, aggregate_sequence` (no head-row restriction) made the same test fail immediately with a genuine per-aggregate order violation (`[2, 1, 4, 3, ...]` instead of `[1, 2, 3, 4, ...]`) — confirming the test actually detects the bug it exists to catch, not just passing by construction. The naive query was never committed. |
| 6 | **Consumer redelivery on crash/rebalance** — a Kafka consumer processes a message, then crashes or is rebalanced away before committing its offset; the message is redelivered to whichever consumer picks up that partition next. | Kafka consumer group protocol (manual offset ack, per the constitution's consumer transaction shape) | `processed_events` insert-or-noop-on-unique-violation (via `ON CONFLICT DO NOTHING`, not a caught exception — see ADR-0012), in the same transaction as the business mutation and any next outbox write | **Proven** | `CrashAfterCommitBeforeAckIntegrationTest` (real fault injection at `AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK`, the exact M0 seam this mechanism needs); `DuplicateDeliveryIntegrationTest` (100 redeliveries, one effect, 100 acks); `RelayProducedDuplicateEndToEndTest` (a *real* relay-produced Kafka duplicate — see row 3 — consumed end-to-end by the real listener) |
| 7 | **Concurrent redelivery** (a generalization of "rebalance mid-processing": two consumer paths racing to process the same event at once, whatever triggers the race) | Kafka consumer group rebalance protocol, or any other source of concurrent delivery of the same record | Same as #6 — `processed_events`' uniqueness constraint (enforced by Postgres itself under real contention, not by application-level locking) doesn't care which caller attempted the insert first, only that exactly one of them wins | **Proven** | `ConcurrentDuplicateDeliveryIntegrationTest` — 16 threads released simultaneously via a `CountDownLatch` against the same event, real database contention, exactly one business effect. This proves the property row 7 actually depends on (the dedupe insert is safe under real concurrent contention, not just sequential retries) directly, by concurrent invocation rather than by literally triggering a Kafka partition rebalance — a rebalance-triggered variant (two live consumer group members actually racing via a real rebalance) was not built; stated here rather than left implicit, since the mechanism proven is a deliberately narrower stand-in for the one named. |
| 8 | **Broker-side replication/leader-failover duplicate** — under `acks=all` with `replication.factor > 1`, a leader failover mid-produce can, in rare cases, result in a message being counted as delivered twice across the old and new leader. | Kafka broker replication protocol | Not applicable to EventForge's current topology | N/A | Every topic in this project is `replication.factor=1` (single-broker KRaft, both in Testcontainers and `docker-compose.yml`). Recorded so it isn't rediscovered as a surprise if replication factor ever increases past 1. |
| 9 | **Dedupe protection itself expiring** — not a new way to *produce* a duplicate, but a way for an *existing* duplicate (any of rows 2/3/6/7) to slip past the layer that's supposed to absorb it: if the relevant `(consumer_group, event_id)` row in `processed_events` has ever been purged, a redelivered/re-relayed copy of that event is treated as first-time and reprocessed for real. | Any future `processed_events` archival/retention mechanism (none exists yet — see ADR-0012 and ADR-0004's T3 posture) | For `payment-service`/`inventory-service`: a second, local `UNIQUE (order_id)` constraint on the business table itself — degraded to a loud DB conflict, not silent. For `notification-service`: **nothing** — deliberately, so the gap is visible rather than papered over (see `SentNotification`'s Javadoc). | **Predicted** | No retention policy is implemented yet, so this can't be triggered without simulating row deletion directly — not done this milestone. ADR-0012 states the failure mode precisely without a test forcing it; a real test needs a real retention implementation to test against first. |
| 10 | **Orchestrator command redispatch after timeout** (M3) — a genuinely *different* mechanism from rows 2/3/6/7: the saga's timeout sweep redispatches `RefundPayment` under a brand-new `event_id` when the original attempt's response never arrived in time. This is not a redelivery of the same message — event-level dedupe (row 6's mechanism) cannot see it at all, since the `event_id` is different every time. | `SagaOrchestrator`'s timeout sweep (`handleRefundPaymentTimeout`, ADR-0015) | The business-level safety net (`payment-service` checking `Payment.status` before refunding — layer 2 of ADR-0016's three-layer design) | **Predicted** | No test in this repo redispatches a command and then lets the ORIGINAL attempt also succeed — every required M3 test scenario either never redispatches, or redispatches against a dead consumer that never processes any attempt (see ADR-0016's "Consequences" section, which states this same gap in its own words). The production code path exists and is reasoned through; the redispatch-lands-on-an-already-answered-order interleaving specifically is not exercised by a test. |
| 11 | **Saga fact arriving out of state** — a fact (e.g. `PaymentAuthorized`, `PaymentRefunded`) arrives when the saga is no longer in the state that fact would normally advance — for instance, a second answer to a redispatched command, after the first answer already moved the saga on. A *different* `event_id` each time, same as row 10, so event-level dedupe alone cannot catch it. | Any saga fact, whenever more than one arrives for a step the saga has already left | `SagaOrchestrator`'s per-handler state guard (every `handleX` method checks the saga is in the exact state it expects before acting — layer 3 of ADR-0016) | **Proven** | `SagaOrchestrationIntegrationTest#aFactArrivingOutOfStateIsACleanNoOpNotADoubleTransition` — a second `PaymentAuthorized` fact for an order already past `AWAITING_PAYMENT` is ignored: state unchanged, no second `ReserveInventory` dispatch. |
| 12 | **Operator-triggered replay** (v2 WS3) — an operator republishes a captured failure from `failed_messages`. Unlike every row above, this duplicate is deliberate and human-initiated. Two sub-shapes: the same message published twice (a crash between the broker's acknowledgement and the row being marked `REPLAYED` leaves it replayable again), and a replay arriving long after the saga it belongs to has moved on. | `FailedMessageReplayer.replay`, invoked from `POST /failed-messages/{id}/replay` | Both existing layers, unchanged, because the payload is republished byte for byte and therefore carries its original `event_id`: `processed_events` absorbs the repeated publish (row 6's mechanism), and `SagaOrchestrator`'s per-handler state guard absorbs the stale arrival (row 11's mechanism). The replayer's own `FOR UPDATE SKIP LOCKED` claim is a third, earlier layer that stops two operators publishing it concurrently in the first place. | **Proven** | `ReplayTwiceIsIdempotentIntegrationTest` — the claim refuses a second replay, and a forced duplicate publish still produces exactly one payment and one `processed_events` row; `StaleReplayAgainstAdvancedSagaIntegrationTest` — a replayed `PaymentAuthorized` whose saga is already past `AWAITING_PAYMENT` changes nothing and dispatches no second `ReserveInventory`. |

## Completion check — as of the end of M4, the project's final milestone

EventForge is feature-frozen at M4; this table's state below is final, not a snapshot mid-way to
Proven.

**Rows 1–8 and 11 are Proven or N/A. Rows 9 and 10 stay Predicted, permanently — no future
milestone remains to move them, and that's stated here plainly rather than left to be noticed.**
Row 9 ("dedupe protection itself expiring") depends on a `processed_events` retention/archival
implementation that was never built (T3 was never closed) — unchanged from M2, and will stay that
way. Row 10 ("orchestrator command redispatch after timeout") is a real mechanism M3's own
compensation design surfaces (ADR-0016 names it directly), reasoned through and handled in
production code, but not exercised by a test — every required scenario either never redispatches or
redispatches against a permanently-dead consumer, neither of which reaches the specific "redispatch,
then the original attempt ALSO lands" interleaving. Both rows are honest, permanent gaps in a
finished project, not silently-dropped scope, and neither status changed between M3 and M4.

**M4 added distributed tracing, not a new way to produce a duplicate.** OpenTelemetry spans and the
OTLP export to Jaeger are observability instrumentation layered onto the existing write/relay/consume
paths — they don't introduce a new code path capable of causing the same `event_id` to be observed
twice, so no row in this table changed, and no row needed adding. This is stated explicitly rather
than left for a reader to have to confirm for themselves by re-deriving it.

Row 7 is marked Proven, but with a stated caveat in its own cell: the test that proves it exercises
concurrent contention directly rather than a literally-triggered Kafka rebalance. That distinction
is recorded there rather than smoothed over, consistent with this document's purpose.

## What M3 actually built

- The saga's own two mechanisms (rows 10-11) sit ON TOP of M2's dedupe primitive, not as a
  replacement for it — every saga command/fact still goes through the same `processed_events`
  event-level check first (row 6's mechanism, unchanged). Rows 10-11 exist because a *retried*
  command carries a genuinely different `event_id`, a shape M2's dedupe was never meant to catch
  and ADR-0016 names as a second, independent kind of duplicate.
- `ConcurrentInventoryReservationIntegrationTest` (inventory-service) proves real row-locked
  contention distinct from anything in this table: not a duplicate at all, but concurrent legitimate
  requests correctly serialized against scarce shared state (constitution item 8f) — recorded here
  for completeness even though it isn't a taxonomy row, since it's the other kind of concurrency
  claim M3 makes.

## What M2 actually built

- `payment-service` and `inventory-service` each run the full consumer transaction constitution
  Part 2 specifies: dedupe insert, business mutation, next outbox write, one transaction, manual ack
  only after commit. `notification-service` runs the same shape with an external, non-transactional
  effect instead of a local one — see ADR-0012 and ADR-0013 for what that changes.
- `notification-service`'s justification (constitution item 6, "if it cannot earn that, say so")
  was tested directly, not assumed: `IndependentConsumerGroupOffsetsIntegrationTest` proves two
  consumer groups on the same topic hold entirely independent offset positions, that resetting and
  replaying one group's offsets does not move the other's, and that replay does not produce
  duplicate notifications (dedupe absorbs it) — the service earned its place in the topology.
