# Duplicate taxonomy

Every mechanism in EventForge that can cause the same logical event to be observed more than
once, downstream of where it originates, which layer is responsible for absorbing it, and the test
that proves the mechanism is real (not hypothetical). M2's idempotent-consumer tests must map onto
an entry here — if a test can't point at a row in this table, either the table is missing a
mechanism or the test isn't testing a real one.

A duplicate, for this document, means: the same `event_id` is observed more than once by something
downstream (a Kafka consumer, or a count on the topic). Every mechanism below produces this exact
shape of problem; only the layer responsible for collapsing it back to one logical effect differs.

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

| # | Mechanism | Where it originates | Absorbed by | Proven by |
|---|-----------|---------------------|-------------|-----------|
| 1 | **Producer-internal retry** (network blip, transient leader unavailability, within one `send()` call's own retry budget) | Kafka client library, inside `delivery.timeout.ms` | Nothing needs to — this is the one case `enable.idempotence=true` actually prevents from becoming a duplicate at all. | Not separately tested in this project; this is Kafka's own guarantee, verified by upstream Kafka's test suite, not ours. `ProducerIdempotenceConfigTest` only asserts the setting is actually configured (trap T2). |
| 2 | **Crash between Kafka ack and outbox commit** — the broker has accepted the message; the relay process dies (or is interrupted) before the `published_at` `UPDATE` commits. The row is retried, unmodified, on the next attempt. | `OutboxRelayWorker.relayNextEvent()`, the `AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED` seam | M2's idempotent consumers (`processed_events`, unique constraint on `(consumer_group, event_id)`) | `OutboxRelayCrashWindowIntegrationTest#aCrashAfterKafkaAckBeforeCommitRepublishesOnRestartAndTheDuplicateIsCorrect` (real fault injection, asserts the Kafka duplicate directly, same `event_id` both copies) |
| 3 | **Client-side send timeout racing actual delivery** — the relay gives up waiting on the send `Future` (treats it as failed, records the attempt), but the underlying send was not cancelled and completes successfully on the broker later; the relay's own retry then sends an independent second copy. | `OutboxRelayWorker.relayNextEvent()`'s `kafkaTemplate.send(record).get(timeout)` call | M2's idempotent consumers (same mechanism as #2 downstream) | `OutboxRelayCrashWindowIntegrationTest#ordersKeepCommittingWhileBrokerIsDownAndAllPublishOnceItRecovers` (scenario a) — observed directly during M1 addendum verification: two Kafka records, same `event_id`, ~9s apart |
| 4 | **General "recorded as failed, but the broker actually has it" ambiguity** — the umbrella case #2 and #3 are both instances of: any time "no confirmed success within our timeout" is treated as failure-and-retry, a duplicate is possible if the original attempt actually lands. This is inherent to at-least-once delivery under ambiguous outcomes, not a bug in either #2 or #3 specifically. | Same as #2/#3 | Same as #2/#3 | Same tests as #2/#3 — this row exists to name the general case explicitly, not to claim a separate test |
| 5 | **Multi-worker concurrent claim overlap** (trap T1) — two relay workers running concurrently both attempt to claim work for the same aggregate. | Would originate in `OutboxRelayWorker`'s claim query if it were unsafe under concurrency | **Nothing needs to absorb this — it does not happen.** Verified, not assumed: the head-row-only claim restriction (only an aggregate's lowest unpublished `aggregate_sequence` is ever an eligible claim target) combined with `FOR UPDATE SKIP LOCKED` means a second worker contending for a locked aggregate finds no fallback row for that aggregate and simply moves on. See ADR-0010's "T1 is solved" amendment. | `MultiWorkerRelayOrderingIntegrationTest` — 10 concurrent workers (verified up to 20 during development) against 6 aggregates, oversubscribed deliberately; 7 consecutive clean runs, zero double-claims, zero order violations |
| 6 | **Consumer redelivery on crash/rebalance** *(M2, not yet built)* — a Kafka consumer processes a message, then crashes or is rebalanced away before committing its offset; the message is redelivered to whichever consumer picks up that partition next. | Kafka consumer group protocol (manual offset ack, per the constitution's consumer transaction shape) | M2's `processed_events` insert-or-noop-on-unique-violation, in the same transaction as the business mutation and any next outbox write | Not yet built — this is the mechanism M2 exists for. Its test must insert into `processed_events` first, redeliver the same `event_id`, and assert the business mutation runs exactly once. |
| 7 | **Consumer rebalance mid-processing** *(M2, not yet built)* — a partition is reassigned to a different consumer instance while the original instance is still mid-processing (not yet committed), and both instances' in-flight work can result in the same message being processed by two consumer instances. | Kafka consumer group rebalance protocol | Same as #6 — `processed_events`'s uniqueness constraint doesn't care which instance attempted the insert first, only that only one of them wins | Not yet built — a variant of #6's test, with two consumer instances (or two threads holding separate group memberships) racing on the same message |
| 8 | **Broker-side replication/leader-failover duplicate** — under `acks=all` with `replication.factor > 1`, a leader failover mid-produce can, in rare cases, result in a message being counted as delivered twice across the old and new leader. | Kafka broker replication protocol | Not applicable to EventForge's current topology | **Not applicable today** — every topic in this project is `replication.factor=1` (single-broker KRaft, both in Testcontainers and `docker-compose.yml`). This row is recorded so it isn't rediscovered as a surprise if replication factor ever increases past 1. |

## What this means for M2

Rows 6 and 7 are M2's actual scope — everything else in this table (rows 1–5) is already handled
(by Kafka itself, by M1's relay design, or verified not to occur) before M2's consumer code ever
runs. M2's test suite should have at least one test per row 6 and row 7; if a new duplicate
mechanism is discovered during M2 that doesn't map onto either row, this table gets a new row
before the fix, not after.
