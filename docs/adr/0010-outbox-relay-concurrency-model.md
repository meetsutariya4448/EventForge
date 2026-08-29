# ADR-0010: Outbox relay concurrency model

## Context

The relay claims unpublished `outbox_events` rows, publishes them to Kafka, and marks them
published. Two correctness properties matter: no row is ever claimed by two workers at once
(double-publish), and per-aggregate publish order is preserved (trap T1) even under retries and
concurrent workers. A third property — efficient use of Postgres connections and locks under a
slow or degraded broker — is a real cost of the design below, deliberately accepted for now and
explicitly deferred to M7 to measure rather than guess at.

## Decision

**Claim, publish, and mark-published happen inside one `@Transactional` method
(`OutboxRelayWorker.relayNextEvent()`), one row at a time.** The claim query:

```sql
SELECT event_id, aggregate_id, event_type, schema_version, correlation_id,
       causation_id, traceparent, tracestate, payload::text AS payload_text, occurred_at
FROM outbox_events o
WHERE published_at IS NULL
  AND (last_attempt_at IS NULL OR last_attempt_at <= ?)
  AND aggregate_sequence = (
      SELECT MIN(o2.aggregate_sequence)
      FROM outbox_events o2
      WHERE o2.aggregate_id = o.aggregate_id AND o2.published_at IS NULL
  )
ORDER BY last_attempt_at ASC NULLS FIRST, aggregate_id, aggregate_sequence
FOR UPDATE SKIP LOCKED
LIMIT 1
```

Three properties of this query, each load-bearing:

- **`FOR UPDATE SKIP LOCKED` + one row per transaction** makes double-claiming structurally
  impossible: there is no window where a row is claimed but not yet locked, and the lock is held
  for the claim → Kafka publish → mark-published sequence, released only on commit or rollback.
- **The head-row restriction** (`aggregate_sequence = MIN(... WHERE published_at IS NULL)`) means
  only an aggregate's lowest unpublished sequence number is ever an eligible claim target. A later
  sequence number for the same aggregate can never be claimed while an earlier one is still
  pending — by any worker, concurrently or not. This is what makes the design safe for **N
  concurrent relay workers**, not just one: a second worker contending for a locked aggregate has
  no fallback row for that aggregate to fall through to, so it simply finds nothing there and
  moves to a different aggregate (or gets `NOTHING_TO_CLAIM`). Verified directly by
  `MultiWorkerRelayOrderingIntegrationTest` — 10 concurrent threads (stress-verified to 20) against
  6 oversubscribed aggregates, real `FOR UPDATE SKIP LOCKED` contention, zero double-claims and
  zero order violations across 7 consecutive runs; a negative control (reverting to naive
  `ORDER BY aggregate_id, aggregate_sequence` with no head-row restriction) reproduced a genuine
  per-aggregate order violation, confirming the test actually detects the property it claims to
  verify.
- **Ordering candidates by `last_attempt_at ASC NULLS FIRST`** (oldest/never-attempted first,
  rather than by `aggregate_id`) prevents head-of-line blocking under retry: without it, a single
  persistently-failing aggregate would keep re-qualifying for reclaim (its `last_attempt_at` is
  always the most recent) and starve every other aggregate in the table.

**Batch size is one row per transaction, not N.** A batch transaction that partially succeeds (row
1 publishes to Kafka, row 2 throws) would roll back the whole batch on exception — including row
1's `published_at` update — even though row 1's Kafka message was already sent and can't be
un-sent. One row per transaction keeps a failure's blast radius to exactly the row that failed.

**The transaction spans the Kafka network call.** This is a known anti-pattern — a database
transaction should not span an external network call — retained deliberately, not accidentally.
It buys **no reduction in duplicates**: a crash after the broker acks the publish and before the
transaction commits republishes the row on the next attempt regardless of whether the transaction
spans the network call or not (proven by
`OutboxRelayCrashWindowIntegrationTest#aCrashAfterKafkaAckBeforeCommitRepublishesOnRestartAndTheDuplicateIsCorrect`).
What it actually costs: a Postgres row lock and pooled JDBC connection held for the duration of
each attempt (up to `kafka-send-timeout-ms`), connection-pool pressure that scales with worker
count under a slow broker, and a long-running transaction holding back Postgres's vacuum horizon
for as long as it's open (feeding the dead-tuple churn `outbox_events` already generates — see
ADR-0004's T3 posture).

**Alternative considered, not implemented: lease-based claiming.** Mark a row with a
`leased_until` timestamp and release the Postgres lock immediately, publish to Kafka outside any
open transaction, then mark it published in a separate fast transaction referencing the lease.
This would decouple lock/connection hold time from Kafka I/O entirely, at the cost of a genuine
reclaim-timeout mechanism for a worker that crashes mid-lease: choosing a timeout long enough to
avoid reclaiming a still-in-progress attempt but short enough to recover promptly, handling clock
skew between the value written and the value compared against, and handling a crash *during* the
reclaim itself. None of that exists today, and none of it is a small addition.

## Consequences

- Safe to run as multiple concurrent relay workers/processes today — no claiming redesign is a
  prerequisite for M6's KEDA autoscaling to scale the relay's process count directly.
- Correctness under concurrency is verified; **efficiency under concurrency is not.** How claim
  latency degrades as worker count or backlog size grows, and whether row-lock contention on hot
  aggregates becomes a bottleneck under real load, are open questions M7 measures, not arguments
  settled here.
- A crash between Kafka publish and the mark-published `UPDATE` (the
  `AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED` fault-injection point) leaves the row unpublished,
  and the same event is republished on the next attempt — expected at-least-once behavior, not a
  bug, and exactly the case M2's idempotent consumers exist to absorb (see
  `docs/duplicate-taxonomy.md`).

## Revisit if

M7's throughput measurements under real concurrent load and a genuinely slow broker show
connection-pool exhaustion, vacuum lag, or publish-latency costs that argue lease-based claiming is
worth its reclaim-timeout complexity — correctness alone doesn't settle that tradeoff, only
measurement does.

## Changelog

- **2026-08-27** — Initial decision: one-row-per-transaction claiming, `ORDER BY aggregate_id,
  aggregate_sequence`. Stated that this "must not run as more than one relay instance until T1 is
  actually solved" — this constraint was wrong, see below.
- **2026-08-27** — M1 crash-window testing found a real head-of-line-blocking bug: under retry, a
  persistently-failing aggregate kept getting reclaimed every cycle (its `last_attempt_at` always
  looked most recent), starving every other aggregate. Added the head-row-only eligibility
  restriction and `last_attempt_at`-first ordering to fix it.
- **2026-08-27** — `MultiWorkerRelayOrderingIntegrationTest` (concurrent workers, oversubscribed
  aggregates, plus a negative control confirming the test genuinely detects order violations)
  showed the head-row restriction added in the previous revision already provides the
  per-aggregate serialization T1 needs — it was fixing an unrelated single-worker retry bug and
  happened to solve multi-worker safety too, which nobody had recognized when it was written. The
  original "must not run more than one instance" constraint was therefore wrong: it assumed
  aggregate-level claiming would need to be purpose-built later, when a fix already in place for a
  different reason had already provided it. This document was rewritten from three layers of
  amendment into the single current decision above, rather than left as a history a reader had to
  reconstruct.
