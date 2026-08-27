# ADR-0010: Outbox relay concurrency model — single-worker, one-row-per-transaction

## Context

M1 needs a working relay: claim unpublished outbox rows, publish them to Kafka, mark them
published. Trap T1 (constitution Part 4) already flags that naive `FOR UPDATE SKIP LOCKED` row
claiming lets two *parallel* relay workers publish two events for the same aggregate out of
creation order — but T1 also says this is "decided in later milestones." M1 has to ship a relay
that is correct *as specified*, without either solving T1 early or writing something that
silently breaks the moment a second worker exists.

## Options considered

**Claim a batch, publish and mark-published in separate transactions.** Better throughput (locks
aren't held across Kafka I/O), but the claim and the eventual publish are two different
transactions — a second poll cycle (or, if ever run, a second worker) could re-claim the same row
in the gap between them, since releasing the claim transaction's lock before publishing removes
the only thing preventing a double-claim.

**Chosen: claim, publish, and mark-published all inside one transaction, one row at a time.** The
`FOR UPDATE SKIP LOCKED` row lock is held for the entire duration of the claim → Kafka publish →
mark-published sequence, released only on commit (success) or rollback (failure). This makes
double-claiming structurally impossible for as long as this stays single-worker: there is no
window where a row is claimed but not yet locked. The tradeoff — holding a Postgres row lock for
the duration of a network round-trip to Kafka — is accepted because M1 runs exactly one relay
instance, so there is no contention to pay for.

**Batch size: one row per transaction, not N.** A batch transaction that partially succeeds (row 1
publishes to Kafka, row 2 throws) would roll back the *whole* batch on exception — including row
1's `published_at` update — even though row 1's Kafka message was already sent and can't be
un-sent. That's not a bug (at-least-once delivery already expects duplicates, which M2's
idempotent consumers exist to absorb), but it needlessly widens the blast radius of one failure to
several rows for no benefit at single-worker scale. One row per transaction keeps failure
isolated to exactly the row that failed.

## Decision

`OutboxRelayWorker.relayNextEvent()` (in `common-events`, reused by any service that enables the
relay) claims exactly one row, publishes it, and marks it published — all inside one
`@Transactional` method. `OutboxRelayScheduler` calls it in a loop, bounded by
`batchCapPerPoll`, on a fixed-delay `@Scheduled` tick. **This must not be run as more than one
relay instance per service until T1 is actually solved** (aggregate-level claiming or deterministic
sharding by `aggregate_id`) — the schema from ADR-0004 already supports that future redesign
(the `(aggregate_id, aggregate_sequence)` ordering and partial index don't need to change), but the
current claiming logic does not implement it.

## Consequences

- Single point of publish per service in M1 — no horizontal scaling of the relay yet. M6 (KEDA
  autoscaling) will need to revisit this directly, since autoscaling the relay itself is exactly
  the scenario T1 warns about.
- A crash between Kafka publish and the mark-published `UPDATE` (the
  `AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED` fault-injection point, now wired to this real call
  site) leaves the row unpublished, and the same event gets republished on the next poll —
  proven directly by `OutboxAndRelayIntegrationTest#aCrashAfterKafkaPublishBeforeMarkPublishedLeavesTheRowUnpublished`.
  This is expected at-least-once behavior, not a bug to fix here.
- Kafka publish is synchronous (`kafkaTemplate.send(record).get(10, TimeUnit.SECONDS)`) inside the
  transaction, so the relay's throughput is bounded by Kafka round-trip latency times one row at a
  time. Acceptable at M1/portfolio scale; a future milestone's performance work (M7) would be where
  this becomes a number worth optimizing against, not before.

## Revisit if

T1 needs solving for real (multiple relay workers, or KEDA-driven relay scaling in M6) — at that
point this ADR's single-worker assumption becomes the thing to redesign, using the aggregate-level
claiming the schema was already built to support.
