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

## Amendment: the transaction-spans-network-call anti-pattern, named plainly

The design above holds a Postgres transaction — row lock included — open for the entire duration
of the synchronous Kafka `send()` call. This is a known anti-pattern: a database transaction
should not span an external network call. It is retained deliberately for M1, not accidentally,
and this amendment states the honest cost/benefit rather than leaving it implied.

**What it does NOT buy: any reduction in duplicates.** It's tempting to read "the claim, publish,
and mark-published are all one atomic unit" as meaning crashes are handled more cleanly than a
split design would. They aren't. A crash after the broker acks the publish and before the
transaction commits republishes the row on the next attempt — this is true of the current
single-transaction design, and it would be exactly as true of a claim-commit / publish /
mark-commit split design with three separate transactions. The window where "the broker has the
message but we haven't durably recorded that fact yet" exists in *any* design that publishes to an
external system from inside a claim-and-mark cycle; spanning the transaction across the network
call doesn't close that window, it just also holds a lock open while the window is open. Scenario
(b) of the M1 crash-window tests
(`OutboxRelayCrashWindowIntegrationTest#aCrashAfterKafkaAckBeforeCommitRepublishesOnRestartAndTheDuplicateIsCorrect`)
demonstrates the duplicate directly; nothing about transaction span changes that outcome.

**What it actually costs:**
- **A Postgres row lock held for the duration of external I/O.** Under a slow or degraded broker,
  every relay attempt holds `outbox_events`' row lock (and the pooled JDBC connection serving it)
  for as long as the Kafka call takes — up to `kafka-send-timeout-ms` per attempt.
- **Connection pool pressure under sustained broker slowness.** With single-worker draining
  (`OutboxRelayScheduler`'s adaptive loop), a slow broker serializes the relay's entire throughput
  behind one connection held open per attempt; a struggling broker doesn't just slow down
  publishing, it starves the pool of a connection for the duration of each stalled attempt.
- **Long-running transactions block vacuum.** A transaction that stays open across a slow Kafka
  call — even a single one — holds back Postgres's vacuum horizon for as long as it's open,
  delaying cleanup of dead tuples database-wide, not just on `outbox_events` (see the T3 posture
  below, where the relay is itself now a source of that dead-tuple churn).

**Alternative considered: lease-based claiming.** Instead of holding the row lock across the
network call, a claim would mark a row with a `leased_until` timestamp (and release the actual
Postgres lock immediately), publish to Kafka outside any open transaction, then mark it published
in a separate, fast transaction referencing the lease. This decouples Postgres lock/connection
hold time from Kafka I/O time entirely. Its cost is real complexity this design avoids: a
**reclaim-timeout mechanism** is needed for a worker that crashes mid-lease without completing —
choosing a timeout that's long enough to avoid reclaiming a still-in-progress attempt but short
enough to recover promptly, handling clock skew between the value written and the value compared
against, and handling a crash *during* the reclaim itself. None of that exists in the current
design, and none of it is a small addition.

**Deferred to M7.** This tradeoff is not being decided here. M7 (performance engineering &
benchmarks) is where this gets a real answer, measured under a genuinely slow broker rather than
argued from principle — connection pool exhaustion, vacuum lag, and actual publish latency under
load are exactly the kind of numbers that decide whether lease-based claiming's added complexity
is worth paying for.
