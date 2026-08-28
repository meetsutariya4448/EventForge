# ADR-0010: Outbox relay concurrency model — one-row-per-transaction, verified safe for N workers

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
double-claiming structurally impossible: there is no window where a row is claimed but not yet
locked, regardless of how many workers are running (see the "T1 is solved" amendment — this turned
out to hold under concurrency too, not just for M1's single running instance). The tradeoff —
holding a Postgres row lock for the duration of a network round-trip to Kafka — is accepted
regardless of worker count; see the transaction-spans-network-call amendment for what that costs.

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
`batchCapPerPoll`, on a fixed-delay `@Scheduled` tick. Multiple concurrent relay workers (multiple
threads or processes calling this same claim query) are now verified safe — see the "T1 is solved"
amendment below; this section originally said otherwise and has been corrected, not just appended
to.

## Consequences

- M1 runs a single relay process per service, but not because concurrent workers are unsafe (they
  aren't — see the "T1 is solved" amendment). M6's KEDA autoscaling can scale the relay's process
  count directly on that basis; no claiming redesign is a prerequisite. Actual throughput under N
  concurrent workers is still an M7 measurement question, not a correctness one.
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

Multi-worker throughput measured in M7 shows contention costs (lock waits, transaction pile-up
under N workers) that argue for lease-based claiming despite T1's correctness already holding —
see the transaction-spans-network-call amendment above, which covers that tradeoff regardless of
worker count.

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
- **Connection pool pressure under sustained broker slowness.** Every concurrent relay
  worker/thread holds its own connection open for the duration of its attempt; a struggling broker
  doesn't just slow down publishing, it holds one pooled connection hostage per in-flight attempt
  for as long as that attempt takes. This scales with worker count (see the T1 amendment below),
  which is exactly why M7 needs to measure it rather than assume it's fine.
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

## Amendment: T1 is solved — multi-worker claiming verified, not just single-worker

The ADR as originally written (and every M1 commit up to this point) said flatly that this design
"must not be run as more than one relay instance per service until T1 is actually solved," framing
aggregate-level claiming or deterministic sharding as *future* work needed before concurrent
workers would be safe. That framing was wrong, and this amendment corrects it based on direct
evidence rather than argument.

**Why it turns out to already be solved.** The head-row-only eligibility rule introduced by the
earlier amendment in this same ADR (`aggregate_sequence = (SELECT MIN(...) WHERE ... published_at
IS NULL)`, added to fix single-worker head-of-line blocking under retry) has a second effect that
wasn't the point when it was written: for any given aggregate, there is only ever **one** row that
can match the claim query's `WHERE` clause at a time — the current head. Combined with
`FOR UPDATE SKIP LOCKED`, this means a second worker's claim query, run concurrently against the
same aggregate, has no *other* row for that aggregate to fall back to when the head is locked — it
simply finds nothing for that aggregate and moves on to a different one (or returns
`NOTHING_TO_CLAIM`). A later sequence number for the same aggregate can never become eligible until
the head publishes and is removed from consideration. This is, in effect, exactly the
"aggregate-level claiming" ADR-0004 and this ADR both said would eventually be needed — it was
already implied by the head-row restriction, just not recognized as solving T1 at the time.

**How it was verified, not just argued.** `MultiWorkerRelayOrderingIntegrationTest` runs
`WORKER_COUNT` threads (10 in the committed test, verified up to 20 during development) all calling
the same `OutboxRelayWorker.relayNextEvent()` concurrently — a valid stand-in for N separate relay
processes, since the bean is stateless and each `@Transactional` call gets its own transaction
regardless of which thread invokes it — against 6 aggregates with 8 sequenced events each,
deliberately oversubscribed (more workers than aggregates) to force real contention on
`FOR UPDATE SKIP LOCKED` rather than happening to avoid it. Run repeatedly (7 consecutive clean
runs across both worker counts during verification): every event published exactly once, and every
aggregate's Kafka records arrived in strict sequence order despite concurrent, interleaved
processing across workers.

**What this does NOT cover.** Verified: correctness (no double-claim, no order violation) under
concurrent workers. Not verified: throughput/contention cost at scale — how claim latency degrades
as worker count or backlog size grows, or whether row-lock contention on hot aggregates becomes a
bottleneck under real load. That's M7's job, same as the transaction-spans-network-call tradeoff
above; T1 being *correct* under concurrency doesn't mean it's *efficient* under concurrency, and
this amendment makes no claim about the latter.

**No implementation change accompanies this amendment.** The claiming SQL is unchanged from the
previous amendment (which was written to fix head-of-line blocking, not multi-worker safety) —
this amendment documents a property that design already had, verified by a new test, not a new
design.

## Revisit if (T1 specifically)

M7's throughput measurements under real concurrent load show contention costs that argue lease-based
claiming is worth its reclaim-timeout complexity despite T1's correctness already holding without
it — or a future milestone needs claiming semantics this head-row restriction doesn't provide (e.g.
processing multiple aggregates' events in a single transaction for throughput, which would
reintroduce the batch-rollback tradeoff the one-row-per-transaction decision above was written to
avoid).
