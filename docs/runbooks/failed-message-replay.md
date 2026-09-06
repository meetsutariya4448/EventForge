# Runbook: captured failed messages, and replaying one

v2 WS3: what an operator does with a record that exhausted its retries. See ADR-0022 for the design
this runbook is the operational half of.

## What a captured failure means

A consumer received a record, retried it `eventforge.consumer.resilience.max-retries` times, and
still could not process it. Before the consumer offset was allowed to move past that record, a row
was written to that service's `failed_messages` table. The partition then kept moving.

**What is true when you see one of these rows:**

- The record's business transaction **did not commit**. Its `processed_events` row rolled back with
  it, so the event is genuinely unprocessed — not half-applied.
- The original message bytes are stored **verbatim**, including the `traceparent` header. A replay
  republishes exactly those bytes, so it carries the original `eventId` into the consumer-side
  dedupe and continues the original trace.
- The partition is **not** stalled. Records behind this one were processed normally.

**What is not true:**

- Nothing retried it in the background. The row sits until someone acts.
- A row's existence does not mean the message is still *worth* replaying — see "Deciding whether to
  replay" below.

## Finding them

Each service owns its own table; there is no cross-service view, because no service may read
another's database.

```sql
-- On the affected service's database.
SELECT failed_message_id, consumer_group, topic, partition_id, record_offset,
       event_type, status, captured_at, replay_attempts, last_replay_error
FROM failed_messages
WHERE status <> 'REPLAYED'
ORDER BY captured_at DESC;

-- Why one of them failed, and what it actually contained.
SELECT failure_reason, payload, headers
FROM failed_messages WHERE failed_message_id = '<id>';
```

Over HTTP (any authenticated user, `VIEWER` included):

```
GET /failed-messages           # outstanding only, newest first
GET /failed-messages?openOnly=false
GET /failed-messages/{id}
```

## Deciding whether to replay

Read `failure_reason` first and sort the row into one of three cases.

**1. A transient dependency failure** — the database was unreachable, a downstream call timed out.
Replay once the dependency is healthy. This is the case replay exists for.

**2. A poison record** — `failure_reason` names a parse error, and `event_id` is `NULL` because the
payload was never a valid envelope. Replaying it will fail again in exactly the same way and
produce a second captured row at a new offset. Do not replay. Mark it abandoned:

```sql
UPDATE failed_messages SET status = 'ABANDONED' WHERE failed_message_id = '<id>';
```

**3. A stale fact whose saga has moved on.** Safe to replay, and it will do nothing: every saga
handler refuses to act unless the saga is in the state that fact belongs to (ADR-0016), so the
replay is absorbed as a logged no-op — asserted by
`StaleReplayAgainstAdvancedSagaIntegrationTest`. Worth knowing so a replay that "did nothing" is
not mistaken for a broken replay. Check first if you want to know in advance:

```sql
-- On order-service's database, for a saga fact.
SELECT state FROM saga_instance WHERE order_id = '<aggregate_id from the payload>';
```

## Replaying

```
POST /failed-messages/{id}/replay
```

`OPERATOR` role required — a `VIEWER` gets `403`. Enforced server-side, not by the console choosing
which buttons to draw (`FailedMessageApiIntegrationTest`).

Responses:

| Status | Meaning | What to do |
|---|---|---|
| `202` | Published to the original topic | Verify the effect landed (below) |
| `409` | Already replayed, abandoned, unknown, or being claimed by someone else right now | Re-read the row; do not retry blindly |
| `403` | Authenticated, but not an `OPERATOR` | Nothing — this is working as intended |
| `502` | The broker did not accept it | Row stays eligible; check broker health, then retry |

Verify the effect rather than trusting the `202` — the message was published, which is not the same
as processed:

```sql
-- On the consuming service's database. One row means the replay was processed exactly once.
SELECT count(*) FROM processed_events
WHERE consumer_group = '<consumer_group from the failed_messages row>'
  AND event_id = '<event_id from the failed_messages row>';
```

## Replaying twice

Safe, at two independent layers, and neither is "exactly-once delivery":

1. The claim (`FOR UPDATE SKIP LOCKED` plus a status check) means two operators clicking replay
   simultaneously produce one publish and one `409`.
2. If a second publish does happen anyway — a crash between the broker's acknowledgement and the
   row being marked leaves the row replayable — the republished bytes carry the original `eventId`,
   so the `(consumer_group, event_id)` ledger absorbs it (ADR-0012).

Delivery is at-least-once. The effect is once because of the dedupe. Both layers are asserted by
`ReplayTwiceIsIdempotentIntegrationTest`.

## When capture itself fails

If the `failed_messages` write fails — the database is down — the exception propagates out of the
recoverer and **the offset is not committed**. The partition stalls on that record and it is
redelivered until the store is reachable again, at which point it is captured and the partition
resumes on its own with no restart.

That is deliberate: stalling loudly is the correct failure direction, because the alternative is
skipping a record with nothing left to remember it. What you observe is a partition that stops
advancing and a repeating ERROR in the consumer's logs. Fix the database; nothing else is required.
Measured, not assumed — `RecovererFailureLeavesOffsetUncommittedIntegrationTest` (offset does not
advance) and `FailureStoreUnavailableIntegrationTest` (and it recovers by itself).

## Turning capture off

`eventforge.failure-capture.enabled=false` restores the previous behaviour exactly: a failed record
is logged and skipped, and nothing durable records it. There is no reason to do this in normal
operation.
