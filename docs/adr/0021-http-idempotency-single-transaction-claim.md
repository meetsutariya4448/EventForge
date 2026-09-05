# ADR-0021: HTTP idempotency — a single-transaction claim, and what its blocking costs

## Context

v2 WS1. `POST /orders` had no idempotency: a client whose socket timed out mid-request had no
safe way to retry, because a retry created a second order. The saga, the outbox and the consumer
dedupe layer all protect events already inside the system; nothing protected the boundary where
requests enter it.

The requirement is stronger than "a repeated request is ignored": **the same request issued
concurrently must create exactly one order**. Sequential retry handling is easy; the simultaneous
case, where every attempt's claim lands inside every other attempt's still-open transaction, is
where designs fail.

## Decision: the claim is the first statement of the creating transaction

`OrderService.createOrderIdempotent` runs, in one `@Transactional`:

1. `SET LOCAL lock_timeout`
2. render the response (id and amount are known before any write)
3. `INSERT INTO idempotency_keys … ON CONFLICT (idempotency_key) DO NOTHING`
4. **0 rows** → read the committed row, compare fingerprints, replay it; no business work runs
5. **1 row** → perform the order write and return

The claim and the effect therefore commit together, or neither does. This is the outbox pattern's
own argument — a durable record and the thing it describes sharing one transaction — applied to
an inbound request instead of an outbound event, and it reuses the `ON CONFLICT DO NOTHING`
primitive `ProcessedEventStore` already proves correct under real contention (ADR-0012).

### Why there is no `IN_PROGRESS` state

The rejected alternative was a separate short transaction committing an `IN_PROGRESS` claim
first. It needs a state column, and it needs a reaper for claims whose owner crashed between
claiming and finishing — an orphaned claim otherwise blocks the very retry it exists to enable.

The single-transaction form has no such state to orphan. Under a race, Postgres blocks the second
inserter on the winner's speculative insertion lock until the winner resolves:

- winner commits → the loser's insert reports zero rows, and its next statement (a fresh READ
  COMMITTED snapshot) is guaranteed to see the committed row;
- winner aborts → the loser's insert succeeds and it legitimately becomes the winner.

There is no third outcome. `rows == 0` strictly implies a committed row exists, which is why the
lookup that follows can never come back empty and why the table needs no stuck-claim reaper —
only one for expired keys.

## Consequence: blocking is not free, and this is the trade-off

A loser does not get an immediate answer. It **blocks**, holding a servlet thread and a pooled
JDBC connection, for as long as the winner's transaction takes or until `lock_timeout` fires.

Under a retry storm — precisely the condition idempotency exists to survive — N simultaneous
duplicates of one key means N−1 blocked requests each pinning a connection. On a small pool that
is a plausible exhaustion path, and it would arrive exactly when the system is already under
stress. `eventforge.idempotency.lock-timeout` defaults to **1s** to bound it, and exceeding it
yields a retryable `409` with `Retry-After`. That is load-shedding, not the normal path.

**The deferred real fix, named rather than implied:** return `409` immediately on discovering an
in-flight claim, instead of blocking on it at all. That requires distinguishing "another
transaction holds this key right now" from "this key is free", which `ON CONFLICT DO NOTHING`
alone cannot express — it blocks rather than reporting the conflict. Doing it properly means a
`pg_try_advisory_xact_lock` on the key's hash before the insert, or an explicit `IN_PROGRESS`
row with the reaper this design deliberately avoided. Neither is built. The blocking design is
adequate at this project's scale and is documented here as a bounded cost, not presented as free.

## Two things verified by running, not assumed

Both were wrong on the first attempt, and both would have shipped as defects.

**1. Postgres `55P03` is not a Spring concurrency exception.** The `409` handler originally caught
`org.springframework.dao.CannotAcquireLockException`, on the reasonable assumption that Spring maps
a lock timeout to a concurrency-failure type. It does not: `lock_timeout` raises SQLSTATE `55P03`
("canceling statement due to lock timeout"), and Spring's translator leaves SQLSTATE class 55
**uncategorized**, delivering `org.springframework.jdbc.UncategorizedSQLException`. The handler
would never have fired and callers would have received `500` instead of a retryable `409`.
`IdempotencyKeyStore` now matches the SQLSTATE itself and raises
`IdempotencyClaimInFlightException`, keeping that knowledge next to the SQL that provokes it
rather than forcing the controller to catch a far broader type.
`IdempotencyLockTimeoutIntegrationTest` fails loudly if this ever changes.

**2. `jsonb` cannot store a response body verbatim.** `response_body` was originally `JSONB`.
jsonb is a parsed document type: it discards the original text, reorders keys and re-renders
whitespace. A body stored as `{"orderId":…,"status":…}` read back as
`{"status": …, "orderId": …}` — semantically identical, textually different, and therefore not a
verbatim replay. A client comparing bytes, or verifying a signature over the body, would see a
different response than the one it was promised. The column is `TEXT`. Nothing queries inside
this value, so jsonb's only real advantage never applied.

## Retention, and what expires with it

`expires_at = created_at + eventforge.idempotency.ttl` (default 24h); `IdempotencyKeyReaper`
deletes expired rows on a schedule. The honest consequence, stated the way ADR-0012 states the
equivalent for `processed_events`: **a retry arriving after the TTL creates a second order.** The
protection is a window, not a permanent property, and the window is a configured guess rather
than a measured one.

The reaper bean is independently switchable (`eventforge.idempotency.reaper-enabled`). That is
not decoration: a `@Scheduled` method with no `initialDelay` fires on the scheduler's own thread
with no happens-before relationship to a test's thread, and M4 lost a full diagnostic session to
exactly that shape — a background poller consuming a row before the test's own call, producing a
failure that read as a production bug. Making the interval long only makes such a race rare;
removing the bean removes it.

## Consequences

- The header is optional. Absent, the original code path runs unchanged, so every pre-existing
  caller and test keeps its exact prior behaviour.
- Reusing a key with a different body is `422`, and the refusal rolls back without disturbing the
  committed claim it collided with.
- A single `OrderResponse.forNewOrder` renders the stored body and the returned body, so the two
  cannot drift apart as the response shape evolves.

## Revisit if

Connection-pool pressure from blocked losers is ever actually observed, at which point the
deferred immediate-409 design above becomes worth its added complexity — or if a caller needs the
idempotency window to outlive 24h, which changes retention from a reaper into an archival
question of the kind ADR-0004's T3 posture describes.
