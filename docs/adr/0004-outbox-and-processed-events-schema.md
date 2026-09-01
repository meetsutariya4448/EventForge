# ADR-0004: `outbox_events` / `processed_events` schema design

## Context

M0 must lay down the outbox and consumer-dedupe schemas without implementing any relay, claiming,
or dedupe logic (that's M1/M2). But the schema itself has to not foreclose two known future traps:

- **T1 — parallel relays vs. per-aggregate ordering.** A naive `FOR UPDATE SKIP LOCKED` claim
  query lets two relay workers publish two events for the *same* aggregate out of creation order.
  The eventual fix is aggregate-level claiming or deterministic sharding by `aggregate_id`. The
  schema must support that without a later migration that reshapes the table.
- **T3 — outbox growth.** `WHERE published_at IS NULL` scans and constant
  `UPDATE ... SET published_at` produce dead tuples and index bloat over time. The schema needs an
  index shaped for the unpublished-rows scan specifically, not a full-table index that keeps
  growing as published rows accumulate.

## Options considered, for the columns that address T1/T3

**No per-aggregate sequence column; rely on `created_at` ordering.** Simpler, but `created_at`
timestamps aren't guaranteed strictly monotonic or unique under concurrent inserts, and a relay
that later claims work per-aggregate needs a value it can compare and increment reliably.
Rejected — `aggregate_sequence BIGINT` is small and gives the relay something totally unambiguous
to order and claim by.

**A single index over `(aggregate_id, aggregate_sequence)` with no unpublished filter.** Simpler,
but grows unbounded with the table and doesn't specifically serve the relay's actual read pattern
("give me the next unpublished events for an aggregate, in order").

**Chosen: `aggregate_sequence BIGINT NOT NULL` + a unique index on
`(aggregate_id, aggregate_sequence)` + a *partial* index on the same columns
`WHERE published_at IS NULL`.** The unique index enforces monotonicity per aggregate and gives a
future relay a natural claim-order scan. The partial index answers exactly the unpublished-rows
query the relay will run, without indexing rows that are already done — so the index stays sized
to the backlog, not the table's full history (T3). Both indexes are already ordered by
`(aggregate_id, aggregate_sequence)`, which is also the ordering a future aggregate-level claiming
strategy needs (T1) — no reshaping required when that logic lands in M1.

**`processed_events` primary key: `(event_id)` alone vs. `(consumer_group, event_id)`.** A service
could plausibly have more than one logical listener over the same topic in the future (e.g. a
metrics listener alongside the business listener), each needing independent idempotency tracking
over the same event. Composite key `(consumer_group, event_id)` future-proofs that without
over-engineering anything now — no dedupe logic exists yet to exploit it, but the column is nearly
free to add today and expensive to retrofit as a primary-key change later.

## Decision

See the migrations for the exact DDL (identical across all four services' databases):
`*/src/main/resources/db/migration/V1__create_outbox_events.sql` and
`V2__create_processed_events.sql`.

`outbox_events` carries `aggregate_type`, `aggregate_id`, `aggregate_sequence`, the full envelope
fields, durable `traceparent`/`tracestate` columns (see ADR-0005), `payload JSONB`, and
publish-tracking columns (`published_at`, `publish_attempts`, `last_attempt_at`, `last_error`) —
all schema, no behavior. `processed_events` carries `(consumer_group, event_id)` as primary key
plus `aggregate_id` and `processed_at`, with a supporting index on `processed_at` for a future
archival/delete-by-age path (a T3-style concern for this table too, not implemented in M0).

Migrated independently into every service's own database — including `notification-service`,
which has nothing to publish yet, purely for schema consistency across services; cheap now,
avoids a special case later if it ever needs to publish delivery-status events.

## Consequences

- No relay, claiming, or dedupe SQL exists yet — the round-trip integration test validates the
  schema with a direct `JdbcTemplate` insert/select, not a simulated relay.
- The unique index on `(aggregate_id, aggregate_sequence)` means whatever assigns sequence numbers
  in M1 must do so without gaps or collisions per aggregate, or inserts will fail — that's a
  deliberate constraint, not an oversight.
- Considered and explicitly deferred: sharing these migrations across services from a common
  classpath location (e.g. shipping the SQL inside `common-events`). That would couple every
  service's schema evolution to `common-events` versioning — more coordination than four
  near-identical files currently justify.

## Revisit if

M1's relay design needs a claiming shape these indexes don't actually serve well in practice, or
M1/M2 discover the sequence-assignment step is a contention point that argues for a different
column shape (e.g. a `BIGSERIAL` instead of application-assigned sequence).

## T3 posture update (M1): the relay is now the thing generating the churn this ADR anticipated

M0 built the partial index and reserved the concern; M1's `OutboxRelayWorker` (ADR-0010) is the
first code that actually writes the churn T3 warned about. This is the honest posture on where
things stand, not an archival implementation — none exists yet.

**What generates dead tuples now.** Every relay attempt against a row — successful or not — is an
`UPDATE` (`markPublished` on success; `recordFailedAttempt` on a recoverable failure). Postgres's
MVCC model means every `UPDATE` leaves the old row version behind as a dead tuple, regardless of
whether the new version's `published_at` value still matches the partial index's
`WHERE published_at IS NULL` predicate. A row that fails several times before eventually
publishing (exactly what the M1 crash-window tests exercise under a degraded broker) produces one
dead tuple *per attempt*, not one per row — retry-heavy periods multiply the churn rate, not just
the final state.

**Current index situation.** The partial index (`idx_outbox_events_unpublished`) itself shrinks
correctly as rows publish — a published row's entry leaves the index the moment `published_at` is
set, so the index's *live* size stays bounded by the backlog, exactly as designed. What it does
*not* address is the dead-tuple debris each `UPDATE` leaves in the base table (and transiently in
the index during the version transition) until `autovacuum` reclaims it. No archival or deletion
path exists for published rows — they accumulate in the table indefinitely, read only by
`autovacuum`'s bookkeeping, never by any query this project runs.

**Expected bloat behavior.** Bloat should scale with two independent things: raw publish volume
(one dead tuple per successful publish, at minimum) and retry rate under broker degradation (one
additional dead tuple per failed attempt, on top of the eventual success). `autovacuum`'s default
thresholds are tuned for general workloads, not specifically for a table with this attempt-heavy
`UPDATE` pattern; whether the defaults keep pace here, or whether this table needs its own
`autovacuum` tuning (lower scale factor) or `FILLFACTOR` adjustment (to favor HOT updates), is
exactly a measurement question — argued from principle here, not from a number.

**What M7 would measure, if it existed.** Actual bloat percentage on `outbox_events` under
sustained load and under sustained broker degradation (retry-heavy conditions), whether
`autovacuum`'s defaults keep pace or lag, and whether an explicit archival/deletion path for
published rows (referenced but not built in either M0 or M1) is warranted before this becomes a
real operational concern. EventForge is feature-frozen at M4 — no archival logic was ever
implemented, and M7 was never started. This section states that boundary plainly, as posture
rather than a fix: a real, named limitation of the project as it stands, not an open task waiting
on a future session.
