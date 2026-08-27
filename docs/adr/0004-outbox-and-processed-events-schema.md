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
