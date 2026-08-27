# ADR-0005: Trace context propagation — Kafka headers + durable outbox columns, never payload

## Context

One of the four claims this project exists to prove (Part 1 of the constitution) is that a single
HTTP request produces one continuous trace across a database-backed outbox and an asynchronous
relay, spanning a multi-service saga. That only works if the W3C trace context (`traceparent`,
`tracestate`) captured at the moment an event is written survives the async gap between "write to
outbox" and "relay publishes to Kafka," and survives transport across Kafka to the next service.

## Options considered

**Embed `traceparent`/`tracestate` inside the event payload JSON.** Works for transport, but
conflates two different concerns: the payload is the business event contract (versioned per
ADR-0002, meaningful to business logic), while trace context is a cross-cutting transport concern
that no business logic should ever need to parse out of a payload. It also means every payload
schema has to reserve fields for something that isn't part of the business event at all.

**Kafka headers only, nothing durable.** Headers are the correct transport mechanism — Kafka
supports arbitrary headers per record, and OpenTelemetry's Kafka instrumentation already expects
context there. But headers alone don't solve the async gap: the relay polls the outbox table on
its own schedule, potentially seconds after the original HTTP request's tracing context is long
gone from any in-memory propagation mechanism. Without a durable copy, the relay would have no
context to restore and would either start a new, disconnected trace or inherit its own polling
loop's trace — both wrong per trap T5.

**Chosen: both — Kafka headers for transport, durable `traceparent`/`tracestate` columns on
`outbox_events` for restoration.** The originating service captures the current trace context at
the moment it writes the outbox row (inside the same DB transaction as the business write) and
stores it in those columns. When the relay later picks up that row — regardless of how much later
— it restores that exact context before publishing, and sets it as Kafka headers on the record it
sends. The next service's consumer reads the headers and continues the same trace. Never the
payload.

## Decision

`EventEnvelope` itself does **not** carry `traceparent`/`tracestate` as fields — they are
deliberately absent from the record (see `EventEnvelope.java`'s Javadoc). They travel as:

- Durable columns on `outbox_events` (`traceparent VARCHAR(64)`, `tracestate VARCHAR(512)`),
  written in the same transaction as the business row and the outbox row itself.
- Kafka record headers (`traceparent`, `tracestate`) on publish — validated directly in the M0
  round-trip test, which sets them as headers and asserts they survive a real Kafka round trip
  unchanged.

M0 only proves the columns exist, accept a valid-shaped value, and that headers survive transport.
**Restoration** — the relay actually re-establishing that context as the active span before
publishing — is M1's job, not M0's; M0 lays the storage and transport shape M1 will restore into.

## Consequences

- Every future outbox-writing service must capture and persist trace context at write time, in the
  same transaction as the business row — not as an afterthought bolted on later.
- The relay (M1) must explicitly *restore* the stored context, not merely inherit whatever context
  its own polling loop happens to be running under (trap T5) — this ADR fixes the storage and
  transport shape that restoration will use; getting restoration itself right is verified in M1's
  tests, not deferred all the way to M4's tracing milestone.
- `VARCHAR(64)`/`VARCHAR(512)` sizing follows the W3C Trace Context spec's defined field formats
  (a `traceparent` value is a fixed-format string; `tracestate` is capped at 512 bytes by the
  spec) — not arbitrary.

## Revisit if

The project adopts a propagation format beyond W3C Trace Context (e.g. needing vendor-specific
baggage) that doesn't fit these column shapes.
