# ADR-0022: Replay publishes outside the outbox, and the architecture rule widens to permit it

## Context

v2 WS3 adds durable capture of records that exhaust their retries, and an operator action to
republish one. `FailedMessageReplayer` sends to Kafka through `KafkaTemplate`.

That is forbidden. `OutboxOnlyPublishingRule` — an ArchUnit rule enforced by a test in each of the
four services — states that no class outside `com.eventforge.events.outbox` may depend on
`KafkaTemplate` at all. It was written in M3 as a structural guard against the dual-write bug: an
event published directly to Kafka is not covered by the database transaction that produced it, so a
crash between the two leaves the broker and the database disagreeing, which is the entire failure
mode the outbox exists to remove.

So this is not a rule that got in the way of a feature. It is a rule that fired on exactly the
class of change it was written to catch, and the question is whether this change is that class of
change or only resembles it.

## Decision

Widen the rule's allowlist to two packages — `com.eventforge.events.outbox` and
`com.eventforge.events.failure` — rather than suppress the rule, annotate an exception, or move
the replayer somewhere the rule does not look.

## Why a replay is not a dual write

The dual-write bug is a specific thing: **a publish whose durable record may not exist.** Two
writes to two systems with no transaction spanning them, so a crash between them loses one.

Replay has the opposite shape. The row in `failed_messages` was committed by
`DurableFailureRecoverer` before the consumer offset was allowed to advance, in an earlier
transaction that has long since ended. By the time anything can be replayed, the durable record is
already there — it is the *only* reason a replay is possible at all, since the request names a
`failed_message_id`. There is no window in which the publish exists and the record does not,
because the record strictly precedes the publish. That is the outbox's actual invariant: *nothing
reaches the broker that is not already durably recorded.* `failed_messages` satisfies it with a
different table.

The failure directions confirm it rather than merely permitting it:

- **Crash after the broker acknowledges, before the row is marked `REPLAYED`.** The row stays
  `REPLAY_REQUESTED` and can be replayed again — a duplicate delivery, which the consumer-side
  `(consumer_group, event_id)` dedupe already absorbs (ADR-0012), because the payload is
  republished byte for byte and the `eventId` with it.
- **Crash after marking, before anything else.** Nothing else follows; the row is accurate.
- **Broker rejects the send.** `PUBLISH_FAILED`, the error is recorded, the row stays eligible.

The one ordering that would be a genuine dual write — mark the row `REPLAYED` first, then publish —
is the one `FailedMessageReplayer` deliberately does not use, for the same reason
`OutboxRelayWorker` does not: it can leave a row claiming a message was sent that never was.

## Why widen, rather than the alternatives

**Suppress the rule for the replayer** (`@ArchIgnore`, an exclusion predicate). This hides the
decision at the site that violates it, where nobody reviewing the rule will see it. The rule's
value is that it makes exceptions expensive; an exception mechanism makes them cheap again.

**Put the replayer in `com.eventforge.events.outbox`.** It would pass the rule unchanged and would
be a lie: the replayer does not touch `outbox_events`, and package membership would stop describing
what a class does. This is the worst option precisely because it is the easiest.

**Have replay write to the outbox instead of publishing directly.** Superficially the most
principled — reuse the machinery, violate nothing. It is wrong on inspection. The outbox relay
publishes to *one* configured topic (`eventforge.outbox.relay.topic`), and a replay must return the
message to *the topic it came from*, which is a different topic in general and is a property of the
captured row, not of the service. The relay also constructs its own envelope and injects a fresh
trace context, so the payload arriving at the broker would no longer be the bytes that were
captured — destroying both the byte-for-byte guarantee that makes the `eventId` dedupe work and the
original `traceparent` that keeps the replay inside the trace it belonged to. Routing replay through
the outbox would mean weakening the outbox to carry arbitrary topics and verbatim payloads, which
degrades the mechanism the codebase depends on in order to preserve an architecture rule about it.

Widening the allowlist keeps the rule doing its job — a third package attempting a direct publish
still fails the build in all four services — while recording, here, why a second package is allowed
and what property qualified it.

## Consequences

- The guard is now "publishing is permitted from two named packages," not one. The bar for a third
  is the argument above: the publish must be preceded by its own committed durable record.
- The rule's failure message names this ADR, so the next person to trip it finds the reasoning
  rather than an unexplained allowlist.
- `FailedMessageReplayer` carries the same responsibility `OutboxRelayWorker` does — claim,
  publish, mark, in that order, never mark-then-publish — and the same consequence: at-least-once
  delivery, absorbed downstream. As everywhere else in this project: **not exactly-once.**

## Status

Accepted (v2 WS3).
