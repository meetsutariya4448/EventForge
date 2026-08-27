package com.eventforge.events.outbox;

import java.time.Instant;
import java.util.UUID;

/**
 * One row to insert into a service's own {@code outbox_events} table (see
 * ADR-0004 for the schema this maps onto). {@code traceparent}/{@code tracestate} are the durable
 * copy of the propagation context captured at write time (ADR-0005); {@code payloadJson} must
 * already be a JSON string — the writer inserts it as {@code ::jsonb} verbatim.
 */
public record OutboxEventRow(
        UUID eventId,
        String aggregateType,
        String aggregateId,
        long aggregateSequence,
        String eventType,
        int schemaVersion,
        UUID correlationId,
        UUID causationId,
        String traceparent,
        String tracestate,
        String payloadJson,
        Instant occurredAt) {}
