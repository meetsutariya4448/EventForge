package com.eventforge.events.envelope;

import com.fasterxml.jackson.databind.JsonNode;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * The event contract every service publishes and consumes.
 *
 * <p>Propagation context (traceparent/tracestate) is deliberately NOT a field here: it travels
 * as Kafka headers in transport and as durable columns in the outbox table for restoration,
 * never inside the payload or the envelope itself.
 */
public record EventEnvelope(
        UUID eventId,
        String eventType,
        int schemaVersion,
        String aggregateId,
        UUID correlationId,
        UUID causationId,
        Instant occurredAt,
        JsonNode payload) {

    public EventEnvelope {
        Objects.requireNonNull(eventId, "eventId must not be null");
        Objects.requireNonNull(eventType, "eventType must not be null");
        Objects.requireNonNull(aggregateId, "aggregateId must not be null");
        Objects.requireNonNull(correlationId, "correlationId must not be null");
        Objects.requireNonNull(occurredAt, "occurredAt must not be null");
        Objects.requireNonNull(payload, "payload must not be null");
        if (schemaVersion < 1) {
            throw new IllegalArgumentException("schemaVersion must be >= 1");
        }
        // causationId is intentionally nullable: the first event in a causal chain
        // (e.g. triggered directly by an HTTP request) has no cause.
    }
}
