package com.eventforge.events.outbox;

import java.time.Instant;
import java.util.UUID;

/** One row claimed off {@code outbox_events} by the relay, not yet published. */
record ClaimedOutboxEvent(
        UUID eventId,
        String aggregateId,
        String eventType,
        int schemaVersion,
        UUID correlationId,
        UUID causationId,
        String traceparent,
        String tracestate,
        String payloadJson,
        Instant occurredAt) {}
