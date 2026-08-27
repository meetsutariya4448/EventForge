package com.eventforge.events.outbox;

import java.sql.Timestamp;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The one write-side mechanism every service that publishes events uses (constitution Part 2:
 * "EVERY service that publishes events uses the same outbox mechanism — not just
 * order-service"). Hand-written SQL per R4 — this is exactly the kind of insert whose
 * constraint/locking behavior needs to stay visible, not hidden behind an ORM.
 *
 * <p>Callers are responsible for calling {@link #write} inside the same {@code @Transactional}
 * boundary as their business-row write — that's what makes the dual write atomic. This class does
 * not open or manage a transaction itself.
 */
public class OutboxWriter {

    private final JdbcTemplate jdbcTemplate;

    public OutboxWriter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void write(OutboxEventRow row) {
        jdbcTemplate.update(
                """
                INSERT INTO outbox_events (
                    event_id, aggregate_type, aggregate_id, aggregate_sequence,
                    event_type, schema_version, correlation_id, causation_id,
                    traceparent, tracestate, payload, occurred_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
                """,
                row.eventId(),
                row.aggregateType(),
                row.aggregateId(),
                row.aggregateSequence(),
                row.eventType(),
                row.schemaVersion(),
                row.correlationId(),
                row.causationId(),
                row.traceparent(),
                row.tracestate(),
                row.payloadJson(),
                Timestamp.from(row.occurredAt()));
    }
}
