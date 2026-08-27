package com.eventforge.events.outbox;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

/**
 * Claims and publishes exactly one outbox row per call, single-worker (see ADR-0010: no
 * aggregate-level claiming yet — trap T1 is deferred, not solved, and this must not run as more
 * than one instance until it is). The claim, publish, and mark-published all happen inside one
 * database transaction, so a crash anywhere in this method leaves the row exactly as it was
 * before the call (still unpublished) or fully done (published) — never half-claimed.
 *
 * <p>Reconstructs the full {@link EventEnvelope} from the claimed row and restores its stored
 * {@code traceparent}/{@code tracestate} as real Kafka headers — this is the "restoration" half
 * of ADR-0005, not just storage.
 */
public class OutboxRelayWorker {

    private static final String CLAIM_SQL =
            """
            SELECT event_id, aggregate_id, event_type, schema_version, correlation_id,
                   causation_id, traceparent, tracestate, payload::text AS payload_text, occurred_at
            FROM outbox_events
            WHERE published_at IS NULL
            ORDER BY aggregate_id, aggregate_sequence
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """;

    private static final String MARK_PUBLISHED_SQL =
            """
            UPDATE outbox_events
            SET published_at = now(), publish_attempts = publish_attempts + 1, last_attempt_at = now()
            WHERE event_id = ?
            """;

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final FaultInjector faultInjector;
    private final String topic;
    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    public OutboxRelayWorker(
            JdbcTemplate jdbcTemplate, KafkaTemplate<String, String> kafkaTemplate, FaultInjector faultInjector, String topic) {
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.faultInjector = faultInjector;
        this.topic = topic;
    }

    /** @return true if a row was claimed and published, false if there was nothing to do. */
    @Transactional
    public boolean relayNextEvent() {
        List<ClaimedOutboxEvent> claimed = jdbcTemplate.query(CLAIM_SQL, this::mapRow);
        if (claimed.isEmpty()) {
            return false;
        }
        ClaimedOutboxEvent event = claimed.get(0);

        try {
            JsonNode payload = mapper.readTree(event.payloadJson());
            EventEnvelope envelope = new EventEnvelope(
                    event.eventId(),
                    event.eventType(),
                    event.schemaVersion(),
                    event.aggregateId(),
                    event.correlationId(),
                    event.causationId(),
                    event.occurredAt(),
                    payload);
            String value = mapper.writeValueAsString(envelope);

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.aggregateId(), value);
            if (event.traceparent() != null) {
                record.headers().add("traceparent", event.traceparent().getBytes(StandardCharsets.UTF_8));
            }
            if (event.tracestate() != null) {
                record.headers().add("tracestate", event.tracestate().getBytes(StandardCharsets.UTF_8));
            }

            kafkaTemplate.send(record).get(10, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new OutboxRelayException("Failed to publish outbox event " + event.eventId(), e);
        }

        faultInjector.inject(FaultInjectionPoint.AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED);

        jdbcTemplate.update(MARK_PUBLISHED_SQL, event.eventId());
        return true;
    }

    private ClaimedOutboxEvent mapRow(ResultSet rs, int rowNum) throws SQLException {
        return new ClaimedOutboxEvent(
                UUID.fromString(rs.getString("event_id")),
                rs.getString("aggregate_id"),
                rs.getString("event_type"),
                rs.getInt("schema_version"),
                UUID.fromString(rs.getString("correlation_id")),
                rs.getString("causation_id") == null ? null : UUID.fromString(rs.getString("causation_id")),
                rs.getString("traceparent"),
                rs.getString("tracestate"),
                rs.getString("payload_text"),
                rs.getTimestamp("occurred_at").toInstant());
    }
}
