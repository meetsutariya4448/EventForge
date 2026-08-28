package com.eventforge.events.outbox;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

/**
 * Claims and attempts exactly one outbox row per call. Safe to run as multiple concurrent
 * instances — trap T1 is solved, not deferred, verified by {@code MultiWorkerRelayOrderingIntegrationTest}
 * (see ADR-0010's "T1 is solved" amendment for how and what remains unverified: throughput under
 * concurrency, not correctness).
 *
 * <p>A real, recoverable publish failure (broker down, timeout, ...) is handled gracefully: the
 * attempt is recorded durably (attempt count, last-attempt time, last error) and the transaction
 * commits normally, leaving the row unpublished and eligible again once {@code retryBackoff}
 * elapses. This is intentionally a <em>single</em> attempt per call — no internal retry loop — the
 * boundedness and the backoff both come from the claim query itself only selecting rows whose
 * {@code last_attempt_at} is outside the backoff window, evaluated against an injected
 * {@link Clock} rather than the database's own clock, so tests can control it deterministically.
 *
 * <p>A <em>simulated crash</em> (the {@link FaultInjectionPoint#AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED}
 * seam) is different in kind from a recoverable failure and is deliberately not caught — see the
 * comment at that call site.
 */
public class OutboxRelayWorker {

    // Restricting to each aggregate's head row (the lowest unpublished aggregate_sequence)
    // preserves strict per-aggregate order — a later sequence number can never be claimed ahead of
    // an earlier one still awaiting retry. Ordering the resulting candidates by last_attempt_at
    // (oldest/never-attempted first), rather than by aggregate_id, is what prevents head-of-line
    // blocking under retry backoff: without it, a single persistently-failing aggregate keeps
    // re-qualifying for reclaim (its last_attempt_at is always the most recent), so a naive
    // "ORDER BY aggregate_id" claim query would keep re-selecting that same aggregate forever and
    // starve every other aggregate in the table — discovered directly by the M1 crash-window tests
    // stopping the broker under multiple concurrent orders (scenario a).
    private static final String CLAIM_SQL =
            """
            SELECT event_id, aggregate_id, event_type, schema_version, correlation_id,
                   causation_id, traceparent, tracestate, payload::text AS payload_text, occurred_at
            FROM outbox_events o
            WHERE published_at IS NULL
              AND (last_attempt_at IS NULL OR last_attempt_at <= ?)
              AND aggregate_sequence = (
                  SELECT MIN(o2.aggregate_sequence)
                  FROM outbox_events o2
                  WHERE o2.aggregate_id = o.aggregate_id AND o2.published_at IS NULL
              )
            ORDER BY last_attempt_at ASC NULLS FIRST, aggregate_id, aggregate_sequence
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            """;

    private static final String MARK_PUBLISHED_SQL =
            """
            UPDATE outbox_events
            SET published_at = ?, publish_attempts = publish_attempts + 1, last_attempt_at = ?
            WHERE event_id = ?
            """;

    private static final String RECORD_FAILED_ATTEMPT_SQL =
            """
            UPDATE outbox_events
            SET publish_attempts = publish_attempts + 1, last_attempt_at = ?, last_error = ?
            WHERE event_id = ?
            """;

    private static final int MAX_ERROR_MESSAGE_LENGTH = 2000;

    private final JdbcTemplate jdbcTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final FaultInjector faultInjector;
    private final String topic;
    private final Clock clock;
    private final Duration retryBackoff;
    private final Duration kafkaSendTimeout;
    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    public OutboxRelayWorker(
            JdbcTemplate jdbcTemplate,
            KafkaTemplate<String, String> kafkaTemplate,
            FaultInjector faultInjector,
            String topic,
            Clock clock,
            Duration retryBackoff,
            Duration kafkaSendTimeout) {
        this.jdbcTemplate = jdbcTemplate;
        this.kafkaTemplate = kafkaTemplate;
        this.faultInjector = faultInjector;
        this.topic = topic;
        this.clock = clock;
        this.retryBackoff = retryBackoff;
        this.kafkaSendTimeout = kafkaSendTimeout;
    }

    @Transactional
    public RelayOutcome relayNextEvent() {
        Instant now = clock.instant();
        Optional<ClaimedOutboxEvent> claimed = claimEligibleRow(now);
        if (claimed.isEmpty()) {
            return RelayOutcome.NOTHING_TO_CLAIM;
        }
        ClaimedOutboxEvent event = claimed.get();

        String value;
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
            value = mapper.writeValueAsString(envelope);
        } catch (Exception e) {
            recordFailedAttempt(event.eventId(), now, e);
            return RelayOutcome.PUBLISH_FAILED;
        }

        ProducerRecord<String, String> record = new ProducerRecord<>(topic, event.aggregateId(), value);
        // M4: this copies the stored traceparent/tracestate verbatim into Kafka headers, which is
        // correct for M1 — there is no tracer here to ask. Once OpenTelemetry lands (M4), this must
        // become extract -> start a child span for "relay publish" -> inject, or the relay stays
        // invisible in the trace and the consumer ends up a sibling of the original HTTP span
        // instead of its descendant. The stored column format (plain strings) does not change.
        if (event.traceparent() != null) {
            record.headers().add("traceparent", event.traceparent().getBytes(StandardCharsets.UTF_8));
        }
        if (event.tracestate() != null) {
            record.headers().add("tracestate", event.tracestate().getBytes(StandardCharsets.UTF_8));
        }

        try {
            kafkaTemplate.send(record).get(kafkaSendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            recordFailedAttempt(event.eventId(), now, e);
            return RelayOutcome.PUBLISH_FAILED;
        } catch (Exception e) {
            // A real, recoverable publish failure (broker down, timeout, ...). This commits: a
            // failed attempt is durable information worth keeping, and the row simply stays
            // eligible for another try once retryBackoff elapses (see the claim query above).
            recordFailedAttempt(event.eventId(), now, e);
            return RelayOutcome.PUBLISH_FAILED;
        }

        // The seam a real crash between "Kafka has the message" and "we recorded that fact"
        // exercises. Deliberately NOT caught: a crash here must roll back the whole transaction —
        // claim included — so publish_attempts does not increment and the row is republished,
        // unmodified, on the very next poll: a genuine, expected DUPLICATE. This is exactly the
        // scenario M2's idempotent consumers (via processed_events) exist to absorb; nothing on the
        // relay's side is supposed to prevent it.
        faultInjector.inject(FaultInjectionPoint.AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED);

        markPublished(event.eventId(), now);
        return RelayOutcome.PUBLISHED;
    }

    private Optional<ClaimedOutboxEvent> claimEligibleRow(Instant now) {
        Timestamp backoffThreshold = Timestamp.from(now.minus(retryBackoff));
        List<ClaimedOutboxEvent> claimed =
                jdbcTemplate.query(CLAIM_SQL, (PreparedStatement ps) -> ps.setTimestamp(1, backoffThreshold), this::mapRow);
        return claimed.stream().findFirst();
    }

    private void markPublished(UUID eventId, Instant attemptTime) {
        Timestamp ts = Timestamp.from(attemptTime);
        jdbcTemplate.update(MARK_PUBLISHED_SQL, ts, ts, eventId);
    }

    private void recordFailedAttempt(UUID eventId, Instant attemptTime, Exception cause) {
        Throwable root = cause;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        String message = cause.getClass().getSimpleName() + ": " + cause.getMessage()
                + " (root cause: " + root.getClass().getSimpleName() + ": " + root.getMessage() + ")";
        if (message.length() > MAX_ERROR_MESSAGE_LENGTH) {
            message = message.substring(0, MAX_ERROR_MESSAGE_LENGTH);
        }
        jdbcTemplate.update(RECORD_FAILED_ATTEMPT_SQL, Timestamp.from(attemptTime), message, eventId);
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
