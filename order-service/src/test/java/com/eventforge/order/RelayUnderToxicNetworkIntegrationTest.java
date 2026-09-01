package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.outbox.OutboxRelayWorker;
import com.eventforge.events.outbox.RelayOutcome;
import com.eventforge.testing.containers.AbstractToxicKafkaIntegrationTest;
import eu.rekawek.toxiproxy.model.ToxicDirection;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * M1 addendum: proves the Toxiproxy seam ({@link AbstractToxicKafkaIntegrationTest}) actually
 * works, against the real relay — both failure shapes M7 will need for slow-broker measurement,
 * neither of which {@code OutboxRelayCrashWindowIntegrationTest}'s container pause can represent:
 * genuine connection-refused (new connection attempts rejected immediately) and injected network
 * latency (the connection succeeds, but slowly). This is infrastructure verification, not M7's
 * benchmark suite — no measurement or tuning happens here.
 */
@SpringBootTest
class RelayUnderToxicNetworkIntegrationTest extends AbstractToxicKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        // Same reasoning as the crash-window tests: the background poller must not race the
        // explicit relayNextEvent() calls below.
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        // No backoff window to navigate around for this test's "recovers after the toxic clears"
        // assertion — avoids needing an injected Clock just to prove the seam works.
        registry.add("eventforge.outbox.relay.retry-backoff-ms", () -> "0");
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private OutboxRelayWorker relayWorker;

    @Test
    void relayPublishesSuccessfullyThroughInjectedLatency() throws Exception {
        drainAllPending();
        String aggregateId = "toxic-latency-" + UUID.randomUUID();
        insertSyntheticOutboxRow(aggregateId);

        // Well under kafka-send-timeout-ms (10s default) - the connection succeeds, just slowly.
        kafkaProxy.toxics().latency("added-latency", ToxicDirection.DOWNSTREAM, 2000);
        try {
            RelayOutcome outcome = relayWorker.relayNextEvent();
            assertThat(outcome).isEqualTo(RelayOutcome.PUBLISHED);
        } finally {
            kafkaProxy.toxics().get("added-latency").remove();
        }

        Timestamp publishedAt = jdbcTemplate.queryForObject(
                "SELECT published_at FROM outbox_events WHERE aggregate_id = ?", Timestamp.class, aggregateId);
        assertThat(publishedAt).isNotNull();
    }

    @Test
    void relayRecordsAFailedAttemptOnGenuineConnectionRefusedThenRecovers() throws Exception {
        drainAllPending();
        String aggregateId = "toxic-refused-" + UUID.randomUUID();
        insertSyntheticOutboxRow(aggregateId);

        RelayOutcome duringOutage;
        try {
            // Disabling the proxy rejects new connection attempts immediately - genuine
            // connection-refused, distinct in kind from OutboxRelayCrashWindowIntegrationTest's
            // pause (which makes the broker merely unresponsive, not actively refusing).
            kafkaProxy.disable();
            duringOutage = relayWorker.relayNextEvent();
        } finally {
            kafkaProxy.enable();
        }
        assertThat(duringOutage).isEqualTo(RelayOutcome.PUBLISH_FAILED);

        Map<String, Object> afterOutage = jdbcTemplate.queryForMap(
                "SELECT published_at, publish_attempts, last_error FROM outbox_events WHERE aggregate_id = ?",
                aggregateId);
        assertThat(afterOutage.get("published_at")).isNull();
        assertThat(((Number) afterOutage.get("publish_attempts")).intValue()).isEqualTo(1);
        assertThat(afterOutage.get("last_error")).isNotNull();

        RelayOutcome afterRecovery = relayWorker.relayNextEvent();
        assertThat(afterRecovery).isEqualTo(RelayOutcome.PUBLISHED);
    }

    private void drainAllPending() {
        int guard = 0;
        while (relayWorker.relayNextEvent() != RelayOutcome.NOTHING_TO_CLAIM && guard++ < 500) {
            // keep draining leftover unpublished rows from other tests
        }
    }

    private void insertSyntheticOutboxRow(String aggregateId) {
        UUID eventId = UUID.randomUUID();
        jdbcTemplate.update(
                """
                INSERT INTO outbox_events (
                    event_id, aggregate_type, aggregate_id, aggregate_sequence,
                    event_type, schema_version, correlation_id, causation_id,
                    traceparent, tracestate, payload, occurred_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
                """,
                eventId,
                "TestAggregate",
                aggregateId,
                1L,
                "TestEvent",
                1,
                eventId,
                null,
                null,
                null,
                "{\"note\":\"toxic network test\"}",
                Timestamp.from(Instant.now()));
    }
}
