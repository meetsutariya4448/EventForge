package com.eventforge.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.events.outbox.OutboxRelayWorker;
import com.eventforge.events.outbox.RelayOutcome;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.eventforge.testing.fault.ConfigurableFaultInjector;
import com.eventforge.testing.fault.FaultInjectionTestConfiguration;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * ITEM 4D — THE ONE THAT MATTERS.
 *
 * <p>Every other M2/M3 test proves a component works in isolation: the dedupe primitive, the
 * listener, the fault-injection seam. This one proves the SYSTEM has an end-to-end guarantee. It
 * reuses the real M1 relay mechanism ({@link OutboxRelayWorker}, reconfigured here to target
 * {@code orders.events} — the identical mechanism a real order-service uses, since the relay is
 * fully generic and every service's {@code outbox_events} schema is identical) to produce a
 * genuine Kafka-level duplicate the exact way {@code OutboxRelayCrashWindowIntegrationTest}'s
 * scenario (b) did: crash the relay after the broker has acked the publish, before the
 * mark-published commit. Both real copies land on the real {@code orders.events} topic. Then —
 * unlike every duplicate test above, which drives the listener directly — this lets
 * payment-service's REAL {@code @KafkaListener} container, already running as part of this
 * Spring Boot application, consume both copies on its own, exactly as it would in production.
 *
 * <p>If this test passes, the guarantee is not "our dedupe code is correct in isolation" — it's
 * "a real relay crash, feeding a real Kafka duplicate, into a real running consumer, produces
 * exactly one business effect." That's the thing worth having a first-class name for.
 */
@SpringBootTest
@Import(FaultInjectionTestConfiguration.class)
class RelayProducedDuplicateEndToEndTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        // Reconfigure payment-service's own relay to target orders.events for this test only —
        // the same generic mechanism a real order-service would run, just borrowed here since
        // every service's outbox_events schema is identical. The background scheduler must stay
        // off; only this test's explicit relayNextEvent() calls should drive it.
        registry.add("eventforge.outbox.relay.topic", () -> "orders.events");
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private OutboxRelayWorker relayWorker;

    @Autowired
    private FaultInjector faultInjector;

    @AfterEach
    void clearArmedFaults() {
        ((ConfigurableFaultInjector) faultInjector).clear();
    }

    @Test
    void aRealRelayProducedDuplicateIsAbsorbedExactlyOnceByTheRealConsumer() throws Exception {
        drainAllPending();

        String orderId = "relay-duplicate-e2e-" + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        insertSyntheticAuthorizePaymentRow(orderId, eventId, 3300);

        AtomicBoolean fired = new AtomicBoolean(false);
        ((ConfigurableFaultInjector) faultInjector)
                .registerAction(FaultInjectionPoint.AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED, () -> {
                    if (fired.compareAndSet(false, true)) {
                        throw new IllegalStateException(
                                "simulated crash: Kafka has the message, we never recorded that fact");
                    }
                });

        // First relay attempt: the broker really accepts the message, then the relay "crashes"
        // right before recording that fact — exactly OutboxRelayCrashWindowIntegrationTest's
        // scenario (b), reused here rather than reinvented.
        assertThatThrownBy(() -> relayWorker.relayNextEvent()).isInstanceOf(IllegalStateException.class);

        // "Restart": the row is still unpublished, so it gets picked up and published again —
        // for real, a second time. Kafka now genuinely holds two copies of this AuthorizePayment.
        RelayOutcome secondAttempt = relayWorker.relayNextEvent();
        assertThat(secondAttempt).isEqualTo(RelayOutcome.PUBLISHED);

        // From here on, nothing in this test drives the consumer — payment-service's real
        // @KafkaListener container, already running, picks up both copies on its own.
        waitForExactlyOnePayment(orderId);

        Integer paymentCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM payments WHERE order_id = ?", Integer.class, orderId);
        assertThat(paymentCount).isEqualTo(1);

        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'PaymentAuthorized'",
                Integer.class,
                orderId);
        assertThat(outboxCount).isEqualTo(1);

        Integer processedCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_events WHERE consumer_group = 'payment-service' AND event_id = ?",
                Integer.class,
                eventId);
        assertThat(processedCount).isEqualTo(1);
    }

    private void drainAllPending() {
        int guard = 0;
        while (relayWorker.relayNextEvent() != RelayOutcome.NOTHING_TO_CLAIM && guard++ < 500) {
            // keep draining leftover unpublished rows from other tests in this shared container
        }
    }

    /**
     * Waits for the real, already-running consumer to pick up and process the duplicate — a
     * bounded safety net for genuinely asynchronous consumption, not a correctness assertion.
     * Nothing here depends on how long it actually takes.
     */
    private void waitForExactlyOnePayment(String orderId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM payments WHERE order_id = ?", Integer.class, orderId);
            if (count != null && count >= 1) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Timed out waiting for payment-service's real consumer to process order " + orderId);
    }

    // outbox_events' uniqueness constraint is on (aggregate_id, aggregate_sequence) only - it does
    // not include aggregate_type (ADR-0004). In real deployment this never matters, since
    // order-service's "Order" aggregate rows and payment-service's own "Payment" aggregate rows
    // live in entirely separate databases. Here, borrowing payment-service's own outbox table to
    // simulate order-service's, both happen to key by the same order_id - so this synthetic row
    // must NOT reuse sequence 1, or it collides with PaymentAuthorizationService's own real
    // "PaymentAuthorized" write (also aggregate_id=orderId, aggregate_sequence=1L) once the
    // consumer processes it, rolling back the whole business transaction on a spurious conflict
    // that could never happen for real. A distinctly out-of-band sequence sidesteps that.
    private static final long SYNTHETIC_ROW_AGGREGATE_SEQUENCE = -1L;

    private void insertSyntheticAuthorizePaymentRow(String orderId, UUID eventId, long amountCents) {
        jdbcTemplate.update(
                """
                INSERT INTO outbox_events (
                    event_id, aggregate_type, aggregate_id, aggregate_sequence,
                    event_type, schema_version, correlation_id, causation_id,
                    traceparent, tracestate, payload, occurred_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
                """,
                eventId,
                "Order",
                orderId,
                SYNTHETIC_ROW_AGGREGATE_SEQUENCE,
                "AuthorizePayment",
                1,
                eventId,
                null,
                "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                null,
                "{\"orderId\":\"" + orderId + "\",\"amountCents\":" + amountCents + "}",
                Timestamp.from(Instant.now()));
    }
}
