package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.outbox.OutboxRelayWorker;
import com.eventforge.events.outbox.RelayOutcome;
import com.eventforge.order.domain.Order;
import com.eventforge.order.domain.OrderService;
import com.eventforge.order.saga.SagaOrchestrator;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Constitution items 5 and 6: the persisted-deadline timeout sweep, and the hard case —
 * compensation itself failing permanently.
 *
 * <p>Both tests call {@link SagaOrchestrator#sweepTimedOutSagas()} directly rather than waiting on
 * the real {@code @Scheduled} bean (disabled here via a large {@code sweep-interval-ms}), exactly
 * the same determinism discipline {@code OutboxRelayCrashWindowIntegrationTest} established for
 * the relay: no {@code Thread.sleep}, no wall-clock wait for "time passing" — the deadline lives in
 * {@code saga_instance.deadline_at}, evaluated against an injected {@link MutableClock} the test
 * advances explicitly. Calling the sweep method directly, reading only from the database, is also
 * the proof that "restart-safe" claim actually means: nothing about resolving a stranded saga
 * depends on which process instance dispatched the original command.
 */
@SpringBootTest
@Import(SagaTimeoutIntegrationTest.ClockConfig.class)
class SagaTimeoutIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final String TOPIC = "orders.events";
    private static final int MAX_COMPENSATION_ATTEMPTS = 3;

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.poll-interval-ms", () -> "3600000");
        registry.add("eventforge.saga.sweep-interval-ms", () -> "3600000");
        registry.add("eventforge.saga.authorize-payment-timeout-ms", () -> "5000");
        registry.add("eventforge.saga.reserve-inventory-timeout-ms", () -> "5000");
        registry.add("eventforge.saga.refund-payment-timeout-ms", () -> "5000");
        registry.add("eventforge.saga.max-compensation-attempts", () -> String.valueOf(MAX_COMPENSATION_ATTEMPTS));
    }

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxRelayWorker relayWorker;

    @Autowired
    private SagaOrchestrator sagaOrchestrator;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private MeterRegistry meterRegistry;

    @Autowired
    private MutableClock mutableClock;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void sagaStrandedByADeadInventoryConsumerEventuallyReachesATerminalStateAfterRestart() throws Exception {
        drainAllPending();
        Order order = orderService.createOrder(3000, "sku-timeout", 1L);
        UUID orderId = order.getOrderId();
        drainAllPending();

        assertThat(consumeOneByType(orderId, "AuthorizePayment", Duration.ofSeconds(20))).isNotNull();
        publishFact("payments.events", orderId, "PaymentAuthorized", Map.of(
                "orderId", orderId.toString(), "paymentId", UUID.randomUUID().toString(), "amountCents", 3000, "status", "AUTHORIZED"));
        waitForSagaState(orderId, "AWAITING_INVENTORY", Duration.ofSeconds(20));

        drainAllPending();
        assertThat(consumeOneByType(orderId, "ReserveInventory", Duration.ofSeconds(20))).isNotNull();

        // inventory-service never responds — a dead consumer, simulated simply by never publishing
        // InventoryReserved/InventoryReservationFailed for this order at all.
        mutableClock.advance(Duration.ofSeconds(6));

        // "Restart": SagaOrchestrator holds no in-memory state between calls (it's a stateless
        // Spring bean reading/writing only saga_instance/saga_step) — calling sweepTimedOutSagas()
        // again is indistinguishable from a freshly-started process picking the stranded saga back
        // up on its own next scheduled tick.
        sagaOrchestrator.sweepTimedOutSagas();

        waitForSagaState(orderId, "AWAITING_REFUND", Duration.ofSeconds(10));
        drainAllPending();
        assertThat(consumeOneByType(orderId, "RefundPayment", Duration.ofSeconds(20))).isNotNull();

        publishFact("payments.events", orderId, "PaymentRefunded", Map.of(
                "orderId", orderId.toString(), "paymentId", UUID.randomUUID().toString(), "amountCents", 3000, "status", "REFUNDED"));
        waitForSagaState(orderId, "COMPENSATED", Duration.ofSeconds(20));

        drainAllPending();
        assertThat(consumeOneByType(orderId, "OrderCancelled", Duration.ofSeconds(20))).isNotNull();
        assertThat(orderStatus(orderId)).isEqualTo("CANCELLED");
    }

    @Test
    void permanentCompensationFailureReachesATerminalStateWithAnAlertAndNoInfiniteLoop() throws Exception {
        drainAllPending();
        Order order = orderService.createOrder(1800, "sku-perm-fail", 1L);
        UUID orderId = order.getOrderId();
        drainAllPending();

        assertThat(consumeOneByType(orderId, "AuthorizePayment", Duration.ofSeconds(20))).isNotNull();
        publishFact("payments.events", orderId, "PaymentAuthorized", Map.of(
                "orderId", orderId.toString(), "paymentId", UUID.randomUUID().toString(), "amountCents", 1800, "status", "AUTHORIZED"));
        waitForSagaState(orderId, "AWAITING_INVENTORY", Duration.ofSeconds(20));

        drainAllPending();
        assertThat(consumeOneByType(orderId, "ReserveInventory", Duration.ofSeconds(20))).isNotNull();

        publishFact("inventory.events", orderId, "InventoryReservationFailed", Map.of("orderId", orderId.toString(), "reason", "forced for this test"));
        waitForSagaState(orderId, "AWAITING_REFUND", Duration.ofSeconds(20));
        drainAllPending();
        assertThat(consumeOneByType(orderId, "RefundPayment", Duration.ofSeconds(20))).isNotNull();

        double before = compensationFailedCount();

        // payment-service never answers ANY RefundPayment attempt (dead consumer for the whole
        // compensation path) — THE HARD CASE (constitution item 6). Bounded: exactly
        // MAX_COMPENSATION_ATTEMPTS sweep ticks resolve it, not an infinite retry loop.
        for (int i = 0; i < MAX_COMPENSATION_ATTEMPTS; i++) {
            mutableClock.advance(Duration.ofSeconds(6));
            sagaOrchestrator.sweepTimedOutSagas();
        }

        assertThat(sagaState(orderId)).isEqualTo("COMPENSATION_FAILED");
        assertThat(orderStatus(orderId)).isEqualTo("CANCELLATION_FAILED");

        Long refundStepCount = jdbcTemplate.queryForObject(
                """
                SELECT count(*) FROM saga_step s JOIN saga_instance i ON s.saga_id = i.saga_id
                WHERE i.order_id = ? AND s.step_name = 'RefundPayment'
                """,
                Long.class,
                orderId);
        assertThat(refundStepCount).isEqualTo(MAX_COMPENSATION_ATTEMPTS);

        // Advancing further and sweeping again must NOT dispatch a 4th attempt — this is the "no
        // infinite loop" half of the requirement, checked directly rather than just trusting the
        // bound held.
        mutableClock.advance(Duration.ofSeconds(6));
        sagaOrchestrator.sweepTimedOutSagas();
        Long refundStepCountAfterExtraSweep = jdbcTemplate.queryForObject(
                """
                SELECT count(*) FROM saga_step s JOIN saga_instance i ON s.saga_id = i.saga_id
                WHERE i.order_id = ? AND s.step_name = 'RefundPayment'
                """,
                Long.class,
                orderId);
        assertThat(refundStepCountAfterExtraSweep).isEqualTo(MAX_COMPENSATION_ATTEMPTS);

        // The alert surface (constitution item 6): a Micrometer counter an operator's monitoring
        // would page on in a real deployment, plus the terminal state itself being a plain SQL
        // query away (SELECT * FROM orders WHERE status = 'CANCELLATION_FAILED') — see
        // docs/runbooks/compensation-failure.md for the manual remediation path.
        assertThat(compensationFailedCount()).isEqualTo(before + 1.0);
    }

    private double compensationFailedCount() {
        var counter = meterRegistry.find("saga.compensation.failed").counter();
        return counter == null ? 0.0 : counter.count();
    }

    private void publishFact(String topic, UUID orderId, String eventType, Map<String, Object> payload) throws Exception {
        EventEnvelope envelope = new EventEnvelope(
                UUID.randomUUID(),
                eventType,
                1,
                orderId.toString(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                Instant.now(),
                mapper.valueToTree(payload));
        kafkaTemplate
                .send(MessageBuilder.withPayload(mapper.writeValueAsString(envelope))
                        .setHeader(KafkaHeaders.TOPIC, topic)
                        .setHeader(KafkaHeaders.KEY, orderId.toString())
                        .build())
                .get();
    }

    private String sagaState(UUID orderId) {
        return jdbcTemplate.queryForObject("SELECT state FROM saga_instance WHERE order_id = ?", String.class, orderId);
    }

    private String orderStatus(UUID orderId) {
        return jdbcTemplate.queryForObject("SELECT status FROM orders WHERE order_id = ?", String.class, orderId);
    }

    private void waitForSagaState(UUID orderId, String expected, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (expected.equals(sagaState(orderId))) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Timed out waiting for saga on order " + orderId + " to reach state " + expected
                + " (was " + sagaState(orderId) + ")");
    }

    private void drainAllPending() {
        int guard = 0;
        while (relayWorker.relayNextEvent() != RelayOutcome.NOTHING_TO_CLAIM && guard++ < 500) {
            // keep draining
        }
    }

    private ConsumerRecord<String, String> consumeOneByType(UUID orderId, String eventType, Duration timeout) throws Exception {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "saga-timeout-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    if (!orderId.toString().equals(record.key())) {
                        continue;
                    }
                    EventEnvelope envelope = mapper.readValue(record.value(), EventEnvelope.class);
                    if (eventType.equals(envelope.eventType())) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("Timed out waiting for a " + eventType + " record for order " + orderId);
    }

    /** A {@link Clock} the test advances explicitly rather than waiting on real elapsed time. */
    static final class MutableClock extends Clock {
        private volatile Instant instant;

        MutableClock(Instant initial) {
            this.instant = initial;
        }

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }

    @TestConfiguration
    static class ClockConfig {
        @Bean
        @Primary
        MutableClock mutableClock() {
            return new MutableClock(Instant.now());
        }
    }
}
