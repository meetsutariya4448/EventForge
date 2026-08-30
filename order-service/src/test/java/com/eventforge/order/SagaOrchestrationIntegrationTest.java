package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.outbox.OutboxRelayWorker;
import com.eventforge.events.outbox.RelayOutcome;
import com.eventforge.order.domain.Order;
import com.eventforge.order.domain.OrderService;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Constitution items 3, 4a/4b (orchestration side), and 8a/8b: the orchestrator's own behavior,
 * proven against real Postgres and real Kafka — the saga's own transitions, commands, and terminal
 * state, driven by synthetic facts standing in for payment-service/inventory-service (which aren't
 * booted in-process here — the same hop-by-hop testing discipline M2 already established: each
 * service's own suite proves its own hop for real; {@code PaymentAuthorizationService}'s and
 * {@code InventoryReservationService}'s own tests prove the participant side of this same
 * exchange).
 */
@SpringBootTest
class SagaOrchestrationIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final String TOPIC = "orders.events";

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.poll-interval-ms", () -> "3600000");
        registry.add("eventforge.saga.sweep-interval-ms", () -> "3600000");
    }

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxRelayWorker relayWorker;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void happyPathReachesCompletedWithAConsistentLedger() throws Exception {
        drainAllPending();

        Order order = orderService.createOrder(5000, "sku-happy", 2L);
        UUID orderId = order.getOrderId();
        drainAllPending();

        assertThat(consumeOneByType(orderId, "AuthorizePayment", Duration.ofSeconds(20))).isNotNull();
        assertThat(sagaState(orderId)).isEqualTo("AWAITING_PAYMENT");

        publishFact("payments.events", orderId, "PaymentAuthorized", paymentAuthorizedPayload(orderId));
        waitForSagaState(orderId, "AWAITING_INVENTORY", Duration.ofSeconds(20));

        drainAllPending();
        ConsumerRecord<String, String> reserveCommand = consumeOneByType(orderId, "ReserveInventory", Duration.ofSeconds(20));
        EventEnvelope reserveEnvelope = mapper.readValue(reserveCommand.value(), EventEnvelope.class);
        assertThat(reserveEnvelope.payload().get("sku").asText()).isEqualTo("sku-happy");
        assertThat(reserveEnvelope.payload().get("quantity").asLong()).isEqualTo(2L);
        // Causal chain: ReserveInventory's cause is the PaymentAuthorized fact, not OrderCreated.
        assertThat(reserveEnvelope.causationId()).isNotEqualTo(reserveEnvelope.correlationId());

        publishFact("inventory.events", orderId, "InventoryReserved", inventoryReservedPayload(orderId, "sku-happy", 2));
        waitForSagaState(orderId, "COMPLETED", Duration.ofSeconds(20));

        drainAllPending();
        assertThat(consumeOneByType(orderId, "OrderConfirmed", Duration.ofSeconds(20))).isNotNull();

        assertThat(orderStatus(orderId)).isEqualTo("CONFIRMED");

        List<String> stepNames = jdbcTemplate.queryForList(
                """
                SELECT s.step_name FROM saga_step s JOIN saga_instance i ON s.saga_id = i.saga_id
                WHERE i.order_id = ? ORDER BY s.dispatched_at
                """,
                String.class,
                orderId);
        assertThat(stepNames).containsExactly("AuthorizePayment", "ReserveInventory", "OrderConfirmed");

        List<String> stepStatuses = jdbcTemplate.queryForList(
                """
                SELECT s.status FROM saga_step s JOIN saga_instance i ON s.saga_id = i.saga_id
                WHERE i.order_id = ? ORDER BY s.dispatched_at
                """,
                String.class,
                orderId);
        assertThat(stepStatuses).containsExactly("SUCCEEDED", "SUCCEEDED", "SUCCEEDED");
    }

    @Test
    void forcedInventoryFailureCompensatesAndCancelsTheOrder() throws Exception {
        drainAllPending();

        Order order = orderService.createOrder(4200, "sku-fail", 1L);
        UUID orderId = order.getOrderId();
        drainAllPending();

        assertThat(consumeOneByType(orderId, "AuthorizePayment", Duration.ofSeconds(20))).isNotNull();
        publishFact("payments.events", orderId, "PaymentAuthorized", paymentAuthorizedPayload(orderId));
        waitForSagaState(orderId, "AWAITING_INVENTORY", Duration.ofSeconds(20));

        drainAllPending();
        assertThat(consumeOneByType(orderId, "ReserveInventory", Duration.ofSeconds(20))).isNotNull();

        // A forced, real business failure (constitution item 8b) — see
        // InventoryReservationService's own tests for the real stock-arithmetic mechanism this
        // fact stands in for here.
        publishFact("inventory.events", orderId, "InventoryReservationFailed", Map.of("orderId", orderId.toString(), "reason", "insufficient stock for sku-fail"));
        waitForSagaState(orderId, "AWAITING_REFUND", Duration.ofSeconds(20));

        drainAllPending();
        ConsumerRecord<String, String> refundCommand = consumeOneByType(orderId, "RefundPayment", Duration.ofSeconds(20));
        assertThat(refundCommand).isNotNull();

        publishFact("payments.events", orderId, "PaymentRefunded", paymentRefundedPayload(orderId));
        waitForSagaState(orderId, "COMPENSATED", Duration.ofSeconds(20));

        drainAllPending();
        assertThat(consumeOneByType(orderId, "OrderCancelled", Duration.ofSeconds(20))).isNotNull();

        assertThat(orderStatus(orderId)).isEqualTo("CANCELLED");

        // Ledger consistency (constitution item 8b): the full step history is coherent and
        // SQL-inspectable, not just the final state.
        List<Map<String, Object>> steps = jdbcTemplate.queryForList(
                """
                SELECT s.step_name, s.status FROM saga_step s JOIN saga_instance i ON s.saga_id = i.saga_id
                WHERE i.order_id = ? ORDER BY s.dispatched_at
                """,
                orderId);
        assertThat(steps).hasSize(4);
        assertThat(steps.get(0)).containsEntry("step_name", "AuthorizePayment").containsEntry("status", "SUCCEEDED");
        assertThat(steps.get(1)).containsEntry("step_name", "ReserveInventory").containsEntry("status", "FAILED");
        assertThat(steps.get(2)).containsEntry("step_name", "RefundPayment").containsEntry("status", "SUCCEEDED");
        assertThat(steps.get(3)).containsEntry("step_name", "OrderCancelled").containsEntry("status", "SUCCEEDED");
    }

    @Test
    void aFactArrivingOutOfStateIsACleanNoOpNotADoubleTransition() throws Exception {
        drainAllPending();

        Order order = orderService.createOrder(1500, "sku-stale-fact", 1L);
        UUID orderId = order.getOrderId();
        drainAllPending();

        assertThat(consumeOneByType(orderId, "AuthorizePayment", Duration.ofSeconds(20))).isNotNull();
        publishFact("payments.events", orderId, "PaymentAuthorized", paymentAuthorizedPayload(orderId));
        waitForSagaState(orderId, "AWAITING_INVENTORY", Duration.ofSeconds(20));
        drainAllPending();
        assertThat(consumeOneByType(orderId, "ReserveInventory", Duration.ofSeconds(20))).isNotNull();

        // ADR-0016's state-guard layer: a SECOND PaymentAuthorized fact for this order (a
        // different event_id, standing in for a sweep-redispatched AuthorizePayment whose first
        // answer already landed - not a Kafka-level redelivery, which processedEventStore already
        // handles) must be ignored, not re-processed.
        publishFact("payments.events", orderId, "PaymentAuthorized", paymentAuthorizedPayload(orderId));

        // Give the (wrongly-processed, if the guard failed) second fact time to land, then assert
        // nothing changed: still AWAITING_INVENTORY, still exactly one ReserveInventory dispatch.
        Thread.sleep(2000);
        assertThat(sagaState(orderId)).isEqualTo("AWAITING_INVENTORY");

        Long reserveInventoryStepCount = jdbcTemplate.queryForObject(
                """
                SELECT count(*) FROM saga_step s JOIN saga_instance i ON s.saga_id = i.saga_id
                WHERE i.order_id = ? AND s.step_name = 'ReserveInventory'
                """,
                Long.class,
                orderId);
        assertThat(reserveInventoryStepCount).isEqualTo(1L);
    }

    private Map<String, Object> paymentAuthorizedPayload(UUID orderId) {
        return Map.of(
                "orderId", orderId.toString(),
                "paymentId", UUID.randomUUID().toString(),
                "amountCents", 5000,
                "status", "AUTHORIZED");
    }

    private Map<String, Object> paymentRefundedPayload(UUID orderId) {
        return Map.of(
                "orderId", orderId.toString(),
                "paymentId", UUID.randomUUID().toString(),
                "amountCents", 4200,
                "status", "REFUNDED");
    }

    private Map<String, Object> inventoryReservedPayload(UUID orderId, String sku, long quantity) {
        return Map.of(
                "orderId", orderId.toString(),
                "reservationId", UUID.randomUUID().toString(),
                "sku", sku,
                "quantity", quantity);
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
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "saga-test-" + UUID.randomUUID());
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
}
