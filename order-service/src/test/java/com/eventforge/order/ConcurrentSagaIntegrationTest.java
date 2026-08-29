package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.outbox.OutboxRelayWorker;
import com.eventforge.events.outbox.RelayOutcome;
import com.eventforge.order.domain.OrderService;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
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
 * Constitution item 8f: concurrent sagas on distinct orders must not interfere. {@code
 * InventoryReservationServiceConcurrentReservationIntegrationTest} (inventory-service's own suite)
 * proves the sharper claim — real row-locked contention when two sagas touch the same inventory
 * item and stock is scarce enough that one must legitimately fail. This test proves the
 * orchestration layer's half: N sagas, sharing the same SKU, run through the full happy path
 * concurrently and each ends in its own independently-correct terminal state with no cross-saga
 * state leakage — one saga's dispatch never lands against another saga's row.
 */
@SpringBootTest
class ConcurrentSagaIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final int SAGA_COUNT = 5;
    private static final String SHARED_SKU = "sku-concurrent-shared";

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
    void concurrentSagasOnDistinctOrdersDoNotInterfere() throws Exception {
        drainAllPending();

        List<UUID> orderIds = new ArrayList<>();
        CountDownLatch startingGate = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(SAGA_COUNT);
        try {
            List<Callable<UUID>> starters = new ArrayList<>();
            for (int i = 0; i < SAGA_COUNT; i++) {
                long amount = 1000 + i;
                starters.add(() -> {
                    startingGate.await();
                    return orderService.createOrder(amount, SHARED_SKU, 1L, null, null).getOrderId();
                });
            }
            List<Future<UUID>> futures = new ArrayList<>();
            for (Callable<UUID> starter : starters) {
                futures.add(executor.submit(starter));
            }
            startingGate.countDown();
            for (Future<UUID> future : futures) {
                orderIds.add(future.get(30, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdown();
        }
        assertThat(orderIds).hasSize(SAGA_COUNT);
        assertThat(orderIds).doesNotHaveDuplicates();

        drainAllPending();

        // Feed every saga's PaymentAuthorized fact concurrently, interleaved across all 5 orders -
        // real concurrent processing on the orchestrator's own consumer, not sequential.
        publishConcurrently(orderIds, orderId -> factEnvelope(
                "PaymentAuthorized",
                orderId,
                Map.of("orderId", orderId.toString(), "paymentId", UUID.randomUUID().toString(), "amountCents", 1000, "status", "AUTHORIZED")),
                "payments.events");

        for (UUID orderId : orderIds) {
            waitForSagaState(orderId, "AWAITING_INVENTORY", Duration.ofSeconds(20));
        }
        drainAllPending();

        publishConcurrently(orderIds, orderId -> factEnvelope(
                "InventoryReserved",
                orderId,
                Map.of("orderId", orderId.toString(), "reservationId", UUID.randomUUID().toString(), "sku", SHARED_SKU, "quantity", 1)),
                "inventory.events");

        for (UUID orderId : orderIds) {
            waitForSagaState(orderId, "COMPLETED", Duration.ofSeconds(20));
        }
        drainAllPending();

        // No cross-contamination: every order reached CONFIRMED, every saga has exactly its own
        // 3 steps (not more, not fewer, not another saga's), and every saga_step row belongs to
        // the saga it claims to.
        for (UUID orderId : orderIds) {
            assertThat(orderStatus(orderId)).isEqualTo("CONFIRMED");

            Long stepCount = jdbcTemplate.queryForObject(
                    """
                    SELECT count(*) FROM saga_step s JOIN saga_instance i ON s.saga_id = i.saga_id
                    WHERE i.order_id = ?
                    """,
                    Long.class,
                    orderId);
            assertThat(stepCount).isEqualTo(3L);
        }

        long totalSagas = orderIds.stream()
                .filter(orderId -> jdbcTemplate.queryForObject(
                                "SELECT count(*) FROM saga_instance WHERE order_id = ?", Long.class, orderId)
                        == 1L)
                .count();
        assertThat(totalSagas).isEqualTo((long) SAGA_COUNT);
    }

    private interface FactFactory {
        EventEnvelope build(UUID orderId) throws Exception;
    }

    private void publishConcurrently(List<UUID> orderIds, FactFactory factory, String topic) throws Exception {
        ExecutorService executor = Executors.newFixedThreadPool(orderIds.size());
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (UUID orderId : orderIds) {
                futures.add(executor.submit(() -> {
                    try {
                        EventEnvelope envelope = factory.build(orderId);
                        kafkaTemplate
                                .send(MessageBuilder.withPayload(mapper.writeValueAsString(envelope))
                                        .setHeader(KafkaHeaders.TOPIC, topic)
                                        .setHeader(KafkaHeaders.KEY, orderId.toString())
                                        .build())
                                .get();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
            }
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdown();
        }
    }

    private EventEnvelope factEnvelope(String eventType, UUID orderId, Map<String, Object> payload) {
        return new EventEnvelope(
                UUID.randomUUID(), eventType, 1, orderId.toString(), UUID.randomUUID(), UUID.randomUUID(), Instant.now(), mapper.valueToTree(payload));
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
}
