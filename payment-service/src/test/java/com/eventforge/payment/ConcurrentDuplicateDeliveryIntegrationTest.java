package com.eventforge.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.payment.consumer.PaymentEventListener;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Item 4b: sequential-only duplicate testing does not count. This delivers the same event from
 * {@code WORKER_COUNT} threads simultaneously (a {@link CountDownLatch} releases them together, to
 * maximize real contention on {@code processed_events}' unique constraint rather than letting them
 * trickle in one at a time), asserting exactly one business effect regardless of which thread's
 * {@code INSERT ... ON CONFLICT DO NOTHING} actually wins the race.
 */
@SpringBootTest
class ConcurrentDuplicateDeliveryIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final int WORKER_COUNT = 16;

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.poll-interval-ms", () -> "3600000");
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> String.valueOf(WORKER_COUNT + 5));
    }

    @Autowired
    private PaymentEventListener listener;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void concurrentDeliveryOfTheSameEventProducesExactlyOneEffect() throws Exception {
        String orderId = "dup-concurrent-" + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String value = buildOrderCreatedJson(orderId, eventId, 2200);

        CountDownLatch startingGate = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(WORKER_COUNT);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int i = 0; i < WORKER_COUNT; i++) {
                tasks.add(() -> {
                    ConsumerRecord<String, String> record = new ConsumerRecord<>("orders.events", 0, 0L, orderId, value);
                    Acknowledgment ack = mock(Acknowledgment.class);
                    startingGate.await();
                    listener.onOrderEvent(record, ack);
                    return null;
                });
            }

            List<Future<Void>> futures = new ArrayList<>();
            for (Callable<Void> task : tasks) {
                futures.add(executor.submit(task));
            }
            startingGate.countDown();
            for (Future<Void> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        Integer paymentCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM payments WHERE order_id = ?", Integer.class, orderId);
        assertThat(paymentCount).isEqualTo(1);

        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'PaymentAuthorized'",
                Integer.class,
                orderId);
        assertThat(outboxCount).isEqualTo(1);
    }

    private String buildOrderCreatedJson(String orderId, UUID eventId, long amountCents) throws Exception {
        EventEnvelope envelope = new EventEnvelope(
                eventId,
                "AuthorizePayment",
                1,
                orderId,
                UUID.randomUUID(),
                null,
                Instant.now(),
                mapper.createObjectNode().put("orderId", orderId).put("amountCents", amountCents));
        return mapper.writeValueAsString(envelope);
    }
}
