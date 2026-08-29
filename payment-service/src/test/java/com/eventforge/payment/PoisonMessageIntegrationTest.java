package com.eventforge.payment;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.consumer.LoggingConsumerRecordRecoverer;
import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
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
 * Item 4e: DLQ design is M5's — this only asserts what M2 is actually responsible for: a poison
 * message does not stall its partition, and its failure is visible (an ERROR log plus
 * {@link LoggingConsumerRecordRecoverer}'s counter, checkable directly rather than scraped from
 * logs) and bounded (a fixed number of retries, then skipped — no retry topics, no infinite
 * redelivery loop).
 *
 * <p>The poison record and the good record that follows it share the same Kafka key, so Kafka's
 * default partitioner sends them to the same partition — proving the specific partition the
 * poison message landed on keeps moving, not just that some other, unaffected partition did.
 */
@SpringBootTest
class PoisonMessageIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        // Short, bounded retry/backoff so this test's real (unavoidable) wait for Spring Kafka's
        // own retry-then-skip mechanism stays small — no assertion here depends on the exact
        // duration, only on the eventual, bounded outcome.
        registry.add("eventforge.consumer.resilience.max-retries", () -> "2");
        registry.add("eventforge.consumer.resilience.backoff-ms", () -> "200");
        registry.add("eventforge.outbox.relay.poll-interval-ms", () -> "3600000");
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private LoggingConsumerRecordRecoverer recoverer;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void aPoisonMessageIsSkippedVisiblyAndTheGoodMessageBehindItStillProcesses() throws Exception {
        String key = "poison-" + UUID.randomUUID();
        String orderId = key; // same key -> same partition for both records
        int recoveredBefore = recoverer.recoveredCount();

        // Not valid EventEnvelope JSON at all - a genuinely poisoned record, not just an
        // application-level validation failure.
        kafkaTemplate.send(MessageBuilder.withPayload("{not-valid-json-at-all")
                        .setHeader(KafkaHeaders.TOPIC, "orders.events")
                        .setHeader(KafkaHeaders.KEY, key)
                        .build())
                .get();

        UUID goodEventId = UUID.randomUUID();
        EventEnvelope goodEnvelope = new EventEnvelope(
                goodEventId,
                "OrderCreated",
                1,
                orderId,
                UUID.randomUUID(),
                null,
                Instant.now(),
                mapper.createObjectNode().put("orderId", orderId).put("amountCents", 1250));
        kafkaTemplate.send(MessageBuilder.withPayload(mapper.writeValueAsString(goodEnvelope))
                        .setHeader(KafkaHeaders.TOPIC, "orders.events")
                        .setHeader(KafkaHeaders.KEY, key)
                        .build())
                .get();

        waitForPayment(orderId);

        Integer paymentCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM payments WHERE order_id = ?", Integer.class, orderId);
        assertThat(paymentCount).isEqualTo(1);

        // Visible: the recoverer (ERROR-logging, per constitution item 5's "document what an
        // operator observes") ran at least once for the poison record.
        assertThat(recoverer.recoveredCount()).isGreaterThan(recoveredBefore);
    }

    /** Bounded wait for the real consumer to work through the poison record's retries and reach
     * the good one behind it — not a correctness assertion on how long that takes. */
    private void waitForPayment(String orderId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM payments WHERE order_id = ?", Integer.class, orderId);
            if (count != null && count >= 1) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Timed out waiting for the good message behind the poison one to be processed");
    }
}
