package com.eventforge.payment;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.payment.domain.PaymentAuthorizationService;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The WS3 spike, kept as a permanent test because the answer is load-bearing.
 *
 * <p>v2's durable failure capture depends on one piece of Spring Kafka behaviour: if the
 * recoverer throws — which is what a failure-store write failing would do — the offset must
 * <b>not</b> be committed, so the record is redelivered rather than silently lost. The whole
 * "capture strictly precedes offset advance" ordering rests on it. If instead Spring Kafka
 * committed the offset anyway, a failed capture would lose the message permanently and the
 * design would have to move to a custom {@code CommonErrorHandler} that controls the ack itself.
 *
 * <p>This asserts it directly rather than by inference: the poison record's exact partition and
 * offset are captured at send time, and the consumer group's committed offset is then read back
 * through {@link Admin}. Committed must not advance past the record the recoverer failed on.
 */
@SpringBootTest
class RecovererFailureLeavesOffsetUncommittedIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final String TOPIC = "orders.events";

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        // Short bounded retry so the recoverer is reached quickly; nothing here asserts on timing.
        registry.add("eventforge.consumer.resilience.max-retries", () -> "1");
        registry.add("eventforge.consumer.resilience.backoff-ms", () -> "100");
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
    }

    /**
     * Stands in for a failure store that is unavailable: the recoverer is reached and then fails.
     * Registered as a {@link ConsumerRecordRecoverer} bean, which suppresses the production
     * {@code LoggingConsumerRecordRecoverer} via its {@code @ConditionalOnMissingBean}.
     */
    static class ThrowingRecoverer implements ConsumerRecordRecoverer {

        private final AtomicInteger invocations = new AtomicInteger();

        @Override
        public void accept(ConsumerRecord<?, ?> record, Exception exception) {
            invocations.incrementAndGet();
            throw new IllegalStateException("simulated failure-store outage");
        }

        int invocations() {
            return invocations.get();
        }
    }

    @TestConfiguration
    static class ThrowingRecovererConfiguration {
        @Bean
        ThrowingRecoverer throwingRecoverer() {
            return new ThrowingRecoverer();
        }
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private ThrowingRecoverer recoverer;

    @Autowired
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    private final com.fasterxml.jackson.databind.ObjectMapper mapper =
            com.eventforge.events.envelope.EventEnvelopeMapper.create();

    @Test
    void whenTheRecovererThrowsTheOffsetIsNotCommittedPastTheFailedRecord() throws Exception {
        String key = "recoverer-failure-" + UUID.randomUUID();

        // Positive control, and it is the point of this step rather than setup noise. A test that
        // only observed "the offset did not advance" cannot distinguish the behaviour it wants
        // from a consumer that never reached the partition at all — a null committed offset would
        // satisfy that assertion for entirely the wrong reason. So: drive one GOOD record through
        // first, on the same key and therefore the same partition, and establish that this group
        // does commit here.
        SendResult<String, String> good = kafkaTemplate
                .send(MessageBuilder.withPayload(authorizePaymentJson(key))
                        .setHeader(KafkaHeaders.TOPIC, TOPIC)
                        .setHeader(KafkaHeaders.KEY, key)
                        .build())
                .get();
        TopicPartition partition =
                new TopicPartition(TOPIC, good.getRecordMetadata().partition());
        awaitPaymentFor(key);
        Long committedAfterGood = awaitCommittedAtLeast(partition, good.getRecordMetadata().offset() + 1);
        assertThat(committedAfterGood)
                .as("control: this group must genuinely commit on %s, or the real assertion proves nothing",
                        partition)
                .isNotNull();

        // Now the record whose "capture" fails.
        SendResult<String, String> poison = kafkaTemplate
                .send(MessageBuilder.withPayload("{not-valid-json-at-all")
                        .setHeader(KafkaHeaders.TOPIC, TOPIC)
                        .setHeader(KafkaHeaders.KEY, key)
                        .build())
                .get();
        long poisonOffset = poison.getRecordMetadata().offset();

        awaitRecovererInvoked();

        Long committed = committedOffset(partition);

        // Recorded, not just asserted: the concrete numbers are the spike's actual answer.
        System.out.printf(
                "SPIKE RESULT: partition=%s controlCommitted=%d poisonOffset=%d committedAfterFailedRecovery=%s "
                        + "recovererInvocations=%d%n",
                partition, committedAfterGood, poisonOffset, committed, recoverer.invocations());

        assertThat(committed)
                .as(
                        "recoverer threw on offset %d of %s; committed offset is %s. Were it %d, the "
                                + "offset would have advanced past a record whose capture failed and the "
                                + "message would be lost — WS3's capture-before-offset-advance ordering "
                                + "would then need a custom CommonErrorHandler that controls the ack itself.",
                        poisonOffset, partition, committed, poisonOffset + 1)
                .isNotNull()
                .isLessThanOrEqualTo(poisonOffset);
    }

    private String authorizePaymentJson(String orderId) throws Exception {
        com.eventforge.events.envelope.EventEnvelope envelope = new com.eventforge.events.envelope.EventEnvelope(
                UUID.randomUUID(),
                "AuthorizePayment",
                1,
                orderId,
                UUID.randomUUID(),
                null,
                java.time.Instant.now(),
                mapper.createObjectNode().put("orderId", orderId).put("amountCents", 1500));
        return mapper.writeValueAsString(envelope);
    }

    private void awaitPaymentFor(String orderId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM payments WHERE order_id = ?", Integer.class, orderId);
            if (count != null && count >= 1) {
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Control record was never processed; the consumer is not reaching this partition");
    }

    /** Waits for the group's committed offset to reach at least {@code target}. */
    private Long awaitCommittedAtLeast(TopicPartition partition, long target) throws Exception {
        long deadline = System.currentTimeMillis() + 60_000L;
        Long committed = null;
        while (System.currentTimeMillis() < deadline) {
            committed = committedOffset(partition);
            if (committed != null && committed >= target) {
                return committed;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Control: committed offset never reached " + target + " (was " + committed + ")");
    }

    /**
     * Waits until the recoverer has actually been reached, so the offset assertion is about a
     * failed recovery rather than a record still mid-retry.
     */
    private void awaitRecovererInvoked() throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            if (recoverer.invocations() > 0) {
                // Give the container a moment to do whatever it does after a failed recovery,
                // so this reads the settled position rather than one mid-flight.
                Thread.sleep(2_000L);
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("The recoverer was never reached; the poison record did not exhaust its retries");
    }

    private Long committedOffset(TopicPartition partition) throws Exception {
        try (Admin admin = Admin.create(
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(
                            PaymentAuthorizationService.CONSUMER_GROUP)
                    .partitionsToOffsetAndMetadata()
                    .get(30, java.util.concurrent.TimeUnit.SECONDS);
            OffsetAndMetadata metadata = offsets.get(partition);
            return metadata == null ? null : metadata.offset();
        }
    }
}
