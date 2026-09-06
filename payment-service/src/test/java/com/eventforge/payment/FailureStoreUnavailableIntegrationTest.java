package com.eventforge.payment;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.failure.FailedMessageRow;
import com.eventforge.events.failure.FailedMessageStore;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.payment.domain.PaymentAuthorizationService;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.eventforge.testing.fault.ConfigurableFaultInjector;
import com.eventforge.testing.fault.FaultInjectionTestConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.support.MessageBuilder;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * What happens when the failure store itself is unavailable — the case that decides whether
 * durable capture is a real guarantee or only a convenience.
 *
 * <p>The wrong answer would be to skip the record: the offset advances, the message is gone, and
 * nothing remembers it, which is strictly worse than the log-and-skip behaviour capture replaced,
 * because now an operator has a console that says everything is fine. The right answer is to stall
 * loudly: the exception propagates out of the recoverer, the offset stays put, and the record is
 * redelivered until the store comes back.
 *
 * <p>Both halves are asserted here rather than only the first. A test that showed only "the offset
 * did not advance" would be satisfied by a system that had permanently wedged. So the fault is
 * then disarmed — the store "comes back" — and the record must be captured and the partition must
 * resume on its own, with no intervention and no restart.
 */
@SpringBootTest
@Import(FaultInjectionTestConfiguration.class)
class FailureStoreUnavailableIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final String TOPIC = "orders.events";

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.consumer.resilience.max-retries", () -> "1");
        registry.add("eventforge.consumer.resilience.backoff-ms", () -> "100");
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private FailedMessageStore store;

    @Autowired
    private FaultInjector faultInjector;

    @Autowired
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @AfterEach
    void clearArmedFaults() {
        ((ConfigurableFaultInjector) faultInjector).clear();
    }

    @Test
    void whileTheStoreIsDownNothingIsCapturedAndNothingIsSkipped() throws Exception {
        String key = "store-down-" + UUID.randomUUID();
        String payload = "{not-valid-json-at-all:" + key;

        AtomicBoolean storeIsDown = new AtomicBoolean(true);
        AtomicInteger captureAttempts = new AtomicInteger();
        ((ConfigurableFaultInjector) faultInjector).registerAction(FaultInjectionPoint.BEFORE_FAILURE_CAPTURE, () -> {
            captureAttempts.incrementAndGet();
            if (storeIsDown.get()) {
                throw new IllegalStateException("simulated failure-store outage");
            }
        });

        SendResult<String, String> sent = kafkaTemplate
                .send(MessageBuilder.withPayload(payload)
                        .setHeader(KafkaHeaders.TOPIC, TOPIC)
                        .setHeader(KafkaHeaders.KEY, key)
                        .build())
                .get();
        TopicPartition partition = new TopicPartition(TOPIC, sent.getRecordMetadata().partition());
        long offset = sent.getRecordMetadata().offset();

        awaitCaptureAttempted(captureAttempts);
        // Let the container settle so this reads a resting position, not one mid-retry.
        Thread.sleep(2_000L);

        assertThat(findByKey(key))
                .as("the capture failed, so there must be no row claiming this record was handled")
                .isEmpty();

        // captureAttempts > 0 above is this assertion's positive control: it proves the consumer
        // genuinely reached this record and exhausted its retries, so a committed offset that has
        // not moved past it means "not skipped" rather than "never got here".
        Long committedWhileDown = committedOffset(partition);
        assertThat(committedWhileDown)
                .as("the record must not be skipped while nothing durable records it; committed=%s, record=%d",
                        committedWhileDown, offset)
                .satisfiesAnyOf(
                        c -> assertThat(c).isNull(), c -> assertThat(c).isLessThanOrEqualTo(offset));

        // The store comes back. No restart, no manual replay, no operator action.
        storeIsDown.set(false);

        FailedMessageRow captured = awaitCapture(key);
        assertThat(captured.offset())
                .as("the redelivered record is the same one, captured at its original coordinates")
                .isEqualTo(offset);
        assertThat(captured.payload()).isEqualTo(payload);

        // The partition resumed on its own — shown by a record behind the captured one being
        // processed, since a recovered record commits no offset of its own under manual ack.
        kafkaTemplate
                .send(MessageBuilder.withPayload(authorizePaymentJson(key))
                        .setHeader(KafkaHeaders.TOPIC, TOPIC)
                        .setHeader(KafkaHeaders.KEY, key)
                        .build())
                .get();
        awaitPaymentFor(key);

        Long committedAfterRecovery = awaitCommittedAtLeast(partition, offset + 1);
        assertThat(committedAfterRecovery)
                .as("once the record is durably captured the partition resumes by itself, with no "
                        + "restart and no operator action")
                .isGreaterThan(offset);
    }

    private String authorizePaymentJson(String orderId) throws Exception {
        EventEnvelope envelope = new EventEnvelope(
                UUID.randomUUID(),
                "AuthorizePayment",
                1,
                orderId,
                UUID.randomUUID(),
                null,
                Instant.now(),
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
        throw new AssertionError("The record behind the captured one was never processed: the partition is stalled");
    }

    private void awaitCaptureAttempted(AtomicInteger attempts) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            if (attempts.get() > 0) {
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("Capture was never attempted; the record did not exhaust its retries");
    }

    private Optional<FailedMessageRow> findByKey(String key) {
        List<FailedMessageRow> open = store.findOpen(200);
        return open.stream().filter(row -> key.equals(row.messageKey())).findFirst();
    }

    private FailedMessageRow awaitCapture(String key) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            Optional<FailedMessageRow> row = findByKey(key);
            if (row.isPresent()) {
                return row.get();
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("The record was never captured after the store recovered");
    }

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
        throw new AssertionError("Committed offset never reached " + target + " (was " + committed + ")");
    }

    private Long committedOffset(TopicPartition partition) throws Exception {
        try (Admin admin =
                Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
            Map<TopicPartition, OffsetAndMetadata> offsets = admin.listConsumerGroupOffsets(
                            PaymentAuthorizationService.CONSUMER_GROUP)
                    .partitionsToOffsetAndMetadata()
                    .get(30, TimeUnit.SECONDS);
            OffsetAndMetadata metadata = offsets.get(partition);
            return metadata == null ? null : metadata.offset();
        }
    }
}
