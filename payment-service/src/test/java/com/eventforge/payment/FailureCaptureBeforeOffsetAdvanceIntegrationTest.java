package com.eventforge.payment;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.failure.FailedMessageRow;
import com.eventforge.events.failure.FailedMessageStatus;
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
 * v2 WS3: a record that exhausts its retries is no longer logged and forgotten — it is a row an
 * operator can find, and the offset only moves past it once that row is committed.
 *
 * <p>This is the second half of an argument whose first half is {@code RecovererFailureLeaves
 * OffsetUncommittedIntegrationTest}. That one proved the negative direction: when the capture
 * fails, the offset does not advance. This proves the positive: when the capture succeeds, the row
 * is there and the offset does advance past it. Neither alone establishes the ordering; the pair
 * does — there is no observable state in which the offset has moved and no row exists.
 *
 * <p>The payload assertion is byte-for-byte deliberately. Replay's whole safety argument is that
 * the republished message is the original one, carrying its original {@code eventId} into the
 * consumer-side dedupe (ADR-0012) and its original {@code traceparent} into the trace it belonged
 * to. A payload that came back normalized would break both silently.
 */
@SpringBootTest
@Import(FaultInjectionTestConfiguration.class)
class FailureCaptureBeforeOffsetAdvanceIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

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
    private org.springframework.jdbc.core.JdbcTemplate jdbcTemplate;

    @Autowired
    private FaultInjector faultInjector;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @AfterEach
    void clearArmedFaults() {
        ((ConfigurableFaultInjector) faultInjector).clear();
    }

    @Test
    void aPoisonRecordIsCapturedDurablyAndOnlyThenDoesTheOffsetMovePastIt() throws Exception {
        // Unique enough to identify this test's own row among any left by sibling tests sharing
        // the container, without truncating a table other tests may be using concurrently.
        String key = "capture-" + UUID.randomUUID();
        // Not valid JSON at all, matching PoisonMessageIntegrationTest: a record that merely fails
        // application-level validation gets acknowledged by the listener and never reaches a
        // recoverer, so it would not exercise capture.
        String payload = "{not-valid-json-at-all:" + key;

        SendResult<String, String> sent = kafkaTemplate
                .send(MessageBuilder.withPayload(payload)
                        .setHeader(KafkaHeaders.TOPIC, TOPIC)
                        .setHeader(KafkaHeaders.KEY, key)
                        .setHeader("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                        .build())
                .get();
        TopicPartition partition = new TopicPartition(TOPIC, sent.getRecordMetadata().partition());
        long offset = sent.getRecordMetadata().offset();

        FailedMessageRow captured = awaitCapture(key);

        assertThat(captured.consumerGroup())
                .as("the group is read from the Consumer that actually failed, not from configuration "
                        + "that could drift from the @KafkaListener's groupId")
                .isEqualTo(PaymentAuthorizationService.CONSUMER_GROUP);
        assertThat(captured.topic()).isEqualTo(TOPIC);
        assertThat(captured.partition()).isEqualTo(partition.partition());
        assertThat(captured.offset()).isEqualTo(offset);
        assertThat(captured.messageKey()).isEqualTo(key);
        assertThat(captured.status()).isEqualTo(FailedMessageStatus.CAPTURED);

        assertThat(captured.payload())
                .as("replay must republish the original bytes; anything normalized here breaks the "
                        + "eventId dedupe and the trace continuity that make a replay safe")
                .isEqualTo(payload);

        assertThat(captured.eventId())
                .as("this record is not a parseable envelope — precisely why it failed — so it is "
                        + "identified by its physical coordinates instead")
                .isNull();
        assertThat(captured.failureReason()).isNotBlank();

        assertThat(captured.headersJson())
                .as("headers are kept verbatim so a replay continues its original trace")
                .contains("traceparent")
                .contains("4bf92f3577b34da6a3ce929d0e0e4736");

        // Only now — after the row exists — is the partition allowed past the record. Shown with a
        // good record on the same key, and therefore the same partition, rather than by reading
        // the committed offset directly: under manual ack a recovered record does not commit an
        // offset of its own, so a bare offset read here returns null and would prove nothing
        // either way. A record behind the poison one being processed is the observable fact that
        // the partition is not stalled, and it makes the offset read below meaningful.
        kafkaTemplate
                .send(MessageBuilder.withPayload(authorizePaymentJson(key))
                        .setHeader(KafkaHeaders.TOPIC, TOPIC)
                        .setHeader(KafkaHeaders.KEY, key)
                        .build())
                .get();
        awaitPaymentFor(key);

        Long committed = awaitCommittedAtLeast(partition, offset + 1);
        assertThat(committed)
                .as("with the record captured, the partition kept moving rather than stalling")
                .isGreaterThan(offset);
    }

    /**
     * The crash window on the other side of the capture: the row is committed, and the process
     * dies before the offset can move past the record. Redelivery then re-runs the capture.
     *
     * <p>This is why capture is keyed on the record's physical coordinates rather than on
     * {@code eventId}. A poison record often has no parseable {@code eventId} at all, so keying on
     * it would either fail outright or let every redelivery accumulate another row — turning one
     * broken message into an operator's list of dozens of copies of itself.
     */
    @Test
    void aCrashBetweenCaptureAndOffsetAdvanceLeavesExactlyOneRowAfterRedelivery() throws Exception {
        String key = "recapture-" + UUID.randomUUID();
        String payload = "{not-valid-json-at-all:" + key;

        AtomicBoolean crashArmed = new AtomicBoolean(true);
        AtomicInteger passes = new AtomicInteger();
        ((ConfigurableFaultInjector) faultInjector)
                .registerAction(FaultInjectionPoint.AFTER_FAILURE_CAPTURE_BEFORE_OFFSET_ADVANCE, () -> {
                    passes.incrementAndGet();
                    if (crashArmed.getAndSet(false)) {
                        throw new IllegalStateException("simulated crash after capture, before offset advance");
                    }
                });

        SendResult<String, String> sent = kafkaTemplate
                .send(MessageBuilder.withPayload(payload)
                        .setHeader(KafkaHeaders.TOPIC, TOPIC)
                        .setHeader(KafkaHeaders.KEY, key)
                        .build())
                .get();
        long offset = sent.getRecordMetadata().offset();

        // Two passes: the one that crashed, and the redelivery that got through.
        awaitAtLeast(passes, 2);
        Thread.sleep(2_000L);

        List<FailedMessageRow> matching = store.findOpen(500).stream()
                .filter(row -> key.equals(row.messageKey()))
                .toList();
        assertThat(matching)
                .as("the redelivered record must collapse onto the row already captured for its "
                        + "coordinates, not add a second copy of the same failure")
                .hasSize(1);
        assertThat(matching.get(0).offset()).isEqualTo(offset);
    }

    private void awaitAtLeast(AtomicInteger counter, int target) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            if (counter.get() >= target) {
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError(
                "Capture reached the post-capture seam only " + counter.get() + " times, expected " + target);
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

    private FailedMessageRow awaitCapture(String key) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            List<FailedMessageRow> open = store.findOpen(200);
            for (FailedMessageRow row : open) {
                if (key.equals(row.messageKey())) {
                    return row;
                }
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("The poison record was never captured in failed_messages");
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
