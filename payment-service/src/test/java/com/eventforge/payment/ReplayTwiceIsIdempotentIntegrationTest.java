package com.eventforge.payment;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.failure.FailedMessageReplayer;
import com.eventforge.events.failure.FailedMessageReplayer.ReplayOutcome;
import com.eventforge.events.failure.FailedMessageRow;
import com.eventforge.events.failure.FailedMessageStatus;
import com.eventforge.events.failure.FailedMessageStore;
import com.eventforge.payment.domain.PaymentAuthorizationService;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Replaying a captured message twice must not produce the effect twice — and this is asserted at
 * both of the two independent layers that make that true, because each protects against a
 * different failure and neither alone is sufficient.
 *
 * <ol>
 *   <li><b>The claim.</b> {@code claimForReplay} uses {@code FOR UPDATE SKIP LOCKED} and a status
 *       check, so two operators clicking replay on the same row produce one publish and one
 *       "already handled" — the second is refused rather than queued behind the first to duplicate
 *       it.
 *   <li><b>The dedupe.</b> Even when a second publish does happen — which it legitimately can,
 *       since a crash between the broker's acknowledgement and the row being marked leaves the row
 *       replayable — the message carries its original {@code eventId}, so the consumer's
 *       {@code (consumer_group, event_id)} ledger (ADR-0012) absorbs it as a no-op.
 * </ol>
 *
 * The second case is the one that matters for the honest claim: delivery here is at-least-once,
 * and the effect is once because of the dedupe, not because delivery was ever exactly-once.
 *
 * <p>The captured rows are written through {@link FailedMessageStore} directly rather than by
 * poisoning a real record. What is under test is the replay path, and driving a genuine failure
 * would additionally require a message that fails once and then succeeds — a condition the test
 * would have to fabricate anyway, with less control over the payload being replayed.
 */
@SpringBootTest
class ReplayTwiceIsIdempotentIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final String TOPIC = "orders.events";

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
    }

    @Autowired
    private FailedMessageStore store;

    @Autowired
    private FailedMessageReplayer replayer;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void aSecondReplayOfTheSameRowIsRefusedAndTheEffectHappensOnce() throws Exception {
        String orderId = "replay-claim-" + UUID.randomUUID();
        UUID failedMessageId = captureAuthorizePayment(orderId, UUID.randomUUID());

        assertThat(replayer.replay(failedMessageId)).isEqualTo(ReplayOutcome.REPLAYED);
        assertThat(replayer.replay(failedMessageId))
                .as("the row is REPLAYED, so a second click has nothing to claim")
                .isEqualTo(ReplayOutcome.NOT_CLAIMABLE);

        awaitPaymentFor(orderId);
        assertThat(paymentCount(orderId)).isEqualTo(1);

        FailedMessageRow row = store.find(failedMessageId).orElseThrow();
        assertThat(row.status()).isEqualTo(FailedMessageStatus.REPLAYED);
        assertThat(row.replayAttempts())
                .as("one claim, therefore one attempt: the refused call must not have counted")
                .isEqualTo(1);
    }

    @Test
    void aGenuinelyDuplicatedReplayStillProducesOnePayment() throws Exception {
        String orderId = "replay-dupe-" + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        UUID failedMessageId = captureAuthorizePayment(orderId, eventId);

        assertThat(replayer.replay(failedMessageId)).isEqualTo(ReplayOutcome.REPLAYED);
        awaitPaymentFor(orderId);

        // Stand in for the crash window the replayer deliberately leaves open: the broker
        // acknowledged, the process died before the row was marked, so the row is replayable
        // again and the same message is published a second time. Forced here rather than waited
        // for, because that window is real but not reproducible on demand.
        jdbcTemplate.update(
                "UPDATE failed_messages SET status = 'CAPTURED' WHERE failed_message_id = ?", failedMessageId);

        assertThat(replayer.replay(failedMessageId)).isEqualTo(ReplayOutcome.REPLAYED);

        // A no-op is the absence of an effect, so give the second delivery real time to have
        // produced one before concluding it did not.
        Thread.sleep(5_000L);

        assertThat(paymentCount(orderId))
                .as("the republished bytes carry the original eventId, so the dedupe ledger absorbs "
                        + "the duplicate — delivery is at-least-once, the effect is once")
                .isEqualTo(1);

        Integer processed = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_events WHERE consumer_group = ? AND event_id = ?",
                Integer.class,
                PaymentAuthorizationService.CONSUMER_GROUP,
                eventId);
        assertThat(processed)
                .as("one ledger row, not two: the second delivery conflicted and did nothing")
                .isEqualTo(1);
    }

    /** Writes a captured failure whose payload is a genuine, replayable AuthorizePayment. */
    private UUID captureAuthorizePayment(String orderId, UUID eventId) throws Exception {
        EventEnvelope envelope = new EventEnvelope(
                eventId,
                "AuthorizePayment",
                1,
                orderId,
                UUID.randomUUID(),
                null,
                Instant.now(),
                mapper.createObjectNode().put("orderId", orderId).put("amountCents", 4200));
        UUID failedMessageId = UUID.randomUUID();
        boolean captured = store.capture(new FailedMessageRow(
                failedMessageId,
                PaymentAuthorizationService.CONSUMER_GROUP,
                TOPIC,
                0,
                // A coordinate no real record in this container will collide with, since the
                // capture's unique index is on (group, topic, partition, offset).
                ThreadLocalRandom.current().nextLong(1_000_000L, Long.MAX_VALUE),
                orderId,
                mapper.writeValueAsString(envelope),
                "{\"traceparent\": \"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01\"}",
                eventId,
                "AuthorizePayment",
                "java.lang.IllegalStateException: simulated prior failure",
                FailedMessageStatus.CAPTURED,
                Instant.now(),
                0,
                null,
                null));
        assertThat(captured).as("test setup: the capture row must actually have been created").isTrue();
        return failedMessageId;
    }

    private Integer paymentCount(String orderId) {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM payments WHERE order_id = ?", Integer.class, orderId);
    }

    private void awaitPaymentFor(String orderId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 60_000L;
        while (System.currentTimeMillis() < deadline) {
            Integer count = paymentCount(orderId);
            if (count != null && count >= 1) {
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("The replayed message never reached the consumer");
    }
}
