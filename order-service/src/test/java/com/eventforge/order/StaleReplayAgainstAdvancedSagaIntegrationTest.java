package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.failure.FailedMessageReplayer;
import com.eventforge.events.failure.FailedMessageReplayer.ReplayOutcome;
import com.eventforge.events.failure.FailedMessageRow;
import com.eventforge.events.failure.FailedMessageStatus;
import com.eventforge.events.failure.FailedMessageStore;
import com.eventforge.events.outbox.OutboxRelayWorker;
import com.eventforge.events.outbox.RelayOutcome;
import com.eventforge.order.domain.Order;
import com.eventforge.order.domain.OrderService;
import com.eventforge.order.saga.SagaOrchestrator;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
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
 * The question replay actually raises in production: an operator finds a failure from some time
 * ago and clicks replay — but the world moved on while it sat there. Here a captured
 * {@code PaymentAuthorized} is replayed into a saga that has already advanced past
 * {@code AWAITING_PAYMENT}.
 *
 * <p>The consumer-side dedupe does not save this case, and it is important to be precise about
 * why: the captured message never completed processing, so its business transaction rolled back
 * and took its {@code processed_events} row with it. Its {@code event_id} is therefore genuinely
 * unseen, the ledger lets it through, and it reaches the orchestrator as a first delivery.
 *
 * <p>What stops it is the second layer — ADR-0016's rule that every handler refuses to act unless
 * the saga is in the state that fact belongs to. {@code SagaOrchestrationIntegrationTest} already
 * proves that guard against a synthetic stale fact; this proves that the replay path is subject to
 * it too, which is the property that makes an operator-triggered replay safe to expose as a button
 * rather than something requiring a human to first reason about saga state.
 *
 * <p>Note the deliberate asymmetry in the assertions: the replay is recorded as {@code REPLAYED}
 * even though it changed nothing. That is correct and worth stating — the row tracks whether the
 * message was republished, not whether it turned out to be actionable. Reporting it as failed
 * would tell an operator to retry something that will never do anything.
 */
@SpringBootTest
class StaleReplayAgainstAdvancedSagaIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        registry.add("eventforge.saga.sweep-interval-ms", () -> "3600000");
    }

    @Autowired
    private OrderService orderService;

    @Autowired
    private OutboxRelayWorker relayWorker;

    @Autowired
    private FailedMessageStore store;

    @Autowired
    private FailedMessageReplayer replayer;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void replayingAFactIntoASagaThatHasMovedOnChangesNothing() throws Exception {
        drainAllPending();

        Order order = orderService.createOrder(5000, "sku-stale-replay", 1L);
        UUID orderId = order.getOrderId();
        drainAllPending();
        assertThat(sagaState(orderId)).isEqualTo("AWAITING_PAYMENT");

        // A PaymentAuthorized that failed to process and was captured. Its event_id was never
        // marked processed — the rollback that sent it here took that row with it — so the dedupe
        // ledger will not stop its replay.
        UUID strandedEventId = UUID.randomUUID();
        UUID failedMessageId = capturePaymentAuthorized(orderId, strandedEventId);
        assertThat(processedEventCount(strandedEventId))
                .as("precondition: this event is genuinely unseen, so the dedupe layer is not what "
                        + "the assertions below are measuring")
                .isZero();

        // Meanwhile the saga advances on a different PaymentAuthorized — the same fact, delivered
        // successfully by some other means (a redispatch answered, an operator's manual step).
        publishPaymentAuthorized(orderId, UUID.randomUUID());
        waitForSagaState(orderId, "AWAITING_INVENTORY", Duration.ofSeconds(20));
        drainAllPending();

        long reserveStepsBefore = reserveInventoryStepCount(orderId);
        assertThat(reserveStepsBefore).isEqualTo(1L);

        // Now the operator replays the stranded message.
        assertThat(replayer.replay(failedMessageId)).isEqualTo(ReplayOutcome.REPLAYED);

        // The replayed fact must actually be delivered and processed before "nothing changed" is
        // worth anything — otherwise this asserts only that a slow message had not arrived yet.
        awaitProcessed(strandedEventId);

        assertThat(sagaState(orderId))
                .as("the saga is past AWAITING_PAYMENT, so the handler's state guard makes this a no-op")
                .isEqualTo("AWAITING_INVENTORY");
        assertThat(reserveInventoryStepCount(orderId))
                .as("a second ReserveInventory dispatch here would mean the inventory service was "
                        + "asked twice to reserve the same stock")
                .isEqualTo(reserveStepsBefore);
        assertThat(orderStatus(orderId)).isEqualTo("PENDING");

        FailedMessageRow row = store.find(failedMessageId).orElseThrow();
        assertThat(row.status())
                .as("the row records that the message was republished, which it was; whether the "
                        + "system acted on it is a different question and not this row's to answer")
                .isEqualTo(FailedMessageStatus.REPLAYED);
    }

    /** Writes a captured PaymentAuthorized as though its processing had failed and rolled back. */
    private UUID capturePaymentAuthorized(UUID orderId, UUID eventId) throws Exception {
        UUID failedMessageId = UUID.randomUUID();
        boolean captured = store.capture(new FailedMessageRow(
                failedMessageId,
                SagaOrchestrator.CONSUMER_GROUP,
                "payments.events",
                0,
                ThreadLocalRandom.current().nextLong(1_000_000L, Long.MAX_VALUE),
                orderId.toString(),
                paymentAuthorizedJson(orderId, eventId),
                null,
                eventId,
                "PaymentAuthorized",
                "java.lang.IllegalStateException: simulated prior failure",
                FailedMessageStatus.CAPTURED,
                Instant.now(),
                0,
                null,
                null));
        assertThat(captured).as("test setup: the capture row must actually have been created").isTrue();
        return failedMessageId;
    }

    private void publishPaymentAuthorized(UUID orderId, UUID eventId) throws Exception {
        kafkaTemplate
                .send(MessageBuilder.withPayload(paymentAuthorizedJson(orderId, eventId))
                        .setHeader(KafkaHeaders.TOPIC, "payments.events")
                        .setHeader(KafkaHeaders.KEY, orderId.toString())
                        .build())
                .get();
    }

    private String paymentAuthorizedJson(UUID orderId, UUID eventId) throws Exception {
        EventEnvelope envelope = new EventEnvelope(
                eventId,
                "PaymentAuthorized",
                1,
                orderId.toString(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                Instant.now(),
                mapper.valueToTree(Map.of(
                        "orderId", orderId.toString(),
                        "paymentId", UUID.randomUUID().toString(),
                        "amountCents", 5000,
                        "status", "AUTHORIZED")));
        return mapper.writeValueAsString(envelope);
    }

    private Integer processedEventCount(UUID eventId) {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_events WHERE consumer_group = ? AND event_id = ?",
                Integer.class,
                SagaOrchestrator.CONSUMER_GROUP,
                eventId);
    }

    private void awaitProcessed(UUID eventId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            Integer count = processedEventCount(eventId);
            if (count != null && count >= 1) {
                // The ledger row and the (absent) transition commit together, so by the time this
                // is visible the handler has finished deciding.
                return;
            }
            Thread.sleep(200L);
        }
        throw new AssertionError("The replayed fact never reached the orchestrator");
    }

    private long reserveInventoryStepCount(UUID orderId) {
        Long count = jdbcTemplate.queryForObject(
                """
                SELECT count(*) FROM saga_step s JOIN saga_instance i ON s.saga_id = i.saga_id
                WHERE i.order_id = ? AND s.step_name = 'ReserveInventory'
                """,
                Long.class,
                orderId);
        return count == null ? 0L : count;
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
            Thread.sleep(200L);
        }
        throw new AssertionError("Timed out waiting for saga on order " + orderId + " to reach " + expected + " (was "
                + sagaState(orderId) + ")");
    }

    private void drainAllPending() {
        int guard = 0;
        while (relayWorker.relayNextEvent() != RelayOutcome.NOTHING_TO_CLAIM && guard++ < 500) {
            // keep draining
        }
    }
}
