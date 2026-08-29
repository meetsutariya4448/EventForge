package com.eventforge.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.payment.consumer.PaymentEventListener;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.eventforge.testing.fault.ConfigurableFaultInjector;
import com.eventforge.testing.fault.FaultInjectionTestConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Item 4c: crash after the business transaction commits, before the offset is acknowledged — the
 * M0 fault-injection harness's {@link FaultInjectionPoint#AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK}
 * seam, wired to a real call site for the first time in this project.
 *
 * <p>A real crash here means the JVM dies — Kafka's broker never learns the offset was committed,
 * and the SAME record is redelivered on restart. That's simulated by catching the injected
 * exception directly in the test (standing in for "the process died and something external
 * restarted it"), then calling the listener again with the fault disarmed, exactly as
 * {@code OutboxRelayCrashWindowIntegrationTest} treated "restart" as simply calling the same
 * stateless bean again.
 */
@SpringBootTest
@Import(FaultInjectionTestConfiguration.class)
class CrashAfterCommitBeforeAckIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.poll-interval-ms", () -> "3600000");
    }

    @Autowired
    private PaymentEventListener listener;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private FaultInjector faultInjector;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @AfterEach
    void clearArmedFaults() {
        ((ConfigurableFaultInjector) faultInjector).clear();
    }

    @Test
    void redeliveryAfterACrashBeforeAckProducesZeroAdditionalBusinessEffect() throws Exception {
        String orderId = "crash-before-ack-" + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        ConsumerRecord<String, String> record = buildOrderCreatedRecord(orderId, eventId, 750);

        AtomicBoolean fired = new AtomicBoolean(false);
        ((ConfigurableFaultInjector) faultInjector)
                .registerAction(FaultInjectionPoint.AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK, () -> {
                    if (fired.compareAndSet(false, true)) {
                        throw new IllegalStateException(
                                "simulated crash: business committed, offset never acknowledged");
                    }
                });

        // First delivery: the business transaction commits for real (payment authorized, outbox
        // row written), then the process "crashes" right before it would have acked the offset.
        Acknowledgment firstAck = mock(Acknowledgment.class);
        assertThatThrownBy(() -> listener.onOrderEvent(record, firstAck)).isInstanceOf(IllegalStateException.class);
        verify(firstAck, never()).acknowledge();

        Integer paymentCountAfterCrash = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM payments WHERE order_id = ?", Integer.class, orderId);
        assertThat(paymentCountAfterCrash).isEqualTo(1);

        // "Redelivery": Kafka never saw the offset committed, so the same record comes again.
        // Nothing crashes this time.
        Acknowledgment secondAck = mock(Acknowledgment.class);
        listener.onOrderEvent(record, secondAck);
        verify(secondAck, times(1)).acknowledge();

        // Zero additional business effect: dedupe absorbed the redelivery as a no-op.
        Integer paymentCountAfterRedelivery = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM payments WHERE order_id = ?", Integer.class, orderId);
        assertThat(paymentCountAfterRedelivery).isEqualTo(1);

        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'PaymentAuthorized'",
                Integer.class,
                orderId);
        assertThat(outboxCount).isEqualTo(1);
    }

    private ConsumerRecord<String, String> buildOrderCreatedRecord(String orderId, UUID eventId, long amountCents)
            throws Exception {
        EventEnvelope envelope = new EventEnvelope(
                eventId,
                "OrderCreated",
                1,
                orderId,
                UUID.randomUUID(),
                null,
                Instant.now(),
                mapper.createObjectNode().put("orderId", orderId).put("amountCents", amountCents));
        String value = mapper.writeValueAsString(envelope);
        return new ConsumerRecord<>("orders.events", 0, 0L, orderId, value);
    }
}
