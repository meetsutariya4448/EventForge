package com.eventforge.payment;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.payment.consumer.PaymentEventListener;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Item 4a: the same event delivered 100 times produces exactly one business effect, exactly one
 * resulting outbox event, and 100 acknowledged offsets — a duplicate is a clean no-op that still
 * advances the offset, not a stall.
 *
 * <p>Drives {@link PaymentEventListener#onOrderEvent} directly, 100 times, with the same
 * {@link ConsumerRecord} and a mock {@link Acknowledgment} — this simulates "the broker redelivered
 * this record 100 times" deterministically and fast, without depending on forcing real Kafka
 * redelivery 100 times over, while still exercising the real listener, real dedupe, and real
 * business/outbox code.
 */
@SpringBootTest
class DuplicateDeliveryIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final int DELIVERY_COUNT = 100;

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        // The real background relay must not drain payment-service's outbox mid-test and
        // interfere with the "exactly one outbox row" assertion below.
        registry.add("eventforge.outbox.relay.poll-interval-ms", () -> "3600000");
    }

    @Autowired
    private PaymentEventListener listener;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void deliveringTheSameEventOneHundredTimesProducesExactlyOneEffect() throws Exception {
        String orderId = "dup-100-" + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        ConsumerRecord<String, String> record = buildOrderCreatedRecord(orderId, eventId, 1500);

        for (int i = 0; i < DELIVERY_COUNT; i++) {
            Acknowledgment ack = mock(Acknowledgment.class);
            listener.onOrderEvent(record, ack);
            verify(ack, times(1)).acknowledge();
        }

        Integer paymentCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM payments WHERE order_id = ?", Integer.class, orderId);
        assertThat(paymentCount).isEqualTo(1);

        Integer outboxCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'PaymentAuthorized'",
                Integer.class,
                orderId);
        assertThat(outboxCount).isEqualTo(1);

        Integer processedCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_events WHERE consumer_group = 'payment-service' AND event_id = ?",
                Integer.class,
                eventId);
        assertThat(processedCount).isEqualTo(1);
    }

    private ConsumerRecord<String, String> buildOrderCreatedRecord(String orderId, UUID eventId, long amountCents)
            throws Exception {
        EventEnvelope envelope = new EventEnvelope(
                eventId,
                "AuthorizePayment",
                1,
                orderId,
                UUID.randomUUID(),
                null,
                Instant.now(),
                mapper.createObjectNode().put("orderId", orderId).put("amountCents", amountCents));
        String value = mapper.writeValueAsString(envelope);

        ConsumerRecord<String, String> record = new ConsumerRecord<>("orders.events", 0, 0L, orderId, value);
        record.headers()
                .add(new RecordHeader(
                        "traceparent",
                        "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".getBytes(StandardCharsets.UTF_8)));
        return record;
    }
}
