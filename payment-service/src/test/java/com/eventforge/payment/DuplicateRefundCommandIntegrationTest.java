package com.eventforge.payment;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.consumer.ConsumerOutcome;
import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.payment.domain.PaymentAuthorizationService;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Constitution item 4 / 8c: "a compensation command delivered twice refunds exactly once."
 * Compensations are exactly where teams forget their own idempotency rules — this proves
 * {@code RefundPayment} gets the SAME dedupe treatment as every M2 consumer, not a special case.
 *
 * <p>Same style as M2's {@code DuplicateDeliveryIntegrationTest}: the same {@code event_id}
 * delivered twice, first via {@code handleAuthorizePayment} (a real payment must exist to refund),
 * then {@code handleRefundPayment} called twice with an identical envelope.
 */
@SpringBootTest
class DuplicateRefundCommandIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
    }

    @Autowired
    private PaymentAuthorizationService paymentAuthorizationService;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void duplicateRefundCommandRefundsExactlyOnce() throws Exception {
        String orderId = "dup-refund-" + UUID.randomUUID();
        long amountCents = 6600;

        EventEnvelope authorize = envelope("AuthorizePayment", orderId, UUID.randomUUID(),
                mapper.createObjectNode().put("orderId", orderId).put("amountCents", amountCents));
        assertThat(paymentAuthorizationService.handleAuthorizePayment(authorize)).isEqualTo(ConsumerOutcome.PROCESSED);

        UUID refundEventId = UUID.randomUUID();
        EventEnvelope refund = envelope("RefundPayment", orderId, refundEventId,
                mapper.createObjectNode().put("orderId", orderId).put("amountCents", amountCents));

        ConsumerOutcome first = paymentAuthorizationService.handleRefundPayment(refund);
        ConsumerOutcome second = paymentAuthorizationService.handleRefundPayment(refund);

        assertThat(first).isEqualTo(ConsumerOutcome.PROCESSED);
        assertThat(second).isEqualTo(ConsumerOutcome.DUPLICATE);

        String status = jdbcTemplate.queryForObject("SELECT status FROM payments WHERE order_id = ?", String.class, orderId);
        assertThat(status).isEqualTo("REFUNDED");

        Integer refundOutboxCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'PaymentRefunded'",
                Integer.class,
                orderId);
        assertThat(refundOutboxCount).isEqualTo(1);

        Integer processedCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_events WHERE consumer_group = 'payment-service' AND event_id = ?",
                Integer.class,
                refundEventId);
        assertThat(processedCount).isEqualTo(1);
    }

    private EventEnvelope envelope(String eventType, String orderId, UUID eventId, com.fasterxml.jackson.databind.JsonNode payload) {
        return new EventEnvelope(eventId, eventType, 1, orderId, UUID.randomUUID(), UUID.randomUUID(), Instant.now(), payload);
    }
}
