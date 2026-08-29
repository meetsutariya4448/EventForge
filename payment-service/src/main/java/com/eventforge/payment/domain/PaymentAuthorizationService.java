package com.eventforge.payment.domain;

import com.eventforge.events.consumer.ConsumerOutcome;
import com.eventforge.events.consumer.ProcessedEventStore;
import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.outbox.OutboxEventRow;
import com.eventforge.events.outbox.OutboxWriter;
import com.eventforge.events.trace.TraceContextCapture;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The consumer transaction invariant (constitution Part 2), for real: dedupe-check, business
 * mutation, next outbox write, all inside one transaction — the same shape
 * {@code OrderService.createOrder} established for the write side in M1.
 *
 * <p>M3 adds the saga's compensation command, {@link #handleRefundPayment}, on the same footing.
 * Constitution item 4: "compensations are the place where teams forget their own idempotency
 * rules" — this one is protected at two layers, not one. See {@link #handleRefundPayment}'s
 * Javadoc and ADR-0016.
 *
 * <p>{@code consumerGroup} is a fixed constant matching this service's {@code @KafkaListener}
 * {@code groupId} — the dedupe key is {@code (consumer_group, event_id)}, never {@code event_id}
 * alone. See ADR-0012.
 */
@Service
public class PaymentAuthorizationService {

    public static final String CONSUMER_GROUP = "payment-service";

    private static final Logger log = LoggerFactory.getLogger(PaymentAuthorizationService.class);

    private final ProcessedEventStore processedEventStore;
    private final PaymentRepository paymentRepository;
    private final OutboxWriter outboxWriter;

    // Same reasoning as OrderService (ADR-0009): Spring Boot 4's own JacksonAutoConfiguration is
    // Jackson-3-shaped and doesn't register a com.fasterxml.jackson.databind.ObjectMapper bean.
    private final ObjectMapper objectMapper = EventEnvelopeMapper.create();

    public PaymentAuthorizationService(
            ProcessedEventStore processedEventStore, PaymentRepository paymentRepository, OutboxWriter outboxWriter) {
        this.processedEventStore = processedEventStore;
        this.paymentRepository = paymentRepository;
        this.outboxWriter = outboxWriter;
    }

    @Transactional
    public ConsumerOutcome handleAuthorizePayment(EventEnvelope envelope, String inboundTraceparent, String inboundTracestate) {
        boolean isNew = processedEventStore.tryMarkProcessed(CONSUMER_GROUP, envelope.eventId(), envelope.aggregateId());
        if (!isNew) {
            // A clean no-op, not an exception: nothing else in this transaction has run, so
            // letting it commit (there's nothing to commit) or roll back is equally harmless.
            // The caller still acknowledges the offset — see PaymentEventListener.
            return ConsumerOutcome.DUPLICATE;
        }

        String orderId = envelope.aggregateId();
        long amountCents = envelope.payload().get("amountCents").asLong();
        Instant now = Instant.now();

        UUID paymentId = UUID.randomUUID();
        Payment payment = new Payment(paymentId, orderId, amountCents, "AUTHORIZED", now, now);
        paymentRepository.save(payment);

        UUID nextEventId = UUID.randomUUID();
        String traceparent = TraceContextCapture.continueOrStart(inboundTraceparent);
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "paymentId", paymentId.toString(),
                "amountCents", amountCents,
                "status", "AUTHORIZED");

        outboxWriter.write(new OutboxEventRow(
                nextEventId,
                "Payment",
                orderId,
                1L, // PaymentAuthorized is always this order's first write; Payment's own
                    // nextSequence starts at 2 to account for exactly this.
                "PaymentAuthorized",
                1,
                envelope.correlationId(),
                envelope.eventId(),
                traceparent,
                inboundTracestate,
                writeJson(payload),
                now));

        return ConsumerOutcome.PROCESSED;
    }

    /**
     * The compensation command (constitution item 3's compensation path). Idempotent at two
     * layers, not one:
     *
     * <ol>
     *   <li><b>Event-level dedupe</b> (the {@code processedEventStore} check below): the exact
     *       same {@code event_id} delivered twice is a clean no-op — nothing runs a second time,
     *       nothing is written a second time. This is what constitution item 4's required test
     *       ("a compensation command delivered twice refunds exactly once") exercises directly.
     *   <li><b>Business-level safety net</b> (checking {@code payment.getStatus()}): a
     *       <em>different</em> {@code event_id} for an order whose payment is already
     *       {@code REFUNDED} — e.g. the orchestrator's timeout sweep redispatching RefundPayment
     *       because it never received the first {@code PaymentRefunded} fact — does not refund a
     *       second time either, but the fact IS re-announced (a fresh outbox write, using
     *       {@link Payment#allocateNextSequence()} since this order's payment row may already have
     *       more than one outbox event) so the orchestrator can still converge if the original
     *       announcement was the thing that never arrived, not just the command.
     * </ol>
     */
    @Transactional
    public ConsumerOutcome handleRefundPayment(EventEnvelope envelope, String inboundTraceparent, String inboundTracestate) {
        boolean isNew = processedEventStore.tryMarkProcessed(CONSUMER_GROUP, envelope.eventId(), envelope.aggregateId());
        if (!isNew) {
            return ConsumerOutcome.DUPLICATE;
        }

        String orderId = envelope.aggregateId();
        Instant now = Instant.now();
        String traceparent = TraceContextCapture.continueOrStart(inboundTraceparent);

        Payment payment = paymentRepository
                .findByOrderId(orderId)
                .orElseThrow(() -> new IllegalStateException("RefundPayment received for order " + orderId + " with no authorized payment on record"));

        if ("REFUNDED".equals(payment.getStatus())) {
            log.info(
                    "RefundPayment for order {} received but payment {} is already REFUNDED - business-level"
                            + " safety net, no second refund; re-announcing PaymentRefunded in case the original"
                            + " fact never reached the orchestrator",
                    orderId,
                    payment.getPaymentId());
            writePaymentRefunded(payment, envelope, traceparent, inboundTracestate, now);
            return ConsumerOutcome.PROCESSED;
        }

        payment.markRefunded(now);
        paymentRepository.save(payment);
        writePaymentRefunded(payment, envelope, traceparent, inboundTracestate, now);
        return ConsumerOutcome.PROCESSED;
    }

    private void writePaymentRefunded(
            Payment payment, EventEnvelope envelope, String traceparent, String inboundTracestate, Instant now) {
        UUID nextEventId = UUID.randomUUID();
        Map<String, Object> payload = Map.of(
                "orderId", payment.getOrderId(),
                "paymentId", payment.getPaymentId().toString(),
                "amountCents", payment.getAmountCents(),
                "status", "REFUNDED");
        outboxWriter.write(new OutboxEventRow(
                nextEventId,
                "Payment",
                payment.getOrderId(),
                payment.allocateNextSequence(),
                "PaymentRefunded",
                1,
                envelope.correlationId(),
                envelope.eventId(),
                traceparent,
                inboundTracestate,
                writeJson(payload),
                now));
    }

    private String writeJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize payment event payload", e);
        }
    }
}
