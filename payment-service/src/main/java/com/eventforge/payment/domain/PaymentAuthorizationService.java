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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * The consumer transaction invariant (constitution Part 2), for real: dedupe-check, business
 * mutation, next outbox write, all inside one transaction — the same shape
 * {@code OrderService.createOrder} established for the write side in M1, now on the consume side.
 *
 * <p>{@code consumerGroup} is a fixed constant matching this service's {@code @KafkaListener}
 * {@code groupId} — the dedupe key is {@code (consumer_group, event_id)}, never {@code event_id}
 * alone, so a different consumer group (notification-service) processes the same event
 * independently. See the dedupe-key ADR.
 */
@Service
public class PaymentAuthorizationService {

    public static final String CONSUMER_GROUP = "payment-service";

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
    public ConsumerOutcome handleOrderCreated(EventEnvelope envelope, String inboundTraceparent, String inboundTracestate) {
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
        paymentRepository.save(new Payment(paymentId, orderId, amountCents, "AUTHORIZED", now, now));

        UUID nextEventId = UUID.randomUUID();
        String traceparent = TraceContextCapture.continueOrStart(inboundTraceparent);
        Map<String, Object> payload = Map.of(
                "orderId", orderId,
                "paymentId", paymentId.toString(),
                "amountCents", amountCents,
                "status", "AUTHORIZED");
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize PaymentAuthorized payload", e);
        }

        outboxWriter.write(new OutboxEventRow(
                nextEventId,
                "Payment",
                orderId,
                1L,
                "PaymentAuthorized",
                1,
                envelope.correlationId(),
                envelope.eventId(),
                traceparent,
                inboundTracestate,
                payloadJson,
                now));

        return ConsumerOutcome.PROCESSED;
    }
}
