package com.eventforge.payment.consumer;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.events.tracing.EventForgeTracer;
import com.eventforge.events.tracing.SpanHandle;
import com.eventforge.payment.domain.PaymentAuthorizationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * Manual ack only, never auto-commit (constitution item 2) — the container factory is configured
 * for this via {@code spring.kafka.consumer.enable-auto-commit=false} and
 * {@code spring.kafka.listener.ack-mode=MANUAL_IMMEDIATE}, asserted against the real factory by
 * {@code ConsumerManualAckConfigTest}, not just trusted from YAML.
 *
 * <p>M3: this now dispatches on {@code event_type} rather than reacting to {@code OrderCreated}
 * directly — the orchestrator (order-service) explicitly commands {@code AuthorizePayment} and
 * {@code RefundPayment}; payment-service no longer self-triggers off the raw domain fact, which is
 * exactly the choreographed-vs-orchestrated distinction constitution item 1 draws. Still one
 * listener on {@code orders.events} (the topic every order-service-authored message multiplexes
 * onto), same as before.
 *
 * <p>{@link FaultInjectionPoint#AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK} fires here, between the
 * business transaction committing and the offset actually being acknowledged.
 */
@Component
public class PaymentEventListener {

    private final PaymentAuthorizationService paymentAuthorizationService;
    private final FaultInjector faultInjector;
    private final EventForgeTracer tracer;
    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    public PaymentEventListener(
            PaymentAuthorizationService paymentAuthorizationService, FaultInjector faultInjector, EventForgeTracer tracer) {
        this.paymentAuthorizationService = paymentAuthorizationService;
        this.faultInjector = faultInjector;
        this.tracer = tracer;
    }

    @KafkaListener(id = "payment-order-events", topics = "orders.events", groupId = PaymentAuthorizationService.CONSUMER_GROUP)
    public void onOrderEvent(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        EventEnvelope envelope = mapper.readValue(record.value(), EventEnvelope.class);
        if (!"AuthorizePayment".equals(envelope.eventType()) && !"RefundPayment".equals(envelope.eventType())) {
            ack.acknowledge();
            return;
        }

        // M4 (constitution item 4): extract from the consumed record's headers, continue the
        // trace, span the business transaction and the next outbox write — handleAuthorizePayment/
        // handleRefundPayment no longer take trace parameters; they capture whatever context this
        // span makes active. See ADR-0017.
        try (SpanHandle span = tracer.startConsumerSpan(
                "payment." + envelope.eventType(), headerValue(record, "traceparent"), headerValue(record, "tracestate"))) {
            try {
                switch (envelope.eventType()) {
                    case "AuthorizePayment" -> paymentAuthorizationService.handleAuthorizePayment(envelope);
                    case "RefundPayment" -> paymentAuthorizationService.handleRefundPayment(envelope);
                    default -> throw new IllegalStateException("unreachable: " + envelope.eventType());
                }
                faultInjector.inject(FaultInjectionPoint.AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK);
            } catch (RuntimeException e) {
                span.recordException(e);
                throw e;
            }
        }

        ack.acknowledge();
    }

    private static String headerValue(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
