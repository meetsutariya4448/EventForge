package com.eventforge.payment.consumer;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
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
    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    public PaymentEventListener(PaymentAuthorizationService paymentAuthorizationService, FaultInjector faultInjector) {
        this.paymentAuthorizationService = paymentAuthorizationService;
        this.faultInjector = faultInjector;
    }

    @KafkaListener(id = "payment-order-events", topics = "orders.events", groupId = PaymentAuthorizationService.CONSUMER_GROUP)
    public void onOrderEvent(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        EventEnvelope envelope = mapper.readValue(record.value(), EventEnvelope.class);

        switch (envelope.eventType()) {
            case "AuthorizePayment" -> paymentAuthorizationService.handleAuthorizePayment(
                    envelope, headerValue(record, "traceparent"), headerValue(record, "tracestate"));
            case "RefundPayment" -> paymentAuthorizationService.handleRefundPayment(
                    envelope, headerValue(record, "traceparent"), headerValue(record, "tracestate"));
            default -> {
                ack.acknowledge();
                return;
            }
        }

        faultInjector.inject(FaultInjectionPoint.AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK);

        ack.acknowledge();
    }

    private static String headerValue(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
