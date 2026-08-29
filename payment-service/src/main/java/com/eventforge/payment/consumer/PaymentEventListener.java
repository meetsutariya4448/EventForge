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
 * <p>{@link FaultInjectionPoint#AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK} fires here, between the
 * business transaction committing and the offset actually being acknowledged — the exact seam M0
 * built and M1 never had a consumer to call it from. A crash here means the business effect (and
 * any next outbox event) is already durably committed, but Kafka still thinks the offset is
 * uncommitted — the broker redelivers the same record, and {@code processedEventStore}'s dedupe
 * check absorbs it as a clean no-op on the next attempt.
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
        if (!"OrderCreated".equals(envelope.eventType())) {
            ack.acknowledge();
            return;
        }

        paymentAuthorizationService.handleOrderCreated(
                envelope, headerValue(record, "traceparent"), headerValue(record, "tracestate"));

        faultInjector.inject(FaultInjectionPoint.AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK);

        ack.acknowledge();
    }

    private static String headerValue(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
