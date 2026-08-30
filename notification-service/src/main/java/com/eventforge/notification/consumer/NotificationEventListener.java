package com.eventforge.notification.consumer;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.events.tracing.EventForgeTracer;
import com.eventforge.events.tracing.SpanHandle;
import com.eventforge.notification.domain.NotificationSendingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * A SEPARATE consumer group ({@code notification-service}) from payment-service's
 * ({@code payment-service}), both consuming {@code orders.events} — this is the whole point of
 * this service existing (constitution item 6): each group holds its own independent offset
 * position on the same topic. The {@code id} is fixed and public so tests can look this listener
 * container up via {@code KafkaListenerEndpointRegistry} to stop/start it around an offset reset.
 *
 * <p>M4: this service publishes nothing downstream, so its span is the LAST hop in the trace —
 * still worth opening (constitution item 7's structured logs need {@code trace_id}/{@code span_id}
 * here too), extracted from the consumed record's headers same as every other consumer.
 */
@Component
public class NotificationEventListener {

    public static final String LISTENER_ID = "notification-order-events";

    private final NotificationSendingService service;
    private final FaultInjector faultInjector;
    private final EventForgeTracer tracer;
    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    public NotificationEventListener(NotificationSendingService service, FaultInjector faultInjector, EventForgeTracer tracer) {
        this.service = service;
        this.faultInjector = faultInjector;
        this.tracer = tracer;
    }

    @KafkaListener(id = LISTENER_ID, topics = "orders.events", groupId = NotificationSendingService.CONSUMER_GROUP)
    public void onOrderEvent(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        EventEnvelope envelope = mapper.readValue(record.value(), EventEnvelope.class);
        if (!"OrderCreated".equals(envelope.eventType())) {
            ack.acknowledge();
            return;
        }

        try (SpanHandle span = tracer.startConsumerSpan(
                "notification.send", headerValue(record, "traceparent"), headerValue(record, "tracestate"))) {
            try {
                service.handleOrderCreated(envelope);
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
