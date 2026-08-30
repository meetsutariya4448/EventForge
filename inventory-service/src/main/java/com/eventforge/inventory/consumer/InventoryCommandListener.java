package com.eventforge.inventory.consumer;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.events.tracing.EventForgeTracer;
import com.eventforge.events.tracing.SpanHandle;
import com.eventforge.inventory.domain.InventoryReservationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * M3: replaces M2's {@code InventoryEventListener}, which reacted to raw {@code OrderCreated}.
 * This listens for the orchestrator's explicit {@code ReserveInventory} command instead — same
 * orchestrated-not-choreographed reasoning as payment-service's listener.
 *
 * <p>M4: extracts the trace context from the consumed record's headers and opens a CONSUMER span
 * around the handler — see ADR-0017.
 */
@Component
public class InventoryCommandListener {

    private final InventoryReservationService service;
    private final FaultInjector faultInjector;
    private final EventForgeTracer tracer;
    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    public InventoryCommandListener(InventoryReservationService service, FaultInjector faultInjector, EventForgeTracer tracer) {
        this.service = service;
        this.faultInjector = faultInjector;
        this.tracer = tracer;
    }

    @KafkaListener(
            id = "inventory-reserve-commands",
            topics = "orders.events",
            groupId = InventoryReservationService.CONSUMER_GROUP)
    public void onOrderTopicEvent(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        EventEnvelope envelope = mapper.readValue(record.value(), EventEnvelope.class);
        if (!"ReserveInventory".equals(envelope.eventType())) {
            ack.acknowledge();
            return;
        }

        try (SpanHandle span = tracer.startConsumerSpan(
                "inventory.reserve", headerValue(record, "traceparent"), headerValue(record, "tracestate"))) {
            try {
                service.handleReserveInventory(envelope);
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
