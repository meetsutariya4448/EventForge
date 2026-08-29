package com.eventforge.inventory.consumer;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.inventory.domain.InventoryEventProcessingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/** Wired identically to payment-service's listener — same manual-ack, dedupe, and fault-injection
 * seam — deliberately minimal business logic (see {@link InventoryEventProcessingService}). */
@Component
public class InventoryEventListener {

    private final InventoryEventProcessingService service;
    private final FaultInjector faultInjector;
    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    public InventoryEventListener(InventoryEventProcessingService service, FaultInjector faultInjector) {
        this.service = service;
        this.faultInjector = faultInjector;
    }

    @KafkaListener(
            id = "inventory-order-events",
            topics = "orders.events",
            groupId = InventoryEventProcessingService.CONSUMER_GROUP)
    public void onOrderEvent(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        EventEnvelope envelope = mapper.readValue(record.value(), EventEnvelope.class);
        if (!"OrderCreated".equals(envelope.eventType())) {
            ack.acknowledge();
            return;
        }

        service.handleOrderCreated(envelope);
        faultInjector.inject(FaultInjectionPoint.AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK);
        ack.acknowledge();
    }
}
