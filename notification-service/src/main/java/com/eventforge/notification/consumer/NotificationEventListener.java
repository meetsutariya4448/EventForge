package com.eventforge.notification.consumer;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.notification.domain.NotificationSendingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * A SEPARATE consumer group ({@code notification-service}) from payment-service's
 * ({@code payment-service}), both consuming {@code orders.events} — this is the whole point of
 * this service existing (constitution item 6): each group holds its own independent offset
 * position on the same topic. The {@code id} is fixed and public so tests can look this listener
 * container up via {@code KafkaListenerEndpointRegistry} to stop/start it around an offset reset.
 */
@Component
public class NotificationEventListener {

    public static final String LISTENER_ID = "notification-order-events";

    private final NotificationSendingService service;
    private final FaultInjector faultInjector;
    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    public NotificationEventListener(NotificationSendingService service, FaultInjector faultInjector) {
        this.service = service;
        this.faultInjector = faultInjector;
    }

    @KafkaListener(id = LISTENER_ID, topics = "orders.events", groupId = NotificationSendingService.CONSUMER_GROUP)
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
