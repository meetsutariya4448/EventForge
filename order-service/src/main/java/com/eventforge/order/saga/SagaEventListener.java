package com.eventforge.order.saga;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

/**
 * The orchestrator's own consumer: every fact a participant publishes in response to a dispatched
 * command arrives here, under order-service's own consumer group ({@link SagaOrchestrator#CONSUMER_GROUP}).
 * Both topics share this one listener (same group, same deserializers) — {@code payments.events}
 * carries PaymentAuthorized/PaymentRefunded, {@code inventory.events} carries
 * InventoryReserved/InventoryReservationFailed. Manual ack only, same M2 discipline every other
 * consumer in this project follows (see {@code OrderServiceConsumerManualAckConfigTest}).
 */
@Component
public class SagaEventListener {

    private final SagaOrchestrator orchestrator;
    private final FaultInjector faultInjector;
    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    public SagaEventListener(SagaOrchestrator orchestrator, FaultInjector faultInjector) {
        this.orchestrator = orchestrator;
        this.faultInjector = faultInjector;
    }

    @KafkaListener(
            id = "order-saga-events",
            topics = {"payments.events", "inventory.events"},
            groupId = SagaOrchestrator.CONSUMER_GROUP)
    public void onSagaFact(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        EventEnvelope envelope = mapper.readValue(record.value(), EventEnvelope.class);

        orchestrator.handleFact(envelope, headerValue(record, "traceparent"), headerValue(record, "tracestate"));

        faultInjector.inject(FaultInjectionPoint.AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK);

        ack.acknowledge();
    }

    private static String headerValue(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
