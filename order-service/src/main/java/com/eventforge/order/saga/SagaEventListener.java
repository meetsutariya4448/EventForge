package com.eventforge.order.saga;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.events.tracing.EventForgeTracer;
import com.eventforge.events.tracing.SpanHandle;
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
 *
 * <p>M4 (constitution item 4): extracts the trace context from the consumed record's headers and
 * opens a CONSUMER span around {@link SagaOrchestrator#handleFact}, spanning both the business
 * transition and its next outbox write — {@code handleFact} no longer takes trace parameters at
 * all; it captures whatever context this span makes active. See ADR-0017.
 */
@Component
public class SagaEventListener {

    private final SagaOrchestrator orchestrator;
    private final FaultInjector faultInjector;
    private final EventForgeTracer tracer;
    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    public SagaEventListener(SagaOrchestrator orchestrator, FaultInjector faultInjector, EventForgeTracer tracer) {
        this.orchestrator = orchestrator;
        this.faultInjector = faultInjector;
        this.tracer = tracer;
    }

    @KafkaListener(
            id = "order-saga-events",
            topics = {"payments.events", "inventory.events"},
            groupId = SagaOrchestrator.CONSUMER_GROUP)
    public void onSagaFact(ConsumerRecord<String, String> record, Acknowledgment ack) throws Exception {
        EventEnvelope envelope = mapper.readValue(record.value(), EventEnvelope.class);

        try (SpanHandle span = tracer.startConsumerSpan(
                "saga." + envelope.eventType(), headerValue(record, "traceparent"), headerValue(record, "tracestate"))) {
            try {
                orchestrator.handleFact(envelope);
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
