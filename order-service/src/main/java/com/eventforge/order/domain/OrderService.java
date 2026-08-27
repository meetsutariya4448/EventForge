package com.eventforge.order.domain;

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
 * Where M1's core claim is proven: the order row and its OrderCreated outbox row are written in
 * one method, inside one {@code @Transactional} boundary — both commit together or neither does.
 * No distributed transaction, no two-phase commit; just one Postgres transaction covering both
 * writes (see docs/architecture.md and ADR-0010).
 */
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxWriter outboxWriter;

    // Deliberately not an autowired Spring-managed ObjectMapper: Spring Boot 4's own
    // JacksonAutoConfiguration is Jackson-3-shaped (tools.jackson) and doesn't register a
    // com.fasterxml.jackson.databind.ObjectMapper bean (see ADR-0009). Every service uses the
    // same explicit mapper the event contract itself defines, not an ambient default.
    private final ObjectMapper objectMapper = EventEnvelopeMapper.create();

    public OrderService(OrderRepository orderRepository, OutboxWriter outboxWriter) {
        this.orderRepository = orderRepository;
        this.outboxWriter = outboxWriter;
    }

    @Transactional
    public Order createOrder(long amountCents, String inboundTraceparent, String inboundTracestate) {
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();

        Order order = new Order(orderId, "PENDING", amountCents, now, now);
        orderRepository.save(order);

        UUID eventId = UUID.randomUUID();
        String traceparent = TraceContextCapture.continueOrStart(inboundTraceparent);

        Map<String, Object> payload =
                Map.of("orderId", orderId.toString(), "amountCents", amountCents, "status", "PENDING");
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OrderCreated payload", e);
        }

        outboxWriter.write(new OutboxEventRow(
                eventId,
                "Order",
                orderId.toString(),
                1L,
                "OrderCreated",
                1,
                eventId, // root event: correlates with itself
                null, // root event: no cause
                traceparent,
                inboundTracestate,
                payloadJson,
                now));

        return order;
    }
}
