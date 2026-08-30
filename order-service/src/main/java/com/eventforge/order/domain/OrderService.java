package com.eventforge.order.domain;

import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.outbox.OutboxEventRow;
import com.eventforge.events.outbox.OutboxWriter;
import com.eventforge.events.tracing.EventForgeTracer;
import com.eventforge.order.saga.SagaOrchestrator;
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
 *
 * <p>M3 extends the same transaction one step further: {@link SagaOrchestrator#startSaga} writes
 * the {@code saga_instance} row and dispatches AuthorizePayment (sequence 2) in this SAME
 * transaction — the order row, OrderCreated, the saga row, and AuthorizePayment all commit
 * together or none do. See ADR-0014 for why colocating the orchestrator here is what makes this
 * atomicity possible at all.
 */
@Service
public class OrderService {

    private final OrderRepository orderRepository;
    private final OutboxWriter outboxWriter;
    private final SagaOrchestrator sagaOrchestrator;
    private final EventForgeTracer tracer;

    // Deliberately not an autowired Spring-managed ObjectMapper: Spring Boot 4's own
    // JacksonAutoConfiguration is Jackson-3-shaped (tools.jackson) and doesn't register a
    // com.fasterxml.jackson.databind.ObjectMapper bean (see ADR-0009). Every service uses the
    // same explicit mapper the event contract itself defines, not an ambient default.
    private final ObjectMapper objectMapper = EventEnvelopeMapper.create();

    public OrderService(
            OrderRepository orderRepository, OutboxWriter outboxWriter, SagaOrchestrator sagaOrchestrator, EventForgeTracer tracer) {
        this.orderRepository = orderRepository;
        this.outboxWriter = outboxWriter;
        this.sagaOrchestrator = sagaOrchestrator;
        this.tracer = tracer;
    }

    @Transactional
    public Order createOrder(long amountCents, String sku, long quantity) {
        UUID orderId = UUID.randomUUID();
        Instant now = Instant.now();

        Order order = new Order(orderId, "PENDING", amountCents, now, now);
        orderRepository.save(order);

        UUID eventId = UUID.randomUUID();
        // M4 (constitution item 2): the ACTIVE context — the SERVER span OrderController opened
        // around this whole call — captured via the real W3C propagator, not fabricated and not
        // reaching into any other hop's context. See ADR-0017.
        EventForgeTracer.CapturedContext context = tracer.captureCurrentContext();

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
                context.traceparent(),
                context.tracestate(),
                payloadJson,
                now));

        sagaOrchestrator.startSaga(order, sku, quantity, eventId);

        return order;
    }
}
