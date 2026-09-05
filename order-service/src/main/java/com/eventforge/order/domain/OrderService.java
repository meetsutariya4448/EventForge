package com.eventforge.order.domain;

import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.outbox.OutboxEventRow;
import com.eventforge.events.outbox.OutboxWriter;
import com.eventforge.events.tracing.EventForgeTracer;
import com.eventforge.order.api.OrderResponse;
import com.eventforge.order.idempotency.IdempotencyKeyConflictException;
import com.eventforge.order.idempotency.IdempotencyKeyStore;
import com.eventforge.order.idempotency.IdempotencyOutcome;
import com.eventforge.order.idempotency.IdempotencyProperties;
import com.eventforge.order.idempotency.IdempotencyRecord;
import com.eventforge.order.idempotency.RequestFingerprint;
import com.eventforge.order.saga.SagaOrchestrator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import org.springframework.http.HttpStatus;
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
    private final IdempotencyKeyStore idempotencyKeyStore;
    private final Duration lockTimeout;

    // Deliberately not an autowired Spring-managed ObjectMapper: Spring Boot 4's own
    // JacksonAutoConfiguration is Jackson-3-shaped (tools.jackson) and doesn't register a
    // com.fasterxml.jackson.databind.ObjectMapper bean (see ADR-0009). Every service uses the
    // same explicit mapper the event contract itself defines, not an ambient default.
    private final ObjectMapper objectMapper = EventEnvelopeMapper.create();

    public OrderService(
            OrderRepository orderRepository,
            OutboxWriter outboxWriter,
            SagaOrchestrator sagaOrchestrator,
            EventForgeTracer tracer,
            IdempotencyKeyStore idempotencyKeyStore,
            IdempotencyProperties idempotencyProperties) {
        this.orderRepository = orderRepository;
        this.outboxWriter = outboxWriter;
        this.sagaOrchestrator = sagaOrchestrator;
        this.tracer = tracer;
        this.idempotencyKeyStore = idempotencyKeyStore;
        this.lockTimeout = idempotencyProperties.lockTimeout();
    }

    @Transactional
    public Order createOrder(long amountCents, String sku, long quantity) {
        return createOrder(UUID.randomUUID(), amountCents, sku, quantity);
    }

    /**
     * The create-order work itself, taking a caller-supplied id.
     *
     * <p>The id is a parameter rather than generated here because the idempotent path
     * ({@link #createOrderIdempotent}) must know it before this runs: its claim row records the
     * response the caller will get, and that claim is the first statement of this same
     * transaction.
     */
    @Transactional
    public Order createOrder(UUID orderId, long amountCents, String sku, long quantity) {
        Instant now = Instant.now();

        Order order = new Order(orderId, Order.STATUS_PENDING, amountCents, now, now);
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

    /**
     * Create-order under an {@code Idempotency-Key}, so a client retry cannot produce a second
     * order.
     *
     * <p>The claim is the <b>first statement of this transaction</b>, and the order write
     * follows it in the same transaction. That ordering is the whole design: claim and effect
     * commit together or neither does, so a failed attempt leaves no claim behind to block the
     * retry it was supposed to enable. It is the outbox pattern's own argument — a durable
     * record and the thing it describes sharing one transaction — applied to an inbound request.
     *
     * <p>Under a genuine race, Postgres blocks the second inserter on the winner's speculative
     * insertion lock until the winner commits or aborts. If the winner commits, this returns
     * zero rows and the subsequent lookup (a later statement, so a fresh READ COMMITTED
     * snapshot) is guaranteed to see the committed row. If the winner aborts, this insert
     * succeeds and this request legitimately becomes the winner. There is no third outcome,
     * which is why the table carries no {@code IN_PROGRESS} state and needs no stuck-claim
     * reaper. See ADR-0021, including the cost of that blocking.
     */
    @Transactional
    public IdempotencyOutcome createOrderIdempotent(String idempotencyKey, long amountCents, String sku, long quantity) {
        idempotencyKeyStore.applyLockTimeout(lockTimeout);

        UUID orderId = UUID.randomUUID();
        String fingerprint = RequestFingerprint.of(amountCents, sku, quantity);
        // Rendered once, here: this same object is what gets stored for a future replay and what
        // this caller gets back, so the two cannot drift apart.
        OrderResponse response = OrderResponse.forNewOrder(orderId, amountCents);
        String responseBody;
        try {
            responseBody = objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OrderResponse", e);
        }

        boolean claimed = idempotencyKeyStore.tryClaim(
                idempotencyKey, fingerprint, orderId, HttpStatus.CREATED.value(), responseBody);

        if (!claimed) {
            IdempotencyRecord existing = idempotencyKeyStore
                    .find(idempotencyKey)
                    .orElseThrow(() -> new IllegalStateException(
                            "Idempotency key " + idempotencyKey + " was neither claimable nor present; "
                                    + "this should be unreachable under READ COMMITTED"));
            if (!existing.requestFingerprint().equals(fingerprint)) {
                throw new IdempotencyKeyConflictException(
                        "Idempotency-Key was already used for a request with a different body");
            }
            return new IdempotencyOutcome.Replayed(existing.responseStatus(), existing.responseBody());
        }

        createOrder(orderId, amountCents, sku, quantity);
        return new IdempotencyOutcome.Created(response);
    }
}
