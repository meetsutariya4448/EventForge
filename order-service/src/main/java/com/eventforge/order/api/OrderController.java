package com.eventforge.order.api;

import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.events.tracing.EventForgeTracer;
import com.eventforge.events.tracing.SpanHandle;
import com.eventforge.order.domain.Order;
import com.eventforge.order.domain.OrderService;
import com.eventforge.order.idempotency.IdempotencyClaimInFlightException;
import com.eventforge.order.idempotency.IdempotencyKeyConflictException;
import com.eventforge.order.idempotency.IdempotencyOutcome;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.util.UUID;
import java.util.function.Supplier;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * M4: this is the one HTTP entry point in the whole system, and the one place a trace can begin
 * from a real inbound {@code traceparent} header rather than a Kafka record's. The
 * {@link EventForgeTracer#startServerSpan} call below continues that inbound header if present, or
 * starts a fresh trace otherwise — {@code OrderService.createOrder} no longer takes trace-context
 * parameters at all; it reads the ACTIVE context this span establishes via
 * {@link EventForgeTracer#captureCurrentContext()} instead (constitution item 2). See ADR-0017.
 */
@RestController
@RequestMapping("/orders")
public class OrderController {

    private final OrderService orderService;
    private final FaultInjector faultInjector;
    private final EventForgeTracer tracer;

    // Same explicit mapper the event contract defines rather than an ambient Spring-managed one
    // — Spring Boot 4's JacksonAutoConfiguration is Jackson-3-shaped and registers no
    // com.fasterxml.jackson.databind.ObjectMapper bean (ADR-0009).
    private final ObjectMapper objectMapper = EventEnvelopeMapper.create();

    public OrderController(OrderService orderService, FaultInjector faultInjector, EventForgeTracer tracer) {
        this.orderService = orderService;
        this.faultInjector = faultInjector;
        this.tracer = tracer;
    }

    /**
     * Creating an order mutates state, so it is {@code OPERATOR}-only: a {@code VIEWER} can see
     * what the system is doing but cannot make it do anything. That split is what makes
     * "unauthorized mutation is denied" an enforced property rather than a UI convention.
     */
    @PreAuthorize("hasRole('OPERATOR')")
    @PostMapping
    public ResponseEntity<String> createOrder(
            @RequestBody CreateOrderRequest request,
            @RequestHeader(value = "Idempotency-Key", required = false) String idempotencyKey,
            @RequestHeader(value = "traceparent", required = false) String traceparent,
            @RequestHeader(value = "tracestate", required = false) String tracestate) {
        if (request.amountCents() <= 0) {
            throw new IllegalArgumentException("amountCents must be positive");
        }

        // The header is optional, and its absence takes the original code path unchanged. That
        // is deliberate: idempotency is opt-in per request, so every existing caller and test
        // keeps its exact prior behaviour.
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            return withServerSpan(traceparent, tracestate, () -> {
                Order order = orderService.createOrder(request.amountCents(), request.sku(), request.quantity());
                OrderResponse response = OrderResponse.forNewOrder(order.getOrderId(), order.getAmountCents());
                return created(order.getOrderId(), serialize(response), false);
            });
        }

        return withServerSpan(traceparent, tracestate, () -> {
            IdempotencyOutcome outcome = orderService.createOrderIdempotent(
                    idempotencyKey, request.amountCents(), request.sku(), request.quantity());
            return switch (outcome) {
                case IdempotencyOutcome.Created created ->
                    created(created.response().orderId(), serialize(created.response()), false);
                // The stored body, verbatim — not re-rendered. A retry gets back exactly what the
                // original caller got.
                case IdempotencyOutcome.Replayed replayed ->
                    ResponseEntity.status(replayed.status())
                            .header("Idempotency-Replayed", "true")
                            .contentType(MediaType.APPLICATION_JSON)
                            .body(replayed.responseBody());
            };
        });
    }

    /**
     * Runs the request body inside the one server span this system's traces begin at, recording
     * any exception on it, and fires the post-commit fault-injection seam.
     */
    private ResponseEntity<String> withServerSpan(
            String traceparent, String tracestate, Supplier<ResponseEntity<String>> work) {
        try (SpanHandle span = tracer.startServerSpan("POST /orders", traceparent, tracestate)) {
            ResponseEntity<String> response;
            try {
                response = work.get();
            } catch (RuntimeException e) {
                span.recordException(e);
                throw e;
            }

            // The order+outbox transaction has now committed. This is the real seam the
            // fault-injection point exists for: a crash right here would leave a durable,
            // unpublished outbox row for the relay to find and publish later — proving the outbox
            // pattern's point.
            faultInjector.inject(FaultInjectionPoint.AFTER_DB_COMMIT_BEFORE_KAFKA_PUBLISH);
            return response;
        }
    }

    private ResponseEntity<String> created(UUID orderId, String body, boolean replayed) {
        return ResponseEntity.created(URI.create("/orders/" + orderId))
                .contentType(MediaType.APPLICATION_JSON)
                .header("Idempotency-Replayed", Boolean.toString(replayed))
                .body(body);
    }

    private String serialize(OrderResponse response) {
        try {
            return objectMapper.writeValueAsString(response);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize OrderResponse", e);
        }
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(e.getMessage());
    }

    /**
     * Same key, different body: a client bug rather than a retry. 422 rather than 409 because the
     * request is well-formed but semantically unprocessable under a key that already means
     * something else.
     */
    @ExceptionHandler(IdempotencyKeyConflictException.class)
    public ResponseEntity<String> handleIdempotencyConflict(IdempotencyKeyConflictException e) {
        return ResponseEntity.status(HttpStatus.UNPROCESSABLE_CONTENT).body(e.getMessage());
    }

    /**
     * Another request holds an uncommitted claim on this key and did not finish within
     * {@code eventforge.idempotency.lock-timeout}. The caller's own retry is the right resolution,
     * so this is a retryable 409 rather than a failure — see ADR-0021 on why the request blocks
     * at all, and what that costs.
     *
     * <p>Handles the store's own translated type, not a Spring one: Postgres {@code 55P03}
     * arrives as {@code UncategorizedSQLException}, which is far too broad to map to 409 here.
     */
    @ExceptionHandler(IdempotencyClaimInFlightException.class)
    public ResponseEntity<String> handleClaimInFlight(IdempotencyClaimInFlightException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .header(HttpHeaders.RETRY_AFTER, "1")
                .body("A request with this Idempotency-Key is currently in flight; retry shortly");
    }
}
