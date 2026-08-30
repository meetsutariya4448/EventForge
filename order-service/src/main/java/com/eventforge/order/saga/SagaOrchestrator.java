package com.eventforge.order.saga;

import com.eventforge.events.consumer.ConsumerOutcome;
import com.eventforge.events.consumer.ProcessedEventStore;
import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.outbox.OutboxEventRow;
import com.eventforge.events.outbox.OutboxWriter;
import com.eventforge.events.tracing.EventForgeTracer;
import com.eventforge.events.tracing.SpanHandle;
import com.eventforge.order.domain.Order;
import com.eventforge.order.domain.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.opentelemetry.api.trace.SpanKind;
import java.time.Clock;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * The saga orchestrator, living inside order-service rather than as a fifth service — see
 * ADR-0014 for why. Explicitly dispatches one command at a time and only advances on the
 * participant's response (constitution item 1: "orchestrated... not choreographed"): every
 * command this class writes goes through the same {@link OutboxWriter} every other write in this
 * project uses, onto order-service's own {@code orders.events} topic, multiplexed by
 * {@code event_type} exactly like {@code OrderCreated} already was in M1/M2.
 *
 * <p>Every handler here is idempotent at two layers, not one (constitution item 4's warning that
 * compensations are where teams forget this): {@link #handleFact} deduplicates on
 * {@code (consumer_group, event_id)} first, exactly like every M2 consumer; every handler below
 * that also refuses to act unless the saga is in the exact state it expects, so a second copy of a
 * fact arriving under a *different* event_id (a sweep-triggered command redispatch, see
 * {@link #handleTimeout}) is still a clean no-op, not a double transition. See ADR-0016.
 *
 * <p>M4: every outbox write here calls {@link EventForgeTracer#captureCurrentContext()} rather
 * than threading a {@code traceparent}/{@code tracestate} string through every method signature —
 * the ACTIVE span is whatever {@link SagaEventListener} (for a fact) or {@link #handleTimeout}
 * itself (for a clock-driven dispatch, which opens its own root span since there's no inbound
 * message to continue) made current before calling in. See ADR-0017.
 */
@Service
public class SagaOrchestrator {

    public static final String CONSUMER_GROUP = "order-service";

    private static final Logger log = LoggerFactory.getLogger(SagaOrchestrator.class);

    private static final List<SagaState> AWAITING_STATES =
            List.of(SagaState.AWAITING_PAYMENT, SagaState.AWAITING_INVENTORY, SagaState.AWAITING_REFUND);

    private final SagaInstanceRepository sagaInstanceRepository;
    private final SagaStepRepository sagaStepRepository;
    private final OrderRepository orderRepository;
    private final OutboxWriter outboxWriter;
    private final ProcessedEventStore processedEventStore;
    private final Clock clock;
    private final SagaProperties properties;
    private final Counter compensationFailedCounter;
    private final TransactionTemplate transactionTemplate;
    private final EventForgeTracer tracer;

    // Same reasoning as every other service (ADR-0009): Spring Boot 4's own JacksonAutoConfiguration
    // doesn't register a com.fasterxml.jackson.databind.ObjectMapper bean.
    private final ObjectMapper objectMapper = EventEnvelopeMapper.create();

    public SagaOrchestrator(
            SagaInstanceRepository sagaInstanceRepository,
            SagaStepRepository sagaStepRepository,
            OrderRepository orderRepository,
            OutboxWriter outboxWriter,
            ProcessedEventStore processedEventStore,
            Clock clock,
            SagaProperties properties,
            MeterRegistry meterRegistry,
            PlatformTransactionManager transactionManager,
            EventForgeTracer tracer) {
        this.sagaInstanceRepository = sagaInstanceRepository;
        this.sagaStepRepository = sagaStepRepository;
        this.orderRepository = orderRepository;
        this.outboxWriter = outboxWriter;
        this.processedEventStore = processedEventStore;
        this.clock = clock;
        this.properties = properties;
        this.compensationFailedCounter = Counter.builder("saga.compensation.failed")
                .description("Sagas that reached COMPENSATION_FAILED - see docs/runbooks/compensation-failure.md")
                .register(meterRegistry);
        // Not @Transactional on handleTimeout: sweepTimedOutSagas() calls it once per timed-out
        // saga from WITHIN this same class. That's a self-invocation, which bypasses Spring's
        // AOP proxy entirely and silently makes @Transactional a no-op on the called method — a
        // real bug this project hit directly (see the changelog note on this class). Programmatic
        // transaction demarcation via TransactionTemplate has no proxy to bypass.
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.tracer = tracer;
    }

    /**
     * Starts a new saga and dispatches its first step. Called from {@code OrderService.createOrder}
     * inside the SAME transaction as the {@code orders} row and the {@code OrderCreated} outbox
     * write (sequence 1) — the saga row and the AuthorizePayment dispatch (sequence 2) commit
     * atomically with both, the one property a separate orchestrator service could not offer
     * without reintroducing a dual-write (see ADR-0014). Also the same ACTIVE span (the HTTP
     * server span {@code OrderController} opened) — {@link #dispatchAndAwait} captures it exactly
     * like {@code OrderService.createOrder}'s own {@code OrderCreated} write did.
     */
    public void startSaga(Order order, String sku, long quantity, UUID orderCreatedEventId) {
        Instant now = clock.instant();
        UUID sagaId = UUID.randomUUID();
        Instant deadline = now.plusMillis(properties.authorizePaymentTimeoutMs());
        SagaInstance saga = new SagaInstance(
                sagaId,
                order.getOrderId(),
                orderCreatedEventId,
                SagaState.AWAITING_PAYMENT,
                order.getAmountCents(),
                sku,
                quantity,
                2L,
                deadline,
                now);
        // Reassign to the returned reference: SagaInstance uses a client-assigned UUID id (not
        // @GeneratedValue), so Spring Data JPA's save() on a brand-new instance routes through
        // entityManager.merge() rather than persist() — merge() copies state onto a DIFFERENT
        // managed instance and returns THAT one, leaving the original object we just built
        // unmanaged. Mutating the original afterward (allocateNextSequence(), below, via
        // dispatchAndAwait) would silently never be flushed if we kept using it.
        saga = sagaInstanceRepository.save(saga);

        Map<String, Object> payload = Map.of("orderId", order.getOrderId().toString(), "amountCents", order.getAmountCents());
        dispatchAndAwait(saga, SagaEventTypes.AUTHORIZE_PAYMENT, orderCreatedEventId, payload, now);
    }

    /**
     * Entry point for {@link com.eventforge.order.saga.SagaEventListener}: every fact published by
     * a participant in response to a dispatched command arrives here. Called from within the
     * CONSUMER span the listener already opened around the Kafka record's headers — every outbox
     * write below captures that same active context.
     */
    @Transactional
    public ConsumerOutcome handleFact(EventEnvelope envelope) {
        boolean isNew = processedEventStore.tryMarkProcessed(CONSUMER_GROUP, envelope.eventId(), envelope.aggregateId());
        if (!isNew) {
            return ConsumerOutcome.DUPLICATE;
        }

        UUID orderId = UUID.fromString(envelope.aggregateId());
        Optional<SagaInstance> maybeSaga = sagaInstanceRepository.findByOrderId(orderId);
        if (maybeSaga.isEmpty()) {
            log.warn("Received {} for order {} with no matching saga instance - ignoring", envelope.eventType(), orderId);
            return ConsumerOutcome.PROCESSED;
        }
        SagaInstance saga = maybeSaga.get();

        switch (envelope.eventType()) {
            case SagaEventTypes.PAYMENT_AUTHORIZED -> handlePaymentAuthorized(saga, envelope);
            case SagaEventTypes.INVENTORY_RESERVED -> handleInventoryReserved(saga, envelope);
            case SagaEventTypes.INVENTORY_RESERVATION_FAILED -> handleInventoryReservationFailed(saga, envelope);
            case SagaEventTypes.PAYMENT_REFUNDED -> handlePaymentRefunded(saga, envelope);
            default -> log.warn("Unrecognized saga fact event type {} for order {}", envelope.eventType(), orderId);
        }
        return ConsumerOutcome.PROCESSED;
    }

    private void handlePaymentAuthorized(SagaInstance saga, EventEnvelope envelope) {
        Instant now = clock.instant();
        if (saga.getState() != SagaState.AWAITING_PAYMENT) {
            log.info(
                    "Ignoring PaymentAuthorized for saga {} in state {} (expected AWAITING_PAYMENT) - stale or"
                            + " duplicate fact, clean no-op",
                    saga.getSagaId(),
                    saga.getState());
            return;
        }
        completeStep(saga, SagaEventTypes.AUTHORIZE_PAYMENT, now, "PaymentAuthorized received");

        Instant deadline = now.plusMillis(properties.reserveInventoryTimeoutMs());
        saga.transitionTo(SagaState.AWAITING_INVENTORY, deadline, now);
        sagaInstanceRepository.save(saga);

        Map<String, Object> payload =
                Map.of("orderId", saga.getOrderId().toString(), "sku", saga.getSku(), "quantity", saga.getQuantity());
        dispatchAndAwait(saga, SagaEventTypes.RESERVE_INVENTORY, envelope.eventId(), payload, now);
    }

    private void handleInventoryReserved(SagaInstance saga, EventEnvelope envelope) {
        Instant now = clock.instant();
        if (saga.getState() != SagaState.AWAITING_INVENTORY) {
            log.info(
                    "Ignoring InventoryReserved for saga {} in state {} (expected AWAITING_INVENTORY) - stale or"
                            + " duplicate fact, clean no-op",
                    saga.getSagaId(),
                    saga.getState());
            return;
        }
        completeStep(saga, SagaEventTypes.RESERVE_INVENTORY, now, "InventoryReserved received");
        saga.transitionTo(SagaState.COMPLETED, null, now);
        sagaInstanceRepository.save(saga);

        Order order = orderRepository.findById(saga.getOrderId()).orElseThrow();
        order.confirm(now);
        orderRepository.save(order);

        Map<String, Object> payload = Map.of("orderId", saga.getOrderId().toString());
        fireTerminalEvent(saga, SagaEventTypes.ORDER_CONFIRMED, envelope.eventId(), payload, now);
    }

    private void handleInventoryReservationFailed(SagaInstance saga, EventEnvelope envelope) {
        Instant now = clock.instant();
        if (saga.getState() != SagaState.AWAITING_INVENTORY) {
            log.info(
                    "Ignoring InventoryReservationFailed for saga {} in state {} (expected AWAITING_INVENTORY) -"
                            + " stale or duplicate fact, clean no-op",
                    saga.getSagaId(),
                    saga.getState());
            return;
        }
        String reason = envelope.payload().has("reason") ? envelope.payload().get("reason").asText() : "unspecified";
        failStep(saga, SagaEventTypes.RESERVE_INVENTORY, now, "InventoryReservationFailed: " + reason);
        beginCompensation(saga, envelope.eventId(), now);
    }

    private void handlePaymentRefunded(SagaInstance saga, EventEnvelope envelope) {
        Instant now = clock.instant();
        if (saga.getState() != SagaState.AWAITING_REFUND) {
            log.info(
                    "Ignoring PaymentRefunded for saga {} in state {} (expected AWAITING_REFUND) - stale or"
                            + " duplicate fact (e.g. a second answer to a sweep-redispatched RefundPayment),"
                            + " clean no-op",
                    saga.getSagaId(),
                    saga.getState());
            return;
        }
        completeStep(saga, SagaEventTypes.REFUND_PAYMENT, now, "PaymentRefunded received");
        saga.transitionTo(SagaState.COMPENSATED, null, now);
        sagaInstanceRepository.save(saga);

        Order order = orderRepository.findById(saga.getOrderId()).orElseThrow();
        order.cancel(now);
        orderRepository.save(order);

        Map<String, Object> payload = Map.of("orderId", saga.getOrderId().toString(), "reason", "inventory reservation failed");
        fireTerminalEvent(saga, SagaEventTypes.ORDER_CANCELLED, envelope.eventId(), payload, now);
    }

    private void beginCompensation(SagaInstance saga, UUID causationId, Instant now) {
        Instant deadline = now.plusMillis(properties.refundPaymentTimeoutMs());
        saga.transitionTo(SagaState.AWAITING_REFUND, deadline, now);
        saga.recordCompensationAttempt(now);
        sagaInstanceRepository.save(saga);

        Map<String, Object> payload = Map.of("orderId", saga.getOrderId().toString(), "amountCents", saga.getAmountCents());
        dispatchAndAwait(saga, SagaEventTypes.REFUND_PAYMENT, causationId, payload, now);
    }

    /**
     * The persisted-deadline timeout sweep (constitution item 5): the source of truth for "has this
     * step gone unanswered too long" is {@code saga_instance.deadline_at}, evaluated against the
     * injected {@link Clock} — never an in-memory timer, which is exactly what would die with the
     * process this sweep is designed to survive restarting. Called by
     * {@link SagaTimeoutSweeper}; each timed-out saga is handled in its own transaction (see
     * {@link #handleTimeout}), matching {@code OutboxRelayScheduler}'s one-unit-of-work-per-call
     * shape.
     */
    public void sweepTimedOutSagas() {
        Instant now = clock.instant();
        List<SagaInstance> timedOut = sagaInstanceRepository.findByStateInAndDeadlineAtLessThanEqual(AWAITING_STATES, now);
        for (SagaInstance saga : timedOut) {
            handleTimeout(saga.getSagaId());
        }
    }

    /**
     * Deliberately NOT {@code @Transactional}: this is called from {@link #sweepTimedOutSagas()},
     * which lives on the very same object — a self-invocation that bypasses Spring's AOP proxy
     * entirely and would silently turn {@code @Transactional} here into a no-op (each repository
     * call would open and close its own tiny transaction instead of one atomic unit of work,
     * exactly the "read a stale value, write it back" bug this project caught directly while
     * building this class — see the constructor's comment on {@link #transactionTemplate}).
     * {@link TransactionTemplate} demarcates the transaction programmatically instead, which has
     * no proxy to bypass.
     *
     * <p>M4: a clock-driven timeout has no inbound message to continue a trace from, so this opens
     * a brand-new ROOT span (constitution item 2 doesn't apply here — there's no active context to
     * capture, only one to originate) before doing anything else, so any outbox write the timeout
     * path triggers still has an active context to capture.
     */
    public void handleTimeout(UUID sagaId) {
        try (SpanHandle span = tracer.startRootSpan("saga.timeout", SpanKind.INTERNAL)) {
            try {
                transactionTemplate.executeWithoutResult(status -> handleTimeoutInTransaction(sagaId));
            } catch (RuntimeException e) {
                span.recordException(e);
                throw e;
            }
        }
    }

    private void handleTimeoutInTransaction(UUID sagaId) {
        Instant now = clock.instant();
        SagaInstance saga = sagaInstanceRepository.findById(sagaId).orElseThrow();
        // Re-check under this transaction: a real response, or a previous sweep tick, may already
        // have resolved this saga since the outer scan ran.
        if (saga.getDeadlineAt() == null || saga.getDeadlineAt().isAfter(now)) {
            return;
        }

        switch (saga.getState()) {
            case AWAITING_PAYMENT -> handleAuthorizePaymentTimeout(saga, now);
            case AWAITING_INVENTORY -> handleReserveInventoryTimeout(saga, now);
            case AWAITING_REFUND -> handleRefundPaymentTimeout(saga, now);
            default -> { }
        }
    }

    private void handleAuthorizePaymentTimeout(SagaInstance saga, Instant now) {
        failStep(saga, SagaEventTypes.AUTHORIZE_PAYMENT, now, "timed out waiting for PaymentAuthorized");
        saga.transitionTo(SagaState.COMPENSATED, null, now);
        sagaInstanceRepository.save(saga);

        Order order = orderRepository.findById(saga.getOrderId()).orElseThrow();
        order.cancel(now);
        orderRepository.save(order);

        log.warn(
                "Saga {} for order {} timed out waiting for PaymentAuthorized - order cancelled, no refund needed"
                        + " (payment was never authorized)",
                saga.getSagaId(),
                saga.getOrderId());

        Map<String, Object> payload = Map.of("orderId", saga.getOrderId().toString(), "reason", "payment authorization timed out");
        fireTerminalEvent(saga, SagaEventTypes.ORDER_CANCELLED, null, payload, now);
    }

    private void handleReserveInventoryTimeout(SagaInstance saga, Instant now) {
        failStep(saga, SagaEventTypes.RESERVE_INVENTORY, now, "timed out waiting for InventoryReserved/InventoryReservationFailed");
        log.warn(
                "Saga {} for order {} timed out waiting on inventory reservation (dead consumer or lost message) -"
                        + " payment was already authorized, so compensation begins",
                saga.getSagaId(),
                saga.getOrderId());
        beginCompensation(saga, null, now);
    }

    private void handleRefundPaymentTimeout(SagaInstance saga, Instant now) {
        if (saga.getCompensationAttempts() >= properties.maxCompensationAttempts()) {
            // THE HARD CASE (constitution item 6): compensation itself has failed permanently.
            // Bounded, not an infinite retry loop - this is the maxCompensationAttempts-th and
            // last attempt giving up, not attempt number one.
            failStep(saga, SagaEventTypes.REFUND_PAYMENT, now, "compensation abandoned after " + saga.getCompensationAttempts() + " attempts");
            saga.transitionTo(SagaState.COMPENSATION_FAILED, null, now);
            sagaInstanceRepository.save(saga);

            Order order = orderRepository.findById(saga.getOrderId()).orElseThrow();
            order.markCancellationFailed(now);
            orderRepository.save(order);

            compensationFailedCounter.increment();
            log.error(
                    "ALERT saga.compensation.failed: saga {} for order {} reached COMPENSATION_FAILED after {}"
                            + " RefundPayment attempts with no PaymentRefunded response - manual remediation"
                            + " required, see docs/runbooks/compensation-failure.md",
                    saga.getSagaId(),
                    saga.getOrderId(),
                    saga.getCompensationAttempts());
            return;
        }

        failStep(saga, SagaEventTypes.REFUND_PAYMENT, now, "timed out waiting for PaymentRefunded, retrying");
        Instant deadline = now.plusMillis(properties.refundPaymentTimeoutMs());
        saga.transitionTo(SagaState.AWAITING_REFUND, deadline, now);
        saga.recordCompensationAttempt(now);
        sagaInstanceRepository.save(saga);

        log.warn(
                "Saga {} for order {} did not receive PaymentRefunded in time - redispatching RefundPayment"
                        + " (attempt {} of {})",
                saga.getSagaId(),
                saga.getOrderId(),
                saga.getCompensationAttempts(),
                properties.maxCompensationAttempts());

        Map<String, Object> payload = Map.of("orderId", saga.getOrderId().toString(), "amountCents", saga.getAmountCents());
        dispatchAndAwait(saga, SagaEventTypes.REFUND_PAYMENT, null, payload, now);
    }

    private void dispatchAndAwait(SagaInstance saga, String eventType, UUID causationId, Map<String, Object> payload, Instant now) {
        writeOutboxEvent(saga, eventType, causationId, payload, now);
        sagaStepRepository.save(new SagaStep(UUID.randomUUID(), saga.getSagaId(), eventType, "DISPATCHED", now, null));
    }

    private void fireTerminalEvent(SagaInstance saga, String eventType, UUID causationId, Map<String, Object> payload, Instant now) {
        writeOutboxEvent(saga, eventType, causationId, payload, now);
        sagaStepRepository.save(new SagaStep(UUID.randomUUID(), saga.getSagaId(), eventType, "SUCCEEDED", now, null));
    }

    private void writeOutboxEvent(SagaInstance saga, String eventType, UUID causationId, Map<String, Object> payload, Instant now) {
        UUID eventId = UUID.randomUUID();
        long sequence = saga.allocateNextSequence();
        EventForgeTracer.CapturedContext context = tracer.captureCurrentContext();
        String payloadJson;
        try {
            payloadJson = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize " + eventType + " payload", e);
        }
        outboxWriter.write(new OutboxEventRow(
                eventId,
                "Order",
                saga.getOrderId().toString(),
                sequence,
                eventType,
                1,
                saga.getCorrelationId(),
                causationId,
                context.traceparent(),
                context.tracestate(),
                payloadJson,
                now));
    }

    private void completeStep(SagaInstance saga, String stepName, Instant now, String detail) {
        resolveLatestDispatchedStep(saga, stepName).ifPresent(step -> {
            step.complete("SUCCEEDED", now, detail);
            sagaStepRepository.save(step);
        });
    }

    private void failStep(SagaInstance saga, String stepName, Instant now, String detail) {
        resolveLatestDispatchedStep(saga, stepName).ifPresent(step -> {
            step.complete("FAILED", now, detail);
            sagaStepRepository.save(step);
        });
    }

    private Optional<SagaStep> resolveLatestDispatchedStep(SagaInstance saga, String stepName) {
        List<SagaStep> steps = sagaStepRepository.findBySagaIdOrderByDispatchedAtAsc(saga.getSagaId());
        for (int i = steps.size() - 1; i >= 0; i--) {
            SagaStep step = steps.get(i);
            if (step.getStepName().equals(stepName) && "DISPATCHED".equals(step.getStatus())) {
                return Optional.of(step);
            }
        }
        log.warn("No DISPATCHED {} step found for saga {} to resolve - already resolved?", stepName, saga.getSagaId());
        return Optional.empty();
    }
}
