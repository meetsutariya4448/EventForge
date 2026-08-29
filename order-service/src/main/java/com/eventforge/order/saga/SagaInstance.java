package com.eventforge.order.saga;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Persisted saga state (constitution item 1): inspectable via {@code SELECT * FROM saga_instance}
 * mid-flight, not reconstructed from an in-memory map. Ordinary JPA persistence (R4) — the one
 * query that needs raw locking semantics is the timeout sweep's claim, which is a single-instance
 * scan in this project's topology (see ADR-0014), not a multi-worker concurrent-claim problem like
 * the outbox relay's.
 */
@Entity
@Table(name = "saga_instance")
public class SagaInstance {

    @Id
    @Column(name = "saga_id")
    private UUID sagaId;

    @Column(name = "order_id", nullable = false)
    private UUID orderId;

    // The saga's own identifier is sagaId, never overloaded with correlationId (constitution item
    // 1). correlationId is stored here too, though: every dispatch after the first has no inbound
    // EventEnvelope to read it from (a timeout-triggered dispatch has no inbound event at all), so
    // the root correlationId — set once at saga creation from OrderCreated's own eventId — lives
    // here as the single source of truth for the whole causal chain.
    @Column(name = "correlation_id", nullable = false)
    private UUID correlationId;

    @Enumerated(EnumType.STRING)
    @Column(name = "state", nullable = false, length = 32)
    private SagaState state;

    @Column(name = "amount_cents", nullable = false)
    private long amountCents;

    @Column(name = "sku", nullable = false)
    private String sku;

    @Column(name = "quantity", nullable = false)
    private long quantity;

    // This aggregate's (order_id's) next free outbox_events.aggregate_sequence. Seeded at 2 -
    // OrderService already wrote OrderCreated as sequence 1 in the same transaction that creates
    // this row - and allocated (read-then-incremented) once per outbox write this saga makes
    // afterward, across however many separate transactions those turn out to be.
    @Column(name = "next_sequence", nullable = false)
    private long nextSequence;

    @Column(name = "deadline_at")
    private Instant deadlineAt;

    @Column(name = "compensation_attempts", nullable = false)
    private int compensationAttempts;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected SagaInstance() {}

    public SagaInstance(
            UUID sagaId,
            UUID orderId,
            UUID correlationId,
            SagaState state,
            long amountCents,
            String sku,
            long quantity,
            long nextSequence,
            Instant deadlineAt,
            Instant now) {
        this.sagaId = sagaId;
        this.orderId = orderId;
        this.correlationId = correlationId;
        this.state = state;
        this.amountCents = amountCents;
        this.sku = sku;
        this.quantity = quantity;
        this.nextSequence = nextSequence;
        this.deadlineAt = deadlineAt;
        this.compensationAttempts = 0;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public UUID getSagaId() {
        return sagaId;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public UUID getCorrelationId() {
        return correlationId;
    }

    public SagaState getState() {
        return state;
    }

    public long getAmountCents() {
        return amountCents;
    }

    public String getSku() {
        return sku;
    }

    public long getQuantity() {
        return quantity;
    }

    public Instant getDeadlineAt() {
        return deadlineAt;
    }

    public int getCompensationAttempts() {
        return compensationAttempts;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void transitionTo(SagaState newState, Instant deadlineAt, Instant now) {
        this.state = newState;
        this.deadlineAt = deadlineAt;
        this.updatedAt = now;
    }

    public void recordCompensationAttempt(Instant now) {
        this.compensationAttempts++;
        this.updatedAt = now;
    }

    /** Returns this aggregate's next free {@code aggregate_sequence} and reserves it. */
    public long allocateNextSequence() {
        return nextSequence++;
    }
}
