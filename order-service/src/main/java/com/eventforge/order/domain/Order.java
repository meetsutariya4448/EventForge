package com.eventforge.order.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "orders")
public class Order {

    /**
     * The status every order is created with. Named rather than repeated as a literal because
     * v2's idempotency layer has to render a newly-created order's response <em>before</em> the
     * row exists (the claim is the first statement of the creating transaction), so the value is
     * now needed in two places and must not be able to drift between them.
     */
    public static final String STATUS_PENDING = "PENDING";

    @Id
    @Column(name = "order_id")
    private UUID orderId;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "amount_cents", nullable = false)
    private long amountCents;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected Order() {}

    public Order(UUID orderId, String status, long amountCents, Instant createdAt, Instant updatedAt) {
        this.orderId = orderId;
        this.status = status;
        this.amountCents = amountCents;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public UUID getOrderId() {
        return orderId;
    }

    public String getStatus() {
        return status;
    }

    public long getAmountCents() {
        return amountCents;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    /** Saga reached {@code COMPLETED} (constitution item 3's happy-path terminal state). */
    public void confirm(Instant now) {
        this.status = "CONFIRMED";
        this.updatedAt = now;
    }

    /** Saga reached {@code COMPENSATED} — compensation succeeded (or none was needed). */
    public void cancel(Instant now) {
        this.status = "CANCELLED";
        this.updatedAt = now;
    }

    /**
     * Saga reached {@code COMPENSATION_FAILED} (constitution item 6, the hard case): compensation
     * itself did not complete. Distinct from a clean {@code CANCELLED} so this order is visible via
     * SQL to an operator — {@code SELECT * FROM orders WHERE status = 'CANCELLATION_FAILED'} — as
     * needing the manual remediation in {@code docs/runbooks/compensation-failure.md}, not lost in
     * ordinary cancellations.
     */
    public void markCancellationFailed(Instant now) {
        this.status = "CANCELLATION_FAILED";
        this.updatedAt = now;
    }
}
