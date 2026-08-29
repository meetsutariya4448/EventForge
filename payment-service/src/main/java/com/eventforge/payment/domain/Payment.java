package com.eventforge.payment.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "payments")
public class Payment {

    @Id
    @Column(name = "payment_id")
    private UUID paymentId;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "amount_cents", nullable = false)
    private long amountCents;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    // This order's next free outbox_events.aggregate_sequence (M3): PaymentAuthorized always
    // consumes sequence 1 in the same call that creates this row, so this starts at 2 and is
    // allocated from for every write afterward (e.g. PaymentRefunded), however many separate
    // transactions those turn out to span.
    @Column(name = "next_sequence", nullable = false)
    private long nextSequence;

    protected Payment() {}

    public Payment(UUID paymentId, String orderId, long amountCents, String status, Instant createdAt, Instant updatedAt) {
        this.paymentId = paymentId;
        this.orderId = orderId;
        this.amountCents = amountCents;
        this.status = status;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
        this.nextSequence = 2L;
    }

    public UUID getPaymentId() {
        return paymentId;
    }

    public String getOrderId() {
        return orderId;
    }

    public long getAmountCents() {
        return amountCents;
    }

    public String getStatus() {
        return status;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getUpdatedAt() {
        return updatedAt;
    }

    public void markRefunded(Instant now) {
        this.status = "REFUNDED";
        this.updatedAt = now;
    }

    /** Returns this order's next free {@code aggregate_sequence} and reserves it. */
    public long allocateNextSequence() {
        return nextSequence++;
    }
}
