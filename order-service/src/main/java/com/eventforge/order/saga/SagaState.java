package com.eventforge.order.saga;

/**
 * The saga's own state machine — see {@code docs/saga-state-machine.md} for the full diagram.
 * {@code saga_instance.state} stores {@link #name()} directly.
 */
public enum SagaState {
    /** AuthorizePayment dispatched, waiting for PaymentAuthorized. */
    AWAITING_PAYMENT,
    /** ReserveInventory dispatched, waiting for InventoryReserved/InventoryReservationFailed. */
    AWAITING_INVENTORY,
    /** RefundPayment dispatched, waiting for PaymentRefunded — the compensation path. */
    AWAITING_REFUND,
    /** Terminal, success: OrderConfirmed. */
    COMPLETED,
    /** Terminal, compensated: OrderCancelled. */
    COMPENSATED,
    /**
     * Terminal, the hard case (constitution item 6): compensation was attempted up to the
     * configured bound and never confirmed. Requires manual remediation — see
     * {@code docs/runbooks/compensation-failure.md}.
     */
    COMPENSATION_FAILED
}
