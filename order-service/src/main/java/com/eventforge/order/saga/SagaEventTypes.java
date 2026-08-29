package com.eventforge.order.saga;

/**
 * Every {@code event_type} value that can appear on an outbox row or inbound envelope anywhere in
 * the saga (constitution item 3's happy path and compensation path, named literally). Commands
 * (dispatched by the orchestrator) and facts (published by participants in response) all
 * multiplex onto each service's single existing outbox topic — see ADR-0014 — distinguished only
 * by this field, the same filtering pattern M2 already established for {@code OrderCreated}.
 */
public final class SagaEventTypes {

    // Orchestrator-authored, on orders.events.
    public static final String ORDER_CREATED = "OrderCreated";
    public static final String AUTHORIZE_PAYMENT = "AuthorizePayment";
    public static final String RESERVE_INVENTORY = "ReserveInventory";
    public static final String REFUND_PAYMENT = "RefundPayment";
    public static final String ORDER_CONFIRMED = "OrderConfirmed";
    public static final String ORDER_CANCELLED = "OrderCancelled";

    // payment-service-authored, on payments.events.
    public static final String PAYMENT_AUTHORIZED = "PaymentAuthorized";
    public static final String PAYMENT_REFUNDED = "PaymentRefunded";

    // inventory-service-authored, on inventory.events.
    public static final String INVENTORY_RESERVED = "InventoryReserved";
    public static final String INVENTORY_RESERVATION_FAILED = "InventoryReservationFailed";

    private SagaEventTypes() {}
}
