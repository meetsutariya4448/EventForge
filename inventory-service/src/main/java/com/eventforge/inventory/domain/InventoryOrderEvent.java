package com.eventforge.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * Deliberately minimal (constitution scope for M2): records that inventory-service saw an
 * OrderCreated for this order. No stock or reservation semantics — that's M3.
 */
@Entity
@Table(name = "inventory_order_events")
public class InventoryOrderEvent {

    @Id
    @Column(name = "inventory_event_id")
    private UUID inventoryEventId;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "received_at", nullable = false)
    private Instant receivedAt;

    protected InventoryOrderEvent() {}

    public InventoryOrderEvent(UUID inventoryEventId, String orderId, Instant receivedAt) {
        this.inventoryEventId = inventoryEventId;
        this.orderId = orderId;
        this.receivedAt = receivedAt;
    }

    public UUID getInventoryEventId() {
        return inventoryEventId;
    }

    public String getOrderId() {
        return orderId;
    }

    public Instant getReceivedAt() {
        return receivedAt;
    }
}
