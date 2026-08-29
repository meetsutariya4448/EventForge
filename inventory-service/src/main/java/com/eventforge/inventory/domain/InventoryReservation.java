package com.eventforge.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/** One row per successful reservation — a failed ReserveInventory writes no row here. */
@Entity
@Table(name = "inventory_reservations")
public class InventoryReservation {

    @Id
    @Column(name = "reservation_id")
    private UUID reservationId;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(name = "sku", nullable = false)
    private String sku;

    @Column(name = "quantity", nullable = false)
    private long quantity;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "created_at", nullable = false)
    private Instant createdAt;

    @Column(name = "updated_at", nullable = false)
    private Instant updatedAt;

    protected InventoryReservation() {}

    public InventoryReservation(UUID reservationId, String orderId, String sku, long quantity, String status, Instant now) {
        this.reservationId = reservationId;
        this.orderId = orderId;
        this.sku = sku;
        this.quantity = quantity;
        this.status = status;
        this.createdAt = now;
        this.updatedAt = now;
    }

    public UUID getReservationId() {
        return reservationId;
    }

    public String getOrderId() {
        return orderId;
    }

    public String getSku() {
        return sku;
    }

    public long getQuantity() {
        return quantity;
    }

    public String getStatus() {
        return status;
    }
}
