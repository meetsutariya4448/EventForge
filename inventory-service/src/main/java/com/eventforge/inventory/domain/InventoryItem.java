package com.eventforge.inventory.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

/**
 * Real, minimal stock arithmetic (M3) — enough to make ReserveInventory a genuine business
 * decision (sufficient stock vs. not), not a fault-injection hook standing in for one. This is
 * also what makes constitution item 8f's "two sagas touch the same inventory item" scenario a real
 * concurrency test rather than a simulated one: {@link InventoryItemRepository#findWithLockBySku}
 * takes a real row lock, so concurrent reservations against the same SKU genuinely serialize.
 */
@Entity
@Table(name = "inventory_items")
public class InventoryItem {

    @Id
    @Column(name = "sku")
    private String sku;

    @Column(name = "available_quantity", nullable = false)
    private long availableQuantity;

    protected InventoryItem() {}

    public InventoryItem(String sku, long availableQuantity) {
        this.sku = sku;
        this.availableQuantity = availableQuantity;
    }

    public String getSku() {
        return sku;
    }

    public long getAvailableQuantity() {
        return availableQuantity;
    }

    public boolean reserve(long quantity) {
        if (availableQuantity < quantity) {
            return false;
        }
        availableQuantity -= quantity;
        return true;
    }
}
