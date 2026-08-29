package com.eventforge.order.api;

/**
 * {@code sku}/{@code quantity} feed the saga's ReserveInventory command (M3). Both are optional and
 * default to a single unit of a shared placeholder SKU — this project has no real product catalog,
 * so callers who don't care about inventory contention (most tests) don't need to specify one, while
 * tests that deliberately want two sagas to contend over the same item (constitution item 8f) can.
 */
public record CreateOrderRequest(long amountCents, String sku, Long quantity) {

    public static final String DEFAULT_SKU = "DEFAULT-SKU";

    public CreateOrderRequest {
        if (sku == null || sku.isBlank()) {
            sku = DEFAULT_SKU;
        }
        if (quantity == null) {
            quantity = 1L;
        }
    }
}
