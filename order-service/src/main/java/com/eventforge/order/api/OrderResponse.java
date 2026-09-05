package com.eventforge.order.api;

import com.eventforge.order.domain.Order;
import java.util.UUID;

public record OrderResponse(UUID orderId, String status, long amountCents) {

    /**
     * The single place a create-order response is constructed, for every path: the plain
     * create, the idempotent winner's returned body, and the JSON that winner stores for a later
     * retry to replay.
     *
     * <p>That last pair is why this factory exists rather than three {@code new OrderResponse(…)}
     * calls. The stored body and the returned body must be the same shape by construction, not
     * by coincidence — otherwise a future field added to one path and not the other would make a
     * replayed response silently differ from a fresh one, which is exactly the bug idempotency
     * is supposed to rule out.
     *
     * <p>Takes an id and an amount rather than an {@code Order} because the idempotent path must
     * render the response before the row exists — the claim is the first statement of the
     * creating transaction.
     */
    public static OrderResponse forNewOrder(UUID orderId, long amountCents) {
        return new OrderResponse(orderId, Order.STATUS_PENDING, amountCents);
    }
}
