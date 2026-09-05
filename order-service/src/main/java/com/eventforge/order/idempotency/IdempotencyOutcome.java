package com.eventforge.order.idempotency;

import com.eventforge.order.api.OrderResponse;

/**
 * What an idempotent create-order attempt actually did: created the order, or found that an
 * earlier request under the same key already had.
 *
 * <p>Sealed so the controller must handle both arms — the difference is visible to callers (a
 * replay carries the {@code Idempotency-Replayed} header) and silently collapsing the two would
 * make a retry indistinguishable from a first request.
 */
public sealed interface IdempotencyOutcome {

    /** This request won the claim and performed the write. */
    record Created(OrderResponse response) implements IdempotencyOutcome {}

    /**
     * An earlier request under this key already created the order; this is its stored response,
     * replayed verbatim rather than re-rendered.
     */
    record Replayed(int status, String responseBody) implements IdempotencyOutcome {}
}
