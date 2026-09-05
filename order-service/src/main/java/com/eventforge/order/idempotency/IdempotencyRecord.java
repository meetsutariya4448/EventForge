package com.eventforge.order.idempotency;

import java.util.UUID;

/**
 * A committed claim: which request won a given {@code Idempotency-Key}, and the response it was
 * served. {@code responseBody} is the stored JSON verbatim, replayed byte-for-byte to a later
 * retry rather than re-rendered from the order — see {@code OrderService}'s single response
 * builder for why re-rendering would let the two paths drift apart.
 */
public record IdempotencyRecord(
        String idempotencyKey, String requestFingerprint, UUID orderId, int responseStatus, String responseBody) {}
