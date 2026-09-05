package com.eventforge.order.idempotency;

/**
 * The same {@code Idempotency-Key} was reused with a different request body. That is a client
 * bug rather than a retry, so the request is refused: replaying the stored response would hand
 * the caller an order that does not match what it just asked for, and executing the new body
 * under an already-used key would defeat the point of the key.
 *
 * <p>Deliberately its own type rather than a bare {@code RuntimeException} — the controller maps
 * it to 422, and an unmapped runtime exception would land in the 500 bucket instead, reporting a
 * server fault for what is squarely a caller error.
 */
public class IdempotencyKeyConflictException extends RuntimeException {

    public IdempotencyKeyConflictException(String message) {
        super(message);
    }
}
