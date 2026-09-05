package com.eventforge.order.idempotency;

/**
 * Another transaction holds an uncommitted claim on this {@code Idempotency-Key} and did not
 * finish within {@code eventforge.idempotency.lock-timeout}. The caller's own retry is the right
 * resolution, so this maps to a retryable 409 rather than a server error.
 *
 * <p>This type exists because of a verified fact, not an assumption: Postgres raises SQLSTATE
 * {@code 55P03} (lock_not_available) when {@code lock_timeout} fires, and Spring's exception
 * translator does <em>not</em> map SQLSTATE class 55 to {@code CannotAcquireLockException} or any
 * other concurrency type — it falls through to {@code UncategorizedSQLException}. Relying on the
 * translator would therefore have produced a 500. {@link IdempotencyKeyStore} recognises the
 * SQLSTATE itself and raises this instead, keeping that knowledge next to the SQL that provokes
 * it rather than leaking it into a controller that would have to catch a far broader type. See
 * ADR-0021.
 */
public class IdempotencyClaimInFlightException extends RuntimeException {

    public IdempotencyClaimInFlightException(String idempotencyKey, Throwable cause) {
        super("A request with Idempotency-Key '" + idempotencyKey + "' is currently in flight", cause);
    }
}
