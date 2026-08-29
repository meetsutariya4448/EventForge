package com.eventforge.order.saga;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Saga timeout/compensation config (constitution items 5 and 6). Defaults are arbitrary, not
 * tuned — same posture as {@code OutboxRelayProperties}' backoff defaults (R2: not presented as a
 * measured number).
 */
@ConfigurationProperties(prefix = "eventforge.saga")
public record SagaProperties(
        @DefaultValue("10000") long authorizePaymentTimeoutMs,
        @DefaultValue("10000") long reserveInventoryTimeoutMs,
        @DefaultValue("10000") long refundPaymentTimeoutMs,
        // Bounded (constitution item 6): after this many RefundPayment dispatches with no
        // PaymentRefunded response, the saga stops retrying and reaches COMPENSATION_FAILED
        // instead of redispatching forever.
        @DefaultValue("3") int maxCompensationAttempts,
        @DefaultValue("1000") long sweepIntervalMs) {}
