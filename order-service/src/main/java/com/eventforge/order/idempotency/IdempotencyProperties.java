package com.eventforge.order.idempotency;

import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Config for {@code Idempotency-Key} handling on {@code POST /orders}.
 *
 * @param ttl how long a claimed key is honoured. A retry arriving after this window is treated
 *     as a new request and creates a second order — the same honest consequence ADR-0012 records
 *     for {@code processed_events} retention, stated rather than engineered around.
 * @param lockTimeout how long a request will block behind another transaction's in-flight claim
 *     on the same key before giving up with a retryable 409. Deliberately short: a blocked
 *     request pins a servlet thread and a pooled connection for this long, so the value is a
 *     pool-exhaustion bound, not a patience setting. See ADR-0021.
 * @param reaperEnabled whether the background {@link IdempotencyKeyReaper} bean exists at all.
 *     Tests set this false to remove the {@code @Scheduled} method from the context entirely
 *     rather than merely making its interval long — see that class for why the distinction
 *     matters.
 */
@ConfigurationProperties(prefix = "eventforge.idempotency")
public record IdempotencyProperties(
        @DefaultValue("24h") Duration ttl,
        @DefaultValue("1s") Duration lockTimeout,
        @DefaultValue("true") boolean reaperEnabled) {}
