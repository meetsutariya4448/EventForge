package com.eventforge.events.consumer;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Config for the reusable consumer-side resilience wiring (bounded retry with backoff, container
 * pause/resume — constitution item 5, no circuit breaker library). Arbitrary defaults, not tuned —
 * consistent with the relay's own retry knobs (ADR-0010), M7 measures and revisits.
 */
@ConfigurationProperties(prefix = "eventforge.consumer.resilience")
public record ConsumerResilienceProperties(@DefaultValue("3") int maxRetries, @DefaultValue("500") long backoffMs) {}
