package com.eventforge.events.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Config for the reusable outbox relay. Disabled by default — a service opts in by setting
 * {@code eventforge.outbox.relay.enabled=true} and a target {@code topic}. Only order-service
 * enables this in M1; payment/inventory/notification have nothing to publish yet, and will flip
 * this on with no new relay code once M3 gives them something to write to their outbox.
 */
@ConfigurationProperties(prefix = "eventforge.outbox.relay")
public record OutboxRelayProperties(
        @DefaultValue("false") boolean enabled,
        String topic,
        @DefaultValue("3") int topicPartitions,
        @DefaultValue("1") short topicReplicationFactor,
        @DefaultValue("1000") long pollIntervalMs,
        @DefaultValue("50") int batchCapPerPoll) {}
