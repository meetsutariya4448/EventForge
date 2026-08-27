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
        // The fallback interval used only when a drain pass comes up short of batchCapPerPoll
        // (nothing left to claim, or a failed publish) — see OutboxRelayScheduler's adaptive
        // draining. Not tuned; M7 measures and revisits (constitution R2).
        @DefaultValue("1000") long pollIntervalMs,
        // Raised from M1's original 50: a low ceiling makes M6's backlog-generation scenarios
        // impractically slow to set up, since one adaptive-drain pass is capped by this value
        // before falling back to pollIntervalMs. Not a tuned value — M7 measures and tunes.
        @DefaultValue("500") int batchCapPerPoll,
        // How long a failed row sits out before it's eligible to be claimed again. Arbitrary
        // default, not tuned — M7 measures under a slow/degraded broker and revisits (see the
        // ADR-0010 amendment on the lease-based-claiming alternative this doesn't yet implement).
        @DefaultValue("2000") long retryBackoffMs,
        @DefaultValue("10000") long kafkaSendTimeoutMs) {}
