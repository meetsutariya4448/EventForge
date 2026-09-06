package com.eventforge.events.failure;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * Config for durable failure capture and replay (v2 WS3).
 *
 * @param enabled on by default: a service that has the table and consumes Kafka should not lose
 *     failed records because someone forgot to opt in. Turning it off restores the previous
 *     log-and-skip behaviour exactly, which is what {@code FailureStoreUnavailable
 *     IntegrationTest}'s control arm relies on.
 * @param fallbackConsumerGroup used only if Spring Kafka hands the recoverer no {@code Consumer}
 *     to read the real group from — see {@link DurableFailureRecoverer}.
 * @param kafkaSendTimeoutMs how long a replay waits for the broker to acknowledge before it is
 *     recorded as failed and left eligible for another attempt. Same arbitrary-but-bounded
 *     rationale as the relay's own send timeout (ADR-0010).
 */
@ConfigurationProperties(prefix = "eventforge.failure-capture")
public record FailureCaptureProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("unknown") String fallbackConsumerGroup,
        @DefaultValue("5000") long kafkaSendTimeoutMs) {}
