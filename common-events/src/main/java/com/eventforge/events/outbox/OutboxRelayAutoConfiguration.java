package com.eventforge.events.outbox;

import com.eventforge.events.fault.FaultInjector;
import com.eventforge.events.tracing.EventForgeTracer;
import java.time.Clock;
import java.time.Duration;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Wires up the reusable outbox relay — claiming, publishing, and Kafka topic provisioning — for
 * any service that opts in via {@code eventforge.outbox.relay.enabled=true}. Disabled by default,
 * so services with nothing to publish yet don't run a relay against an empty table.
 */
@AutoConfiguration
@EnableConfigurationProperties(OutboxRelayProperties.class)
@ConditionalOnProperty(prefix = "eventforge.outbox.relay", name = "enabled", havingValue = "true")
@EnableScheduling
public class OutboxRelayAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(Clock.class)
    public Clock clock() {
        return Clock.systemUTC();
    }

    @Bean
    public NewTopic outboxRelayTopic(OutboxRelayProperties properties) {
        return TopicBuilder.name(properties.topic())
                .partitions(properties.topicPartitions())
                .replicas(properties.topicReplicationFactor())
                .build();
    }

    @Bean
    public OutboxRelayWorker outboxRelayWorker(
            JdbcTemplate jdbcTemplate,
            KafkaTemplate<String, String> kafkaTemplate,
            FaultInjector faultInjector,
            EventForgeTracer tracer,
            Clock clock,
            OutboxRelayProperties properties) {
        return new OutboxRelayWorker(
                jdbcTemplate,
                kafkaTemplate,
                faultInjector,
                tracer,
                properties.topic(),
                clock,
                Duration.ofMillis(properties.retryBackoffMs()),
                Duration.ofMillis(properties.kafkaSendTimeoutMs()));
    }

    // Configuration, not logic: gated independently of the worker bean above, via
    // eventforge.outbox.relay.scheduler-enabled (default true — production behavior is
    // unchanged, asserted directly by OutboxRelaySchedulerEnabledByDefaultTest). A test that
    // wants the worker bean without the background poller racing its own explicit calls sets
    // this false; no @Scheduled method exists in the context at all when it's off, so there is
    // nothing for @EnableScheduling to schedule — not a longer interval standing in for "off."
    @Bean
    @ConditionalOnProperty(
            prefix = "eventforge.outbox.relay",
            name = "scheduler-enabled",
            havingValue = "true",
            matchIfMissing = true)
    public OutboxRelayScheduler outboxRelayScheduler(OutboxRelayWorker worker, OutboxRelayProperties properties) {
        return new OutboxRelayScheduler(worker, properties.batchCapPerPoll());
    }
}
