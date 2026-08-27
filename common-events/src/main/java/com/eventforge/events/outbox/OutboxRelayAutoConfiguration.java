package com.eventforge.events.outbox;

import com.eventforge.events.fault.FaultInjector;
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
            Clock clock,
            OutboxRelayProperties properties) {
        return new OutboxRelayWorker(
                jdbcTemplate,
                kafkaTemplate,
                faultInjector,
                properties.topic(),
                clock,
                Duration.ofMillis(properties.retryBackoffMs()),
                Duration.ofMillis(properties.kafkaSendTimeoutMs()));
    }

    @Bean
    public OutboxRelayScheduler outboxRelayScheduler(OutboxRelayWorker worker, OutboxRelayProperties properties) {
        return new OutboxRelayScheduler(worker, properties.batchCapPerPoll());
    }
}
