package com.eventforge.order.saga;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Order-service's own outbox relay provisions {@code orders.events} (the topic it produces to) via
 * {@code OutboxRelayAutoConfiguration}'s own {@code NewTopic} bean — but {@link SagaEventListener}
 * also CONSUMES {@code payments.events}/{@code inventory.events}, topics it neither owns nor
 * produces to, and nothing else in this service's own context provisions them. Relying on
 * broker-side auto-creation for topics a consumer subscribes to is fragile (partition/replication
 * count fall back to broker defaults, and startup ordering against payment-service/inventory-service
 * isn't guaranteed) — declared explicitly here instead, matching the same partition/replication
 * convention {@code OutboxRelayAutoConfiguration} uses for every other topic in this project
 * (ADR-0003).
 */
@Configuration
public class SagaTopicsConfiguration {

    @Bean
    public NewTopic paymentsEventsTopic() {
        return TopicBuilder.name("payments.events").partitions(3).replicas((short) 1).build();
    }

    @Bean
    public NewTopic inventoryEventsTopic() {
        return TopicBuilder.name("inventory.events").partitions(3).replicas((short) 1).build();
    }
}
