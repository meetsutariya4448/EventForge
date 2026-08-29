package com.eventforge.inventory;

import com.eventforge.testing.architecture.OutboxOnlyPublishingRule;
import org.junit.jupiter.api.Test;

/** M3 item 7: fails the build if anything in inventory-service publishes to Kafka outside the outbox. */
class NoDirectKafkaPublishArchitectureTest {

    @Test
    void onlyOutboxWriterPublishesToKafka() {
        OutboxOnlyPublishingRule.assertOnlyOutboxWriterPublishesToKafka("com.eventforge.inventory");
    }
}
