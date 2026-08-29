package com.eventforge.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.inventory.domain.InventoryEventProcessingService;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;

/**
 * inventory-service is wired identically to payment-service (dedupe, manual ack, resilience) but
 * its business logic stays deliberately minimal (reservation semantics are M3) — item 4's full
 * a-e suite lives in payment-service, the service with substantive business logic to exercise it
 * against. This is the baseline proof inventory-service's own consumer path — dedupe included —
 * actually works, rather than leaving it completely unverified.
 */
@SpringBootTest
class InventoryEventProcessingIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void consumesOrderCreatedOnceAndDedupesRedelivery() throws Exception {
        String orderId = "inventory-" + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        EventEnvelope envelope = new EventEnvelope(
                eventId,
                "OrderCreated",
                1,
                orderId,
                UUID.randomUUID(),
                null,
                Instant.now(),
                mapper.createObjectNode().put("orderId", orderId).put("amountCents", 500));
        String value = mapper.writeValueAsString(envelope);

        // Publish the same event twice for real - proves dedupe on the real consumer path, not
        // just direct method calls.
        for (int i = 0; i < 2; i++) {
            kafkaTemplate
                    .send(MessageBuilder.withPayload(value)
                            .setHeader(KafkaHeaders.TOPIC, "orders.events")
                            .setHeader(KafkaHeaders.KEY, orderId)
                            .build())
                    .get();
        }

        waitForRow(orderId);

        Integer inventoryRows = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM inventory_order_events WHERE order_id = ?", Integer.class, orderId);
        assertThat(inventoryRows).isEqualTo(1);

        Integer processedRows = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM processed_events WHERE consumer_group = ? AND event_id = ?",
                Integer.class,
                InventoryEventProcessingService.CONSUMER_GROUP,
                eventId);
        assertThat(processedRows).isEqualTo(1);
    }

    private void waitForRow(String orderId) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 30_000L;
        while (System.currentTimeMillis() < deadline) {
            Integer count = jdbcTemplate.queryForObject(
                    "SELECT count(*) FROM inventory_order_events WHERE order_id = ?", Integer.class, orderId);
            if (count != null && count >= 1) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Timed out waiting for inventory-service to process order " + orderId);
    }
}
