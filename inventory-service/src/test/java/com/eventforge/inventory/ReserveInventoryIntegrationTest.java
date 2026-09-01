package com.eventforge.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.consumer.ConsumerOutcome;
import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.inventory.domain.InventoryReservationService;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Real reservation semantics (M3, superseding M2's minimal stub): ReserveInventory either
 * genuinely decrements stock and reports InventoryReserved, or a genuine insufficient-quantity
 * business failure reports InventoryReservationFailed — constitution item 8b's "forced inventory
 * failure" is forced by seeding real scarcity, not a test hook standing in for the business logic.
 * Also proves the same M2 dedupe mechanism applies here unchanged.
 */
@SpringBootTest
class ReserveInventoryIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
    }

    @Autowired
    private InventoryReservationService service;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void sufficientStockReservesAndPublishesInventoryReserved() throws Exception {
        String sku = "sku-" + UUID.randomUUID();
        seedStock(sku, 10);
        String orderId = "reserve-ok-" + UUID.randomUUID();

        EventEnvelope command = reserveCommand(orderId, UUID.randomUUID(), sku, 4);
        ConsumerOutcome outcome = service.handleReserveInventory(command);
        assertThat(outcome).isEqualTo(ConsumerOutcome.PROCESSED);

        Long remaining = jdbcTemplate.queryForObject("SELECT available_quantity FROM inventory_items WHERE sku = ?", Long.class, sku);
        assertThat(remaining).isEqualTo(6L);

        String reservationStatus = jdbcTemplate.queryForObject(
                "SELECT status FROM inventory_reservations WHERE order_id = ?", String.class, orderId);
        assertThat(reservationStatus).isEqualTo("RESERVED");

        Integer reservedOutbox = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'InventoryReserved'",
                Integer.class,
                orderId);
        assertThat(reservedOutbox).isEqualTo(1);
    }

    @Test
    void insufficientStockIsAGenuineBusinessFailureNotATestHook() throws Exception {
        String sku = "sku-scarce-" + UUID.randomUUID();
        seedStock(sku, 1);
        String orderId = "reserve-fail-" + UUID.randomUUID();

        EventEnvelope command = reserveCommand(orderId, UUID.randomUUID(), sku, 5);
        ConsumerOutcome outcome = service.handleReserveInventory(command);
        assertThat(outcome).isEqualTo(ConsumerOutcome.PROCESSED);

        Long remaining = jdbcTemplate.queryForObject("SELECT available_quantity FROM inventory_items WHERE sku = ?", Long.class, sku);
        assertThat(remaining).isEqualTo(1L); // untouched - the failed reservation must not decrement stock

        Integer reservationRows = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM inventory_reservations WHERE order_id = ?", Integer.class, orderId);
        assertThat(reservationRows).isEqualTo(0);

        Integer failedOutbox = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'InventoryReservationFailed'",
                Integer.class,
                orderId);
        assertThat(failedOutbox).isEqualTo(1);
    }

    @Test
    void duplicateReserveCommandReservesExactlyOnce() throws Exception {
        String sku = "sku-dup-" + UUID.randomUUID();
        seedStock(sku, 10);
        String orderId = "reserve-dup-" + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();

        EventEnvelope command = reserveCommand(orderId, eventId, sku, 3);
        service.handleReserveInventory(command);
        ConsumerOutcome second = service.handleReserveInventory(command);
        assertThat(second).isEqualTo(ConsumerOutcome.DUPLICATE);

        Long remaining = jdbcTemplate.queryForObject("SELECT available_quantity FROM inventory_items WHERE sku = ?", Long.class, sku);
        assertThat(remaining).isEqualTo(7L); // decremented once, not twice
    }

    private void seedStock(String sku, long quantity) {
        jdbcTemplate.update("INSERT INTO inventory_items (sku, available_quantity) VALUES (?, ?)", sku, quantity);
    }

    private EventEnvelope reserveCommand(String orderId, UUID eventId, String sku, long quantity) {
        return new EventEnvelope(
                eventId,
                "ReserveInventory",
                1,
                orderId,
                UUID.randomUUID(),
                UUID.randomUUID(),
                Instant.now(),
                mapper.createObjectNode().put("orderId", orderId).put("sku", sku).put("quantity", quantity));
    }
}
