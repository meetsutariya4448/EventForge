package com.eventforge.inventory;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.consumer.ConsumerOutcome;
import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.inventory.domain.InventoryReservationService;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Constitution item 8f, the sharper half: real row-locked contention when multiple sagas (here,
 * standing in as N concurrent ReserveInventory commands for N distinct orders) touch the SAME
 * inventory item and stock is scarce enough that not all of them can succeed. This is what proves
 * {@link com.eventforge.inventory.domain.InventoryItemRepository#findWithLockBySku}'s pessimistic
 * lock actually serializes the check-and-decrement under real concurrency, not just sequentially:
 * exactly as many reservations succeed as there is stock for, the rest fail cleanly, and the final
 * available_quantity is exactly zero — never negative (which would mean the lock didn't hold) and
 * never left with unclaimed stock (which would mean a legitimate reservation was wrongly rejected).
 */
@SpringBootTest
class ConcurrentInventoryReservationIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final int CONCURRENT_ORDERS = 20;
    private static final long STARTING_STOCK = 8;

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.poll-interval-ms", () -> "3600000");
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> String.valueOf(CONCURRENT_ORDERS + 5));
    }

    @Autowired
    private InventoryReservationService service;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void concurrentReservationsAgainstAScarceSharedItemNeverOversell() throws Exception {
        String sku = "sku-contended-" + UUID.randomUUID();
        jdbcTemplate.update("INSERT INTO inventory_items (sku, available_quantity) VALUES (?, ?)", sku, STARTING_STOCK);

        List<String> orderIds = new ArrayList<>();
        for (int i = 0; i < CONCURRENT_ORDERS; i++) {
            orderIds.add("contended-" + UUID.randomUUID());
        }

        AtomicInteger succeeded = new AtomicInteger();
        AtomicInteger failed = new AtomicInteger();
        CountDownLatch startingGate = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENT_ORDERS);
        try {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (String orderId : orderIds) {
                tasks.add(() -> {
                    startingGate.await();
                    EventEnvelope command = new EventEnvelope(
                            UUID.randomUUID(),
                            "ReserveInventory",
                            1,
                            orderId,
                            UUID.randomUUID(),
                            UUID.randomUUID(),
                            Instant.now(),
                            mapper.createObjectNode().put("orderId", orderId).put("sku", sku).put("quantity", 1));
                    ConsumerOutcome outcome = service.handleReserveInventory(command, null, null);
                    assertThat(outcome).isEqualTo(ConsumerOutcome.PROCESSED);
                    String status = jdbcTemplate.queryForObject(
                            "SELECT count(*) FROM inventory_reservations WHERE order_id = ?", Integer.class, orderId) == 1
                            ? "RESERVED"
                            : "FAILED";
                    if ("RESERVED".equals(status)) {
                        succeeded.incrementAndGet();
                    } else {
                        failed.incrementAndGet();
                    }
                    return null;
                });
            }

            List<Future<Void>> futures = new ArrayList<>();
            for (Callable<Void> task : tasks) {
                futures.add(executor.submit(task));
            }
            startingGate.countDown();
            for (Future<Void> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        // Real contention, real outcome: exactly as many reservations succeeded as there was stock
        // for, no over-selling (negative stock) and no under-selling (stock left unclaimed while a
        // legitimate request was wrongly rejected).
        assertThat(succeeded.get()).isEqualTo((int) STARTING_STOCK);
        assertThat(failed.get()).isEqualTo(CONCURRENT_ORDERS - (int) STARTING_STOCK);

        Long remaining = jdbcTemplate.queryForObject("SELECT available_quantity FROM inventory_items WHERE sku = ?", Long.class, sku);
        assertThat(remaining).isEqualTo(0L);

        Integer reservedRowCount = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM inventory_reservations WHERE sku = ? AND status = 'RESERVED'", Integer.class, sku);
        assertThat(reservedRowCount).isEqualTo((int) STARTING_STOCK);
    }
}
