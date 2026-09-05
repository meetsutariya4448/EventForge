package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.eventforge.order.domain.OrderService;
import com.eventforge.order.idempotency.IdempotencyClaimInFlightException;
import com.eventforge.order.idempotency.IdempotencyKeyStore;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Pins the exception type behind {@code POST /orders}' 409, by making the race happen for real
 * rather than reasoning about what Postgres "should" raise.
 *
 * <p>One transaction claims a key and is held open; a second tries to claim the same key and is
 * blocked on the winner's speculative insertion lock until {@code lock_timeout} fires.
 *
 * <p><b>This test already earned its keep.</b> The first version of the 409 handler caught
 * {@code org.springframework.dao.CannotAcquireLockException}, on the reasonable-sounding
 * assumption that Spring maps a lock timeout to a concurrency-failure type. Running this proved
 * otherwise: Postgres raises SQLSTATE {@code 55P03} ("canceling statement due to lock timeout")
 * and Spring's translator leaves class 55 <em>uncategorized</em>, delivering
 * {@code UncategorizedSQLException}. The handler would never have fired and callers would have
 * received 500 instead of a retryable 409. {@link IdempotencyKeyStore} now matches on the
 * SQLSTATE itself and raises {@link IdempotencyClaimInFlightException}; this test fails loudly
 * if that translation ever stops working.
 */
@SpringBootTest
class IdempotencyLockTimeoutIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        registry.add("eventforge.idempotency.reaper-enabled", () -> "false");
        // Short enough to keep the test quick; the mechanism is identical at any value.
        registry.add("eventforge.idempotency.lock-timeout", () -> "300ms");
        // The blocked caller holds a connection for the whole lock_timeout window, so the pool
        // must have room for both participants plus the container's own needs.
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> "10");
    }

    @Autowired
    private OrderService orderService;

    @Autowired
    private IdempotencyKeyStore idempotencyKeyStore;

    @Autowired
    private PlatformTransactionManager transactionManager;

    @Test
    void aClaimHeldByAnUncommittedTransactionSurfacesAsTheTypeMappedTo409() throws Exception {
        String key = "lock-timeout-" + UUID.randomUUID();
        CountDownLatch claimed = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            // Holder: claims the key, then sits on the open transaction so the claim stays
            // uncommitted and therefore still locked.
            Future<?> holder = executor.submit(() -> {
                new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
                    idempotencyKeyStore.tryClaim(key, "fingerprint-a", UUID.randomUUID(), 201, "{}");
                    claimed.countDown();
                    try {
                        release.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            });

            assertThat(claimed.await(30, TimeUnit.SECONDS)).isTrue();

            Throwable thrown = catchThrowable(
                    () -> orderService.createOrderIdempotent(key, 1500, "SKU-LOCK", 1));

            assertThat(thrown)
                    .as("a claim held by an uncommitted transaction must surface as the exception "
                            + "OrderController maps to 409, not as an unmapped failure that would "
                            + "become a 500")
                    .isInstanceOf(IdempotencyClaimInFlightException.class);

            release.countDown();
            holder.get(30, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void lockTimeoutIsBoundedRatherThanWaitingForTheHolder() throws Exception {
        String key = "lock-bound-" + UUID.randomUUID();
        CountDownLatch claimed = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            executor.submit(() -> new TransactionTemplate(transactionManager).executeWithoutResult(status -> {
                idempotencyKeyStore.tryClaim(key, "fingerprint-a", UUID.randomUUID(), 201, "{}");
                claimed.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }));

            assertThat(claimed.await(30, TimeUnit.SECONDS)).isTrue();

            long startedAt = System.nanoTime();
            catchThrowable(() -> orderService.createOrderIdempotent(key, 1500, "SKU-LOCK", 1));
            Duration blockedFor = Duration.ofNanos(System.nanoTime() - startedAt);

            // The holder is still holding when this assertion runs: the caller gave up on its own
            // timeout rather than waiting out the other transaction. Bounded generously against
            // the 30s holder window, so this asserts "gave up early", not a precise duration.
            assertThat(blockedFor)
                    .as("the blocked caller must give up on lock_timeout, not wait for the holder")
                    .isLessThan(Duration.ofSeconds(10));
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }
}
