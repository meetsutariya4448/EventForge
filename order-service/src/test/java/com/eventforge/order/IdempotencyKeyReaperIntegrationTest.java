package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.order.idempotency.IdempotencyKeyStore;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Retention: a claimed key is honoured for its TTL and then removed, so the table does not grow
 * without bound.
 *
 * <p>Time is moved by constructing a store around a different {@link Clock}, and the reap is
 * invoked <em>directly</em> — never by waiting on the {@code @Scheduled} bean. That is the same
 * discipline {@code SagaTimeoutIntegrationTest} follows for the saga sweep, and it exists because
 * of a real M4 incident: a background poller with no {@code initialDelay} raced a test's own
 * explicit call and consumed the row first, producing a failure that looked like a production
 * bug. The reaper bean is switched off entirely here rather than merely slowed, so there is no
 * {@code @Scheduled} method in the context to race at all.
 */
@SpringBootTest
class IdempotencyKeyReaperIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final Duration TTL = Duration.ofHours(24);

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        registry.add("eventforge.idempotency.reaper-enabled", () -> "false");
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    /**
     * {@code deleteExpired()} returns a table-wide count, so these assertions are only meaningful
     * against a table this test owns outright. Without this, a row left behind by a sibling test
     * — still unexpired at its own reap instant, but long expired at another's — gets swept up
     * and counted here. That is exactly what happened when this class was first written: the
     * "nothing has expired" case deleted one row, and the cause was the neighbouring test's
     * survivor, not the reaper.
     */
    @BeforeEach
    void clearKeys() {
        jdbcTemplate.update("DELETE FROM idempotency_keys");
    }

    @Test
    void reapingRemovesKeysPastTheirTtlAndLeavesTheRestAlone() {
        Instant t0 = Instant.parse("2026-01-01T00:00:00Z");
        String oldKey = "reaper-old-" + UUID.randomUUID();
        String freshKey = "reaper-fresh-" + UUID.randomUUID();

        // Claimed at t0, so it expires at t0 + 24h.
        storeAt(t0).tryClaim(oldKey, "fingerprint", UUID.randomUUID(), 201, "{}");

        // Claimed a day later, so it is still well within its own TTL at the reap instant below.
        Instant t1 = t0.plus(Duration.ofHours(25));
        storeAt(t1).tryClaim(freshKey, "fingerprint", UUID.randomUUID(), 201, "{}");

        assertThat(exists(oldKey)).isTrue();
        assertThat(exists(freshKey)).isTrue();

        int deleted = storeAt(t1).deleteExpired();

        assertThat(deleted).as("only the key past its TTL is reaped").isEqualTo(1);
        assertThat(exists(oldKey)).as("expired key is gone").isFalse();
        assertThat(exists(freshKey)).as("unexpired key is retained").isTrue();
    }

    @Test
    void reapingIsANoOpWhenNothingHasExpiredYet() {
        Instant t0 = Instant.parse("2026-02-01T00:00:00Z");
        String key = "reaper-none-" + UUID.randomUUID();
        storeAt(t0).tryClaim(key, "fingerprint", UUID.randomUUID(), 201, "{}");

        // One second before it expires.
        int deleted = storeAt(t0.plus(TTL).minusSeconds(1)).deleteExpired();

        assertThat(deleted).isZero();
        assertThat(exists(key)).isTrue();
    }

    /** A store whose notion of "now" is {@code instant} — the injected-Clock seam, used directly. */
    private IdempotencyKeyStore storeAt(Instant instant) {
        return new IdempotencyKeyStore(jdbcTemplate, Clock.fixed(instant, ZoneOffset.UTC), TTL);
    }

    private boolean exists(String idempotencyKey) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM idempotency_keys WHERE idempotency_key = ?", Integer.class, idempotencyKey);
        return count != null && count > 0;
    }
}
