package com.eventforge.order.idempotency;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;

/**
 * Deletes idempotency keys past their TTL. Thin by design — the {@link Clock}-driven decision
 * about what "expired" means lives in {@link IdempotencyKeyStore#deleteExpired()}, mirroring how
 * {@code SagaTimeoutSweeper} delegates its deadline logic to {@code SagaOrchestrator}.
 *
 * <p><b>Why this bean is separately switchable.</b> Spring's {@code @Scheduled} has no
 * {@code initialDelay} here, so its first tick fires on the scheduler's own thread with no
 * happens-before relationship to a test method's thread. M4 lost a full diagnostic session to
 * exactly that shape: a background poller consumed a row before the test's own explicit call
 * could, and the test's "expected PUBLISHED but was NOTHING_TO_CLAIM" failure looked like a
 * production bug rather than a test race. Pushing an interval far out only makes such a race
 * rare; {@code eventforge.idempotency.reaper-enabled=false} removes the bean, so there is no
 * {@code @Scheduled} method in the context at all and nothing left to race. Tests drive
 * {@link IdempotencyKeyStore#deleteExpired()} directly instead.
 */
public class IdempotencyKeyReaper {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyKeyReaper.class);

    private final IdempotencyKeyStore store;

    public IdempotencyKeyReaper(IdempotencyKeyStore store) {
        this.store = store;
    }

    @Scheduled(fixedDelayString = "${eventforge.idempotency.reap-interval-ms:300000}")
    public void reap() {
        int deleted = store.deleteExpired();
        if (deleted > 0) {
            log.info("Reaped {} expired idempotency key(s)", deleted);
        }
    }
}
