package com.eventforge.events.outbox;

import org.springframework.scheduling.annotation.Scheduled;

/**
 * Adaptive polling: drains immediately, with no sleep, for as long as a drain pass fills up to
 * {@code batchCapPerPoll} (there's likely more work waiting); falls back to the configured
 * {@code @Scheduled} interval only once a pass comes up short of the cap — whether because there
 * was nothing left to claim, or because a claimed row failed to publish. A fixed poll interval
 * would make publication delay a measurement of the sleep timer rather than of the system (see the
 * ADR-0010 amendment).
 */
public class OutboxRelayScheduler {

    private final OutboxRelayWorker worker;
    private final int batchCapPerPoll;

    public OutboxRelayScheduler(OutboxRelayWorker worker, int batchCapPerPoll) {
        this.worker = worker;
        this.batchCapPerPoll = batchCapPerPoll;
    }

    @Scheduled(fixedDelayString = "${eventforge.outbox.relay.poll-interval-ms:1000}")
    public void poll() {
        boolean keepDraining;
        do {
            int published = 0;
            RelayOutcome outcome;
            do {
                outcome = worker.relayNextEvent();
                if (outcome == RelayOutcome.PUBLISHED) {
                    published++;
                }
            } while (outcome == RelayOutcome.PUBLISHED && published < batchCapPerPoll);
            // Hit the cap while still succeeding: there's likely more work waiting, so loop again
            // immediately instead of waiting for the next @Scheduled tick.
            keepDraining = published == batchCapPerPoll;
        } while (keepDraining);
    }
}
