package com.eventforge.events.outbox;

import org.springframework.scheduling.annotation.Scheduled;

/**
 * Polls {@link OutboxRelayWorker} on a fixed delay, draining up to {@code batchCapPerPoll} rows
 * per tick before yielding — bounds how long one poll cycle can run without an unbounded loop.
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
        int processed = 0;
        while (processed < batchCapPerPoll && worker.relayNextEvent()) {
            processed++;
        }
    }
}
