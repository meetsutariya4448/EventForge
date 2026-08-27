package com.eventforge.events.outbox;

/** The result of one {@link OutboxRelayWorker#relayNextEvent()} call. */
public enum RelayOutcome {
    /** Nothing eligible to claim right now (either the outbox is empty, or everything pending is
     * still within its retry backoff window). */
    NOTHING_TO_CLAIM,

    /** The claimed row was published to Kafka and marked published. */
    PUBLISHED,

    /** The claimed row failed to publish (a real, recoverable failure — not a simulated crash).
     * The attempt was recorded durably; the row stays unpublished and eligible again once its
     * retry backoff window elapses. */
    PUBLISH_FAILED
}
