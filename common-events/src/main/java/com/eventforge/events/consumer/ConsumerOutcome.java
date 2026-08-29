package com.eventforge.events.consumer;

/** The result of one attempt to process a consumed event through {@link ProcessedEventStore}. */
public enum ConsumerOutcome {
    /** First time this (consumer_group, event_id) pair has been seen — the business mutation ran. */
    PROCESSED,

    /** Already processed by this consumer group. A clean no-op — not an exception, not a silent
     * swallow: the caller observes this outcome explicitly and still acknowledges the offset. */
    DUPLICATE
}
