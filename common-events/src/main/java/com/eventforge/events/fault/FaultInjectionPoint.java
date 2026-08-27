package com.eventforge.events.fault;

/**
 * Named seams where later milestones need to crash a process at a precise point to test
 * transactional/delivery guarantees. Additive-only: new points may be appended without breaking
 * existing call sites.
 */
public enum FaultInjectionPoint {
    /** After the business+outbox DB transaction commits, before the relay publishes to Kafka. */
    AFTER_DB_COMMIT_BEFORE_KAFKA_PUBLISH,

    /** After the relay publishes to Kafka, before it marks the outbox row published. */
    AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED,

    /** After a consumer's business transaction commits, before it acknowledges the Kafka offset. */
    AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK
}
