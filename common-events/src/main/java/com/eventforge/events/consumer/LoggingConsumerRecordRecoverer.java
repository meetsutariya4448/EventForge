package com.eventforge.events.consumer;

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;

/**
 * What an operator observes once a record exhausts its bounded retries (constitution item 5, and
 * item 4e's "poison message" requirement): an ERROR-level log line naming the exact
 * topic/partition/offset/key, and the partition keeps moving — this recoverer's whole job is to
 * let {@link org.springframework.kafka.listener.DefaultErrorHandler} commit past the bad record
 * instead of retrying it forever. No dead-letter topic, no DLQ (that's M5's design, not this
 * milestone's) — just visible, bounded failure.
 *
 * <p>The counter exists so tests can assert this actually ran without scraping log output.
 */
public class LoggingConsumerRecordRecoverer implements ConsumerRecordRecoverer {

    private static final Logger log = LoggerFactory.getLogger(LoggingConsumerRecordRecoverer.class);

    private final AtomicInteger recoveredCount = new AtomicInteger();

    @Override
    public void accept(ConsumerRecord<?, ?> record, Exception exception) {
        recoveredCount.incrementAndGet();
        log.error(
                "Skipping poison record after exhausting retries: topic={} partition={} offset={} key={}",
                record.topic(),
                record.partition(),
                record.offset(),
                record.key(),
                exception);
    }

    public int recoveredCount() {
        return recoveredCount.get();
    }
}
