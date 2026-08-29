package com.eventforge.notification.domain;

import com.eventforge.events.consumer.ConsumerOutcome;
import com.eventforge.events.consumer.ProcessedEventStore;
import com.eventforge.events.envelope.EventEnvelope;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Same dedupe -> business mutation shape as the other consumers, but the mutation here stands in
 * for an EXTERNAL effect (see {@link SentNotification}'s Javadoc) — this is deliberately the
 * service item 6 uses to demonstrate independent consumer-group offsets, and its business effect
 * is the one in this milestone where a dedupe gap would actually matter to a real customer, not
 * just to this database.
 */
@Service
public class NotificationSendingService {

    public static final String CONSUMER_GROUP = "notification-service";

    private final ProcessedEventStore processedEventStore;
    private final SentNotificationRepository repository;

    public NotificationSendingService(ProcessedEventStore processedEventStore, SentNotificationRepository repository) {
        this.processedEventStore = processedEventStore;
        this.repository = repository;
    }

    @Transactional
    public ConsumerOutcome handleOrderCreated(EventEnvelope envelope) {
        boolean isNew = processedEventStore.tryMarkProcessed(CONSUMER_GROUP, envelope.eventId(), envelope.aggregateId());
        if (!isNew) {
            return ConsumerOutcome.DUPLICATE;
        }

        // Stand-in for calling an external email/SMS provider - see SentNotification's Javadoc.
        repository.save(new SentNotification(UUID.randomUUID(), envelope.aggregateId(), "EMAIL", Instant.now()));

        return ConsumerOutcome.PROCESSED;
    }
}
