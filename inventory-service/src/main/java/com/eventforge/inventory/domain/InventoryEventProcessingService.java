package com.eventforge.inventory.domain;

import com.eventforge.events.consumer.ConsumerOutcome;
import com.eventforge.events.consumer.ProcessedEventStore;
import com.eventforge.events.envelope.EventEnvelope;
import java.time.Instant;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Same dedupe -> business mutation shape as {@code PaymentAuthorizationService}, deliberately
 * minimal on the mutation side (constitution scope for M2: "reservation semantics are M3"). No
 * next outbox event — there's nothing meaningful to say about inventory yet.
 */
@Service
public class InventoryEventProcessingService {

    public static final String CONSUMER_GROUP = "inventory-service";

    private final ProcessedEventStore processedEventStore;
    private final InventoryOrderEventRepository repository;

    public InventoryEventProcessingService(ProcessedEventStore processedEventStore, InventoryOrderEventRepository repository) {
        this.processedEventStore = processedEventStore;
        this.repository = repository;
    }

    @Transactional
    public ConsumerOutcome handleOrderCreated(EventEnvelope envelope) {
        boolean isNew = processedEventStore.tryMarkProcessed(CONSUMER_GROUP, envelope.eventId(), envelope.aggregateId());
        if (!isNew) {
            return ConsumerOutcome.DUPLICATE;
        }

        repository.save(new InventoryOrderEvent(UUID.randomUUID(), envelope.aggregateId(), Instant.now()));

        return ConsumerOutcome.PROCESSED;
    }
}
