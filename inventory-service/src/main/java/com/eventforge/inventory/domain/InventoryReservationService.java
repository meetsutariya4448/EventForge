package com.eventforge.inventory.domain;

import com.eventforge.events.consumer.ConsumerOutcome;
import com.eventforge.events.consumer.ProcessedEventStore;
import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.outbox.OutboxEventRow;
import com.eventforge.events.outbox.OutboxWriter;
import com.eventforge.events.trace.TraceContextCapture;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Handles the orchestrator's ReserveInventory command with real stock arithmetic (constitution
 * item 1: reservation semantics land in M3, superseding M2's minimal "just record we saw it"
 * stub). Same consumer-transaction shape as every other participant: dedupe check, business
 * mutation, next outbox write, one transaction (constitution Part 2).
 *
 * <p>A real, row-locked quantity check (not a forced-failure test hook) is what makes both
 * constitution item 8b ("forced inventory failure") and item 8f ("two sagas touch the same
 * inventory item") provable with the same mechanism: seed a SKU with less stock than requested,
 * or than two concurrent orders request together, and the failure/contention is genuine.
 */
@Service
public class InventoryReservationService {

    public static final String CONSUMER_GROUP = "inventory-service";

    private final ProcessedEventStore processedEventStore;
    private final InventoryItemRepository inventoryItemRepository;
    private final InventoryReservationRepository inventoryReservationRepository;
    private final OutboxWriter outboxWriter;

    // Same reasoning as every other service (ADR-0009).
    private final ObjectMapper objectMapper = EventEnvelopeMapper.create();

    public InventoryReservationService(
            ProcessedEventStore processedEventStore,
            InventoryItemRepository inventoryItemRepository,
            InventoryReservationRepository inventoryReservationRepository,
            OutboxWriter outboxWriter) {
        this.processedEventStore = processedEventStore;
        this.inventoryItemRepository = inventoryItemRepository;
        this.inventoryReservationRepository = inventoryReservationRepository;
        this.outboxWriter = outboxWriter;
    }

    @Transactional
    public ConsumerOutcome handleReserveInventory(EventEnvelope envelope, String inboundTraceparent, String inboundTracestate) {
        boolean isNew = processedEventStore.tryMarkProcessed(CONSUMER_GROUP, envelope.eventId(), envelope.aggregateId());
        if (!isNew) {
            return ConsumerOutcome.DUPLICATE;
        }

        String orderId = envelope.aggregateId();
        String sku = envelope.payload().get("sku").asText();
        long quantity = envelope.payload().get("quantity").asLong();
        Instant now = Instant.now();
        String traceparent = TraceContextCapture.continueOrStart(inboundTraceparent);

        Optional<InventoryItem> maybeItem = inventoryItemRepository.findWithLockBySku(sku);
        if (maybeItem.isEmpty()) {
            writeInventoryReservationFailed(orderId, envelope, traceparent, inboundTracestate, "unknown sku " + sku, now);
            return ConsumerOutcome.PROCESSED;
        }

        InventoryItem item = maybeItem.get();
        if (!item.reserve(quantity)) {
            writeInventoryReservationFailed(
                    orderId,
                    envelope,
                    traceparent,
                    inboundTracestate,
                    "insufficient stock for " + sku + ": requested " + quantity + ", available " + item.getAvailableQuantity(),
                    now);
            return ConsumerOutcome.PROCESSED;
        }
        inventoryItemRepository.save(item);

        UUID reservationId = UUID.randomUUID();
        inventoryReservationRepository.save(new InventoryReservation(reservationId, orderId, sku, quantity, "RESERVED", now));

        UUID nextEventId = UUID.randomUUID();
        Map<String, Object> payload =
                Map.of("orderId", orderId, "reservationId", reservationId.toString(), "sku", sku, "quantity", quantity);
        outboxWriter.write(new OutboxEventRow(
                nextEventId,
                "Inventory",
                orderId,
                1L,
                "InventoryReserved",
                1,
                envelope.correlationId(),
                envelope.eventId(),
                traceparent,
                inboundTracestate,
                writeJson(payload),
                now));

        return ConsumerOutcome.PROCESSED;
    }

    private void writeInventoryReservationFailed(
            String orderId, EventEnvelope envelope, String traceparent, String inboundTracestate, String reason, Instant now) {
        UUID nextEventId = UUID.randomUUID();
        Map<String, Object> payload = Map.of("orderId", orderId, "reason", reason);
        outboxWriter.write(new OutboxEventRow(
                nextEventId,
                "Inventory",
                orderId,
                1L,
                "InventoryReservationFailed",
                1,
                envelope.correlationId(),
                envelope.eventId(),
                traceparent,
                inboundTracestate,
                writeJson(payload),
                now));
    }

    private String writeJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize inventory event payload", e);
        }
    }
}
