package com.eventforge.events.envelope;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EventEnvelopeSerializationTest {

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void roundTripsThroughJsonPreservingAllFields() throws Exception {
        ObjectNode payload = mapper.createObjectNode().put("orderId", "order-123").put("amount", 4999);
        EventEnvelope original = new EventEnvelope(
                UUID.randomUUID(),
                "OrderCreated",
                1,
                "order-123",
                UUID.randomUUID(),
                UUID.randomUUID(),
                Instant.parse("2026-08-26T12:00:00Z"),
                payload);

        String json = mapper.writeValueAsString(original);
        EventEnvelope roundTripped = mapper.readValue(json, EventEnvelope.class);

        assertThat(roundTripped).isEqualTo(original);
    }

    @Test
    void causationIdMayBeNullForRootEvents() throws Exception {
        ObjectNode payload = mapper.createObjectNode().put("orderId", "order-123");
        EventEnvelope original = new EventEnvelope(
                UUID.randomUUID(),
                "OrderCreated",
                1,
                "order-123",
                UUID.randomUUID(),
                null,
                Instant.parse("2026-08-26T12:00:00Z"),
                payload);

        String json = mapper.writeValueAsString(original);
        EventEnvelope roundTripped = mapper.readValue(json, EventEnvelope.class);

        assertThat(roundTripped.causationId()).isNull();
        assertThat(roundTripped).isEqualTo(original);
    }

    @Test
    void rejectsSchemaVersionBelowOne() {
        ObjectNode payload = mapper.createObjectNode();
        org.junit.jupiter.api.Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new EventEnvelope(
                        UUID.randomUUID(),
                        "OrderCreated",
                        0,
                        "order-123",
                        UUID.randomUUID(),
                        null,
                        Instant.now(),
                        payload));
    }
}
