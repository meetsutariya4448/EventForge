package com.eventforge.events.envelope;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

/**
 * Proves the schema-evolution rule from ADR-0002: a consumer built against today's
 * {@link EventEnvelope} can still deserialize an event carrying fields it doesn't know about yet,
 * as long as the change is additive. This stands in for "an old consumer reads a new event"
 * without needing two separately versioned classes or a Schema Registry.
 */
class EventEnvelopeCompatibilityTest {

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void ignoresFieldsAddedByANewerSchemaVersion() throws Exception {
        String jsonFromAFutureSchemaVersion =
                """
                {
                  "eventId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                  "eventType": "OrderCreated",
                  "schemaVersion": 2,
                  "aggregateId": "order-123",
                  "correlationId": "3fa85f64-5717-4562-b3fc-2c963f66afa7",
                  "causationId": null,
                  "occurredAt": "2026-08-26T12:00:00Z",
                  "payload": { "orderId": "order-123" },
                  "shippingPriority": "EXPRESS"
                }
                """;

        EventEnvelope deserialized = mapper.readValue(jsonFromAFutureSchemaVersion, EventEnvelope.class);

        assertThat(deserialized.eventId().toString()).isEqualTo("3fa85f64-5717-4562-b3fc-2c963f66afa6");
        assertThat(deserialized.schemaVersion()).isEqualTo(2);
        assertThat(deserialized.aggregateId()).isEqualTo("order-123");
    }
}
