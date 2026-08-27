package com.eventforge.events.envelope;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/**
 * Shared Jackson configuration for {@link EventEnvelope} (de)serialization across every service.
 *
 * <p>Unknown properties are ignored on deserialization so that an older consumer, built against
 * an earlier schema version, can still read an event published under a newer, additively-changed
 * schema (see ADR-0002).
 */
public final class EventEnvelopeMapper {

    private EventEnvelopeMapper() {}

    public static ObjectMapper create() {
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                .configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
                .build();
    }
}
