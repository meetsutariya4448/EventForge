package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

/**
 * The item-7 deliverable: boots real Postgres and real Kafka (via Testcontainers) and round-trips
 * a single {@link EventEnvelope} end to end. This is schema and transport validation only — no
 * relay, no claiming, no dedupe logic is exercised (those don't exist until later milestones).
 */
@SpringBootTest
class EnvelopeRoundTripIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final String TEST_TOPIC = "envelope-round-trip-test";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void publishesAndConsumesAnEnvelopeWithHeaderPropagatedTraceContext() throws Exception {
        ObjectNode payload = mapper.createObjectNode().put("note", "M0 round-trip fixture");
        EventEnvelope original = new EventEnvelope(
                UUID.randomUUID(),
                "TestFixtureEvent",
                1,
                "order-round-trip-1",
                UUID.randomUUID(),
                null,
                Instant.now(),
                payload);

        // W3C trace context travels as Kafka headers, never inside the payload (ADR-0005).
        String traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        String tracestate = "eventforge=m0-roundtrip";

        Message<String> message = MessageBuilder.withPayload(mapper.writeValueAsString(original))
                .setHeader(KafkaHeaders.TOPIC, TEST_TOPIC)
                .setHeader(KafkaHeaders.KEY, original.aggregateId())
                .setHeader("traceparent", traceparent)
                .setHeader("tracestate", tracestate)
                .build();

        kafkaTemplate.send(message).get(10, TimeUnit.SECONDS);

        ConsumerRecord<String, String> record = consumeOneRecord();

        EventEnvelope roundTripped = mapper.readValue(record.value(), EventEnvelope.class);
        assertThat(roundTripped).isEqualTo(original);
        assertThat(headerValue(record, "traceparent")).isEqualTo(traceparent);
        assertThat(headerValue(record, "tracestate")).isEqualTo(tracestate);
    }

    @Test
    void outboxSchemaAcceptsAFullyPopulatedRowIncludingTraceAndSequenceColumns() {
        UUID eventId = UUID.randomUUID();
        String traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

        jdbcTemplate.update(
                """
                INSERT INTO outbox_events (
                    event_id, aggregate_type, aggregate_id, aggregate_sequence,
                    event_type, schema_version, correlation_id, causation_id,
                    traceparent, tracestate, payload, occurred_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
                """,
                eventId,
                "Order",
                "order-round-trip-1",
                1L,
                "TestFixtureEvent",
                1,
                UUID.randomUUID(),
                null,
                traceparent,
                "eventforge=m0-roundtrip",
                "{\"note\":\"schema validation row\"}",
                Timestamp.from(Instant.now()));

        Map<String, Object> row = jdbcTemplate.queryForMap(
                "SELECT event_id, traceparent, aggregate_sequence, published_at FROM outbox_events WHERE event_id = ?",
                eventId);

        assertThat(row.get("event_id")).isEqualTo(eventId);
        assertThat(row.get("traceparent")).isEqualTo(traceparent);
        assertThat(((Number) row.get("aggregate_sequence")).longValue()).isEqualTo(1L);
        assertThat(row.get("published_at")).isNull();
    }

    private ConsumerRecord<String, String> consumeOneRecord() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "round-trip-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TEST_TOPIC));
            ConsumerRecords<String, String> records = ConsumerRecords.empty();
            long deadline = System.currentTimeMillis() + Duration.ofSeconds(15).toMillis();
            while (records.isEmpty() && System.currentTimeMillis() < deadline) {
                records = consumer.poll(Duration.ofSeconds(2));
            }
            if (records.isEmpty()) {
                throw new IllegalStateException("No record received on topic " + TEST_TOPIC + " within timeout");
            }
            return records.iterator().next();
        }
    }

    private static String headerValue(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
