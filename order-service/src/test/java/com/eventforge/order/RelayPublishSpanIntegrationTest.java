package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.outbox.OutboxRelayWorker;
import com.eventforge.events.outbox.RelayOutcome;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.eventforge.testing.tracing.TracingTestConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Constitution M4 item 3, isolated and fast: proves the relay's specific extract -> child span ->
 * inject behavior directly, without needing the full cross-service chain
 * {@code TraceContinuityIntegrationTest} (e2e-tests module) exercises. A KNOWN parent context is
 * planted directly into an outbox row's stored {@code traceparent} (standing in for "captured at
 * write time" — exactly what a real write does, just without needing a real HTTP span to produce
 * it), the relay publishes it for real, and the exported span plus the outgoing Kafka record's own
 * headers are both checked against that known parent.
 */
@SpringBootTest
@Import(TracingTestConfiguration.class)
class RelayPublishSpanIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        // TracingTestConfiguration's own Javadoc requirement: avoid a second, real SDK instance
        // pointlessly trying to export in the background alongside the in-memory one.
        registry.add("eventforge.tracing.enabled", () -> "false");
    }

    @Autowired
    private OutboxRelayWorker relayWorker;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private InMemorySpanExporter spanExporter;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @BeforeEach
    void resetSpans() {
        spanExporter.reset();
    }

    @Test
    void relayStartsAChildSpanUnderTheStoredParentAndInjectsThatChildIntoKafkaHeaders() throws Exception {
        String orderId = "relay-span-" + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        // A deliberately well-formed, KNOWN W3C traceparent standing in for "whatever the active
        // span was at write time" - real production code produces this via
        // EventForgeTracer.captureCurrentContext(), not hand-built like this; this test only needs
        // a KNOWN value to assert against, not to prove capture itself (OrderService's own tests,
        // and TraceContinuityIntegrationTest, cover that end).
        String knownParentTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
        insertSyntheticRow(orderId, eventId, knownParentTraceparent);

        RelayOutcome outcome = relayWorker.relayNextEvent();
        assertThat(outcome).isEqualTo(RelayOutcome.PUBLISHED);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(1);
        SpanData relaySpan = spans.get(0);

        // The relay's span is a CHILD of the known parent - same trace ID, and its own parent
        // span ID is exactly the parent's span ID (00f067aa0ba902b7), extracted from the stored
        // context, not fabricated and not "no parent."
        assertThat(relaySpan.getTraceId()).isEqualTo("4bf92f3577b34da6a3ce929d0e0e4736");
        assertThat(relaySpan.getParentSpanId()).isEqualTo("00f067aa0ba902b7");
        // And genuinely a DIFFERENT span ID than the parent's - proving this is real injection of
        // the CHILD's own new context, not a verbatim copy of the parent's traceparent string
        // (which would make the relay invisible in the trace - exactly the bug constitution item 3
        // warns about).
        assertThat(relaySpan.getSpanId()).isNotEqualTo("00f067aa0ba902b7");

        ConsumerRecord<String, String> record = consumeOneByKey("orders.events", orderId, java.time.Duration.ofSeconds(20));
        String headerTraceparent = headerValue(record, "traceparent");
        assertThat(headerTraceparent).startsWith("00-4bf92f3577b34da6a3ce929d0e0e4736-");
        // The Kafka header carries the RELAY's span ID (the child that was just injected), not the
        // original parent's - the literal proof that this isn't a verbatim header copy.
        assertThat(headerTraceparent).contains("-" + relaySpan.getSpanId() + "-");
        assertThat(headerTraceparent).doesNotContain("00f067aa0ba902b7");

        // Sanity check on the extraction itself, via the real W3C propagator rather than manual
        // string slicing - the header really does encode the relay span as a valid child context.
        Map<String, String> carrier = new HashMap<>();
        carrier.put("traceparent", headerTraceparent);
        Context extracted = W3CTraceContextPropagator.getInstance()
                .extract(Context.root(), carrier, new TextMapGetter<Map<String, String>>() {
                    @Override
                    public Iterable<String> keys(Map<String, String> c) {
                        return c.keySet();
                    }

                    @Override
                    public String get(Map<String, String> c, String key) {
                        return c == null ? null : c.get(key);
                    }
                });
        assertThat(io.opentelemetry.api.trace.Span.fromContext(extracted).getSpanContext().getSpanId())
                .isEqualTo(relaySpan.getSpanId());
    }

    private void insertSyntheticRow(String orderId, UUID eventId, String traceparent) {
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
                orderId,
                1L,
                "OrderCreated",
                1,
                eventId,
                null,
                traceparent,
                null,
                "{\"orderId\":\"" + orderId + "\",\"amountCents\":1000}",
                Timestamp.from(Instant.now()));
    }

    private ConsumerRecord<String, String> consumeOneByKey(String topic, String key, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "relay-span-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(topic));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    if (key.equals(record.key())) {
                        return record;
                    }
                }
            }
        }
        throw new AssertionError("No record with key " + key + " found on topic " + topic + " within timeout");
    }

    private static String headerValue(ConsumerRecord<String, String> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
