package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.outbox.OutboxRelayWorker;
import com.eventforge.events.outbox.RelayOutcome;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.eventforge.testing.tracing.TracingTestConfiguration;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
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
 * Constitution M4 item 5: "a 30-second delay between the outbox write and the relay publish should
 * show as a gap in one trace, not two traces." The background relay scheduler is disabled here (the
 * established pattern every relay test in this project uses) so the test itself controls exactly
 * when the relay runs — the delay between the write and the (explicitly, later, deliberately
 * delayed) relay call is real elapsed wall-clock time, not simulated.
 */
@SpringBootTest
@Import(TracingTestConfiguration.class)
class RelayAsyncGapTraceIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    // Real, not simulated — deliberately shorter than the constitution's illustrative "30 seconds"
    // to keep this test's own runtime reasonable; the mechanism proven (a real elapsed gap doesn't
    // split the trace) generalizes to any duration, since nothing about span parent-child linkage
    // depends on how much real time passed between the two spans.
    private static final Duration CONFIGURABLE_GAP = Duration.ofSeconds(5);

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.poll-interval-ms", () -> "3600000");
        registry.add("eventforge.tracing.enabled", () -> "false");
    }

    @Autowired
    private OutboxRelayWorker relayWorker;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private InMemorySpanExporter spanExporter;

    @BeforeEach
    void resetSpans() {
        spanExporter.reset();
    }

    @Test
    void aRealMultiSecondGapBetweenWriteAndPublishStaysOneTraceNotTwo() throws Exception {
        String orderId = "async-gap-" + UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        String knownTraceparent = "00-99998888777766665555444433332222-9999888877776666-01";
        Instant writeTime = Instant.now();
        insertSyntheticRow(orderId, eventId, knownTraceparent, writeTime);

        // The real, deliberate gap. Nothing simulated - actual elapsed wall-clock time between the
        // "write" (already durably committed above) and the relay publishing it.
        TimeUnit.MILLISECONDS.sleep(CONFIGURABLE_GAP.toMillis());

        Instant beforePublish = Instant.now();
        RelayOutcome outcome = relayWorker.relayNextEvent();
        assertThat(outcome).isEqualTo(RelayOutcome.PUBLISHED);

        List<SpanData> spans = spanExporter.getFinishedSpanItems();
        assertThat(spans).hasSize(1);
        SpanData relaySpan = spans.get(0);

        // One trace, not two: the relay span belongs to the SAME trace ID the known context
        // carried, and is a direct child of it - the gap did not cause a fresh trace to start.
        assertThat(relaySpan.getTraceId()).isEqualTo("99998888777766665555444433332222");
        assertThat(relaySpan.getParentSpanId()).isEqualTo("9999888877776666");

        // The gap is real and visible IN the trace, not papered over: the relay span's own start
        // time is at least the configured gap after the original write, not immediately after it.
        Instant relaySpanStart = Instant.ofEpochSecond(0, relaySpan.getStartEpochNanos());
        assertThat(relaySpanStart).isAfterOrEqualTo(writeTime.plus(CONFIGURABLE_GAP).minusMillis(500));
        assertThat(relaySpanStart).isAfterOrEqualTo(beforePublish.minusSeconds(1));

        ConsumerRecord<String, String> record = consumeOneByKey("orders.events", orderId, Duration.ofSeconds(20));
        String headerTraceparent = headerValue(record, "traceparent");
        assertThat(headerTraceparent).startsWith("00-99998888777766665555444433332222-");
    }

    private void insertSyntheticRow(String orderId, UUID eventId, String traceparent, Instant occurredAt) {
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
                Timestamp.from(occurredAt));
    }

    private ConsumerRecord<String, String> consumeOneByKey(String topic, String key, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "async-gap-test-" + UUID.randomUUID());
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
