package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.outbox.OutboxRelayWorker;
import com.eventforge.events.outbox.RelayOutcome;
import com.eventforge.order.api.CreateOrderRequest;
import com.eventforge.order.api.OrderResponse;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.eventforge.testing.tracing.TracingTestConfiguration;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Collections;
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
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

/**
 * Constitution M4 item 8: confirms a sampling decision made at the HTTP entry point survives the
 * outbox write and the relay publish, not just that the number 1.0 is configured somewhere. Two
 * inbound {@code traceparent} headers, identical except for the trailing sampled-flag byte
 * (constitution's own framing: "a dropped sampling flag that silently breaks downstream traces is
 * a real bug"): the stored outbox row's {@code traceparent} and the relayed Kafka header's
 * {@code traceparent} must both preserve whichever flag came in, byte for byte, and only the
 * sampled trace produces an exported relay span — see ADR-0019.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import(TracingTestConfiguration.class)
class SamplingPropagationIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.poll-interval-ms", () -> "3600000");
        registry.add("eventforge.tracing.enabled", () -> "false");
        // ParentBased: the actual mechanism under test. A ratio below 1.0 would make an
        // independently-decided root span sometimes sampled anyway, muddying the assertion that
        // it's the PROPAGATED flag being honored, not chance.
        registry.add("eventforge.tracing.sampling-probability", () -> "0.5");
    }

    @Autowired
    private MockMvc mockMvc;

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
    void aNotSampledInboundFlagSurvivesTheOutboxAndTheRelayAndProducesNoExportedSpans() throws Exception {
        String notSampledTraceparent = "00-11112222333344445555666677778888-1111222233334444-00";

        String orderId = createOrderWithInboundTraceparent(notSampledTraceparent);
        drainAllPending();

        String storedTraceparent = jdbcTemplate.queryForObject(
                "SELECT traceparent FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'",
                String.class,
                orderId);
        assertThat(storedTraceparent).startsWith("00-11112222333344445555666677778888-").endsWith("-00");

        ConsumerRecord<String, String> record = consumeOneByKey("orders.events", orderId, Duration.ofSeconds(20));
        String headerTraceparent = headerValue(record, "traceparent");
        assertThat(headerTraceparent).startsWith("00-11112222333344445555666677778888-").endsWith("-00");

        // ParentBased(...) honors the inherited NOT-sampled decision - no span for this trace was
        // ever recorded, regardless of the 0.5 ratio a fresh root decision would have used.
        assertThat(spanExporter.getFinishedSpanItems())
                .as("a not-sampled parent must produce zero exported spans downstream")
                .isEmpty();
    }

    @Test
    void aSampledInboundFlagSurvivesAndProducesRealExportedSpans() throws Exception {
        String sampledTraceparent = "00-aaaa2222333344445555666677778888-aaaa222233334444-01";

        String orderId = createOrderWithInboundTraceparent(sampledTraceparent);
        drainAllPending();

        String storedTraceparent = jdbcTemplate.queryForObject(
                "SELECT traceparent FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'",
                String.class,
                orderId);
        assertThat(storedTraceparent).startsWith("00-aaaa2222333344445555666677778888-").endsWith("-01");

        ConsumerRecord<String, String> record = consumeOneByKey("orders.events", orderId, Duration.ofSeconds(20));
        String headerTraceparent = headerValue(record, "traceparent");
        assertThat(headerTraceparent).startsWith("00-aaaa2222333344445555666677778888-").endsWith("-01");

        assertThat(spanExporter.getFinishedSpanItems())
                .as("a sampled parent must produce real exported spans (the HTTP span and at least the relay's own)")
                .isNotEmpty();
        assertThat(spanExporter.getFinishedSpanItems())
                .allSatisfy(span -> assertThat(span.getTraceId()).isEqualTo("aaaa2222333344445555666677778888"));
    }

    private String createOrderWithInboundTraceparent(String traceparent) throws Exception {
        String responseJson = mockMvc.perform(MockMvcRequestBuilders.post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .header("traceparent", traceparent)
                        .content(mapper.writeValueAsString(new CreateOrderRequest(1500, null, null))))
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString();
        return mapper.readValue(responseJson, OrderResponse.class).orderId().toString();
    }

    private void drainAllPending() {
        int guard = 0;
        while (relayWorker.relayNextEvent() != RelayOutcome.NOTHING_TO_CLAIM && guard++ < 500) {
            // keep draining
        }
    }

    private ConsumerRecord<String, String> consumeOneByKey(String topic, String key, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "sampling-test-" + UUID.randomUUID());
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
