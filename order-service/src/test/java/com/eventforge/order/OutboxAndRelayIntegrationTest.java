package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.events.outbox.OutboxRelayWorker;
import com.eventforge.events.outbox.RelayOutcome;
import com.eventforge.events.trace.TraceContextCapture;
import com.eventforge.order.api.CreateOrderRequest;
import com.eventforge.order.api.OrderResponse;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.eventforge.testing.fault.ConfigurableFaultInjector;
import com.eventforge.testing.fault.FaultInjectionTestConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

/**
 * The M1 deliverable: proves the outbox writer's atomic dual write, the relay's claim-publish-mark
 * cycle (including trace-context restoration into Kafka headers), and that the fault-injection
 * seam built in M0 is now wired to a real call site.
 *
 * <p>Every relay interaction here calls {@link OutboxRelayWorker#relayNextEvent()} directly and
 * synchronously — never through the background {@code @Scheduled} poller — so these tests are
 * fully deterministic with no {@code Thread.sleep}/wall-clock polling for the relay's own state
 * changes. The one unavoidable bounded wait is consuming from the real Kafka broker itself, which
 * is genuinely asynchronous I/O.
 *
 * <p>That "never through the poller" claim used to be an assumption this class didn't actually
 * enforce — the background scheduler was left running, racing these tests' own explicit calls on
 * whichever row happened to be freshly written and unpublished. {@code scheduler-enabled=false}
 * below is what makes the Javadoc above true rather than aspirational.
 */
@SpringBootTest
@AutoConfigureMockMvc
@Import(FaultInjectionTestConfiguration.class)
class OutboxAndRelayIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private OutboxRelayWorker relayWorker;

    @Autowired
    private FaultInjector faultInjector;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @AfterEach
    void clearArmedFaults() {
        ((ConfigurableFaultInjector) faultInjector).clear();
    }

    @Test
    void createOrderWritesOrderAndOutboxRowInOneTransaction() throws Exception {
        UUID orderId = createOrder(3999, null, null);

        Map<String, Object> orderRow =
                jdbcTemplate.queryForMap("SELECT status, amount_cents FROM orders WHERE order_id = ?", orderId);
        assertThat(orderRow.get("status")).isEqualTo("PENDING");
        assertThat(((Number) orderRow.get("amount_cents")).longValue()).isEqualTo(3999L);

        Map<String, Object> outboxRow = jdbcTemplate.queryForMap(
                "SELECT event_type, traceparent, published_at FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'",
                orderId.toString());
        assertThat(outboxRow.get("event_type")).isEqualTo("OrderCreated");
        assertThat(TraceContextCapture.isValid((String) outboxRow.get("traceparent"))).isTrue();
        assertThat(outboxRow.get("published_at")).isNull();
    }

    @Test
    void continuesAnInboundTraceparentInsteadOfStartingANewTrace() throws Exception {
        String inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

        UUID orderId = createOrder(1000, inbound, "eventforge=upstream");

        String storedTraceparent = jdbcTemplate.queryForObject(
                "SELECT traceparent FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'", String.class, orderId.toString());
        assertThat(storedTraceparent).startsWith("00-4bf92f3577b34da6a3ce929d0e0e4736-");
        assertThat(storedTraceparent).doesNotContain("00f067aa0ba902b7");

        String storedTracestate = jdbcTemplate.queryForObject(
                "SELECT tracestate FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'", String.class, orderId.toString());
        assertThat(storedTracestate).isEqualTo("eventforge=upstream");
    }

    @Test
    void relayPublishesClaimedEventToKafkaWithRestoredHeadersAndMarksItPublished() throws Exception {
        drainAllPending();
        UUID orderId = createOrder(2500, null, null);

        RelayOutcome outcome = relayWorker.relayNextEvent();
        assertThat(outcome).isEqualTo(RelayOutcome.PUBLISHED);

        Timestamp publishedAt = jdbcTemplate.queryForObject(
                "SELECT published_at FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'", Timestamp.class, orderId.toString());
        assertThat(publishedAt).isNotNull();

        ConsumerRecord<String, String> record = consumeOneByKey("orders.events", orderId.toString(), Duration.ofSeconds(20));
        EventEnvelope envelope = mapper.readValue(record.value(), EventEnvelope.class);
        assertThat(envelope.eventType()).isEqualTo("OrderCreated");
        assertThat(envelope.aggregateId()).isEqualTo(orderId.toString());
        assertThat(TraceContextCapture.isValid(headerValue(record, "traceparent"))).isTrue();
    }

    @Test
    void aCrashAfterKafkaPublishBeforeMarkPublishedLeavesTheRowUnpublished() throws Exception {
        drainAllPending();
        UUID orderId = createOrder(500, null, null);

        ((ConfigurableFaultInjector) faultInjector)
                .registerAction(FaultInjectionPoint.AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED, () -> {
                    throw new IllegalStateException("simulated crash after publish, before mark-published");
                });

        // The fault fires after the real publish succeeds, outside the try/catch that wraps
        // genuine recoverable publish failures — so it propagates as-is and rolls back the whole
        // transaction. See OutboxRelayCrashWindowIntegrationTest for the fuller version of this
        // scenario (asserting the resulting duplicate on Kafka after "restart").
        assertThatThrownBy(() -> relayWorker.relayNextEvent())
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("simulated crash");

        Timestamp publishedAt = jdbcTemplate.queryForObject(
                "SELECT published_at FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'", Timestamp.class, orderId.toString());
        assertThat(publishedAt).isNull();
    }

    private UUID createOrder(long amountCents, String traceparent, String tracestate) throws Exception {
        // Real credentials against the real filter chain, not mocked authentication: v2 made
        // POST /orders an OPERATOR-only mutation, and what CI should exercise is the chain that
        // actually ships.
        MockHttpServletRequestBuilder requestBuilder = MockMvcRequestBuilders.post("/orders")
                .with(SecurityMockMvcRequestPostProcessors.httpBasic("operator", "operator"))
                .contentType(MediaType.APPLICATION_JSON)
                .content(mapper.writeValueAsString(new CreateOrderRequest(amountCents, null, null)));
        if (traceparent != null) {
            requestBuilder = requestBuilder.header("traceparent", traceparent);
        }
        if (tracestate != null) {
            requestBuilder = requestBuilder.header("tracestate", tracestate);
        }

        String responseJson = mockMvc.perform(requestBuilder)
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString();

        return mapper.readValue(responseJson, OrderResponse.class).orderId();
    }

    private void drainAllPending() {
        int guard = 0;
        while (relayWorker.relayNextEvent() != RelayOutcome.NOTHING_TO_CLAIM && guard++ < 200) {
            // keep draining leftover unpublished rows from other tests in this shared container
        }
    }

    private ConsumerRecord<String, String> consumeOneByKey(String topic, String key, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-relay-test-" + UUID.randomUUID());
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
