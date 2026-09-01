package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.events.outbox.OutboxRelayWorker;
import com.eventforge.events.outbox.RelayOutcome;
import com.eventforge.order.api.CreateOrderRequest;
import com.eventforge.order.api.OrderResponse;
import com.eventforge.testing.fault.ConfigurableFaultInjector;
import com.eventforge.testing.fault.FaultInjectionTestConfiguration;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The headline M1 proof: exercises the M0 fault-injection harness and the M1 relay against real
 * crash and outage windows, not just the happy path.
 *
 * <p>This class owns its own Postgres and Kafka containers — not the shared ones from
 * {@code AbstractPostgresKafkaIntegrationTest} — specifically so scenario (a)/(c) can stop and
 * restart the broker without any blast radius on other test classes sharing static containers.
 *
 * <p>No test here uses {@code Thread.sleep} or asserts on elapsed wall-clock time. The relay's
 * retry backoff is driven by an injected {@link MutableClock} that tests advance explicitly
 * ({@link #mutableClock}) — "time passing" is a deliberate test action, not something waited out
 * for real. The one bounded wait that remains is consuming from the real Kafka broker, which is
 * genuinely asynchronous I/O; no assertion depends on how long that takes, only on what eventually
 * arrives.
 */
@Testcontainers
@SpringBootTest
@AutoConfigureMockMvc
@Import({FaultInjectionTestConfiguration.class, OutboxRelayCrashWindowIntegrationTest.ClockConfig.class})
class OutboxRelayCrashWindowIntegrationTest {

    // Same digest as docker/docker-compose.yml; see AbstractPostgresKafkaIntegrationTest's comment
    // for why asCompatibleSubstituteFor("postgres") is required, not optional.
    private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse(
                    "postgres:16-alpine@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685")
            .asCompatibleSubstituteFor("postgres");

    // Explicit "postgres" name required — see AbstractPostgresKafkaIntegrationTest's comment:
    // Spring Boot's own @ServiceConnection name deduction throws on a digest-suffixed image name
    // unless told the name explicitly, independent of Testcontainers' own substitution above.
    @Container
    @ServiceConnection("postgres")
    static final PostgreSQLContainer postgres = new PostgreSQLContainer(POSTGRES_IMAGE);

    @Container
    @ServiceConnection
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1");

    private static final Duration RETRY_BACKOFF = Duration.ofSeconds(5);
    private static final String TOPIC = "orders.events";

    @DynamicPropertySource
    static void relayTuningForFastFailure(DynamicPropertyRegistry registry) {
        // Short producer-level timeouts so a broker-down attempt fails in seconds, not the
        // library defaults (which run into tens of seconds to minutes). Still real I/O against a
        // real (paused) broker — just bounded tightly for test speed, not tuned for production.
        registry.add("spring.kafka.producer.properties.request.timeout.ms", () -> "2000");
        registry.add("spring.kafka.producer.properties.delivery.timeout.ms", () -> "3000");
        registry.add("spring.kafka.producer.properties.max.block.ms", () -> "3000");
        registry.add("eventforge.outbox.relay.kafka-send-timeout-ms", () -> "4000");
        registry.add("eventforge.outbox.relay.retry-backoff-ms", () -> String.valueOf(RETRY_BACKOFF.toMillis()));
        // These tests drive the relay exclusively via direct, synchronous relayNextEvent() calls
        // for determinism (no wall-clock dependence), especially during the pauseBroker() windows.
        // The OutboxRelayScheduler bean (the background @Scheduled poller) is genuinely omitted
        // from this context, not just delayed past the test's runtime — no @Scheduled method
        // exists here at all, so there is nothing to race the test thread's own calls. See
        // OutboxRelayAutoConfiguration and OutboxRelaySchedulerEnabledByDefaultTest, which proves
        // this flag defaults to leaving the scheduler on in production.
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        // This class shares its injected MutableClock across the relay's own retry backoff and
        // the saga orchestrator's timeout deadlines (both read the same Clock bean). These tests
        // advance that clock by many seconds at a time to drive relay backoff windows, which would
        // also make any saga created here look "timed out" to a real-wall-clock-scheduled sweep —
        // an unrelated M3 mechanism this M1 test has no business exercising. No equivalent
        // enabled-flag exists for the saga sweep, so this one is still pushed out far past any
        // test's runtime rather than genuinely disabled.
        registry.add("eventforge.saga.sweep-interval-ms", () -> "3600000");
    }

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private OutboxRelayWorker relayWorker;

    @Autowired
    private FaultInjector faultInjector;

    @Autowired
    private MutableClock mutableClock;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    /**
     * Simulates "broker unavailable" by freezing the container (Docker pause/cgroup freezer)
     * rather than stopping it. This was a deliberate correction: an earlier version of this test
     * used {@code kafka.stop()}/{@code kafka.start()}, which discovered a real Testcontainers
     * pitfall rather than a relay bug — Docker can reassign a new host port on container restart,
     * and Spring's {@code KafkaTemplate} bootstrap-servers is captured once at application context
     * startup, so it never recovers on its own even after the container comes back (visible as a
     * permanent "Topic ... not present in metadata" timeout, since the client keeps talking to a
     * port nothing is listening on anymore). Pausing freezes the same container in place — no
     * restart, no new port, no state loss — which both sidesteps that trap and is a more faithful
     * simulation of "broker unresponsive" than a clean stop/start would be anyway.
     */
    private void pauseBroker() {
        kafka.getDockerClient().pauseContainerCmd(kafka.getContainerId()).exec();
    }

    private void unpauseBroker() {
        kafka.getDockerClient().unpauseContainerCmd(kafka.getContainerId()).exec();
    }

    @BeforeEach
    void resetClockAndFaults() {
        // Reset to real "now" at the start of every test rather than carrying forward whatever a
        // previous test advanced it to — keeps each test's backoff window self-contained.
        mutableClock.reset(Instant.now());
        ((ConfigurableFaultInjector) faultInjector).clear();
    }

    // ------------------------------------------------------------------------------------------
    // (a) Broker unavailable: writes keep succeeding, nothing publishes, then zero-loss catch-up.
    // ------------------------------------------------------------------------------------------
    @Test
    void ordersKeepCommittingWhileBrokerIsDownAndAllPublishOnceItRecovers() throws Exception {
        drainAllPending();
        List<UUID> orderIds = new ArrayList<>();

        pauseBroker();
        try {
            // The write path (OrderController -> OrderService) never talks to Kafka directly, so
            // it must be completely unaffected by the broker being down.
            for (int i = 0; i < 3; i++) {
                orderIds.add(createOrder(1000 + i));
            }

            for (int i = 0; i < orderIds.size(); i++) {
                assertThat(relayWorker.relayNextEvent()).isEqualTo(RelayOutcome.PUBLISH_FAILED);
            }
            for (UUID id : orderIds) {
                Map<String, Object> row = jdbcTemplate.queryForMap(
                        "SELECT published_at, publish_attempts FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'",
                        id.toString());
                assertThat(row.get("published_at")).isNull();
                assertThat(((Number) row.get("publish_attempts")).intValue()).isGreaterThanOrEqualTo(1);
            }
        } finally {
            unpauseBroker();
        }

        // Advance past the backoff window on every non-published attempt, not just once: bounded
        // by the guard counter (logical progress), never by elapsed real time. Drains until
        // genuinely nothing is left, rather than stopping at orderIds.size() published rows: each
        // order now has TWO outbox rows (OrderCreated, then AuthorizePayment once OrderCreated
        // publishes — M3's saga dispatch, same transaction as the order write), and which
        // interleaving order they publish in across 3 orders isn't fixed, so counting to a target
        // would risk stopping before every order's OrderCreated specifically has gone out.
        int guard = 0;
        mutableClock.advance(RETRY_BACKOFF.plusSeconds(1));
        RelayOutcome outcome;
        do {
            outcome = relayWorker.relayNextEvent();
            if (outcome != RelayOutcome.PUBLISHED) {
                mutableClock.advance(RETRY_BACKOFF.plusSeconds(1));
            }
        } while (outcome != RelayOutcome.NOTHING_TO_CLAIM && guard++ < 60);
        assertThat(outcome).isEqualTo(RelayOutcome.NOTHING_TO_CLAIM);

        Map<String, Integer> expected =
                orderIds.stream().collect(Collectors.toMap(UUID::toString, id -> 1));
        Map<String, List<ConsumerRecord<String, String>>> byKey =
                consumeGroupedByKey(expected, Duration.ofSeconds(30));
        for (UUID id : orderIds) {
            // Zero LOSS is the guarantee this scenario actually proves — every committed order
            // eventually publishes. Zero duplication is not: calling .get(timeout) on the Kafka
            // send Future gives up WAITING, but does not cancel the underlying send — a "failed"
            // attempt (by our client-side clock) can still land on the broker later, and our own
            // retry then sends a second, independent copy of the same logical event. This was
            // observed directly in this test (both copies carry the same eventId). It's a
            // different mechanism from scenario (b)'s crash-window duplicate, but the same
            // category of outcome, and M2's idempotent consumers absorb this one too.
            assertThat(byKey.getOrDefault(id.toString(), List.of())).isNotEmpty();
        }
    }

    // ------------------------------------------------------------------------------------------
    // (b) Crash after Kafka ack, before commit: the resulting duplicate is the correct outcome.
    // ------------------------------------------------------------------------------------------
    @Test
    void aCrashAfterKafkaAckBeforeCommitRepublishesOnRestartAndTheDuplicateIsCorrect() throws Exception {
        drainAllPending();
        UUID orderId = createOrder(500);

        AtomicBoolean fired = new AtomicBoolean(false);
        ((ConfigurableFaultInjector) faultInjector)
                .registerAction(FaultInjectionPoint.AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED, () -> {
                    if (fired.compareAndSet(false, true)) {
                        throw new IllegalStateException(
                                "simulated crash: Kafka has the message, we never recorded that fact");
                    }
                });

        // First attempt: the broker receives and acks the message, then the process "crashes"
        // right before it would have recorded that fact.
        assertThatThrownBy(() -> relayWorker.relayNextEvent()).isInstanceOf(IllegalStateException.class);

        Timestamp publishedAtAfterCrash = jdbcTemplate.queryForObject(
                "SELECT published_at FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'", Timestamp.class, orderId.toString());
        assertThat(publishedAtAfterCrash).isNull();

        // "Restart": the worker holds no in-memory state between calls (ADR-0010) — everything
        // relevant lives in outbox_events — so calling it again is indistinguishable from a fresh
        // process picking the row back up. Nothing crashes this time, so it publishes for real.
        RelayOutcome secondAttempt = relayWorker.relayNextEvent();
        assertThat(secondAttempt).isEqualTo(RelayOutcome.PUBLISHED);

        // ========================================================================================
        // THIS IS THE POINT OF THE TEST. Kafka now holds TWO copies of the same OrderCreated event
        // for this order: one from the crashed first attempt (which the broker had already
        // received and acked before the simulated crash), and one from the successful retry. This
        // is the CORRECT, EXPECTED outcome of at-least-once delivery under a crash in exactly this
        // window — not a bug — and nothing on the relay's side is supposed to prevent it. M2's
        // idempotent consumers (deduping on event_id via processed_events) exist specifically to
        // collapse this back down to one logical effect on the consuming side. If this assertion
        // ever starts failing because someone "fixed" the relay to not duplicate here, that removes
        // M2's entire reason to exist — don't fix it, revert it.
        // ========================================================================================
        Map<String, List<ConsumerRecord<String, String>>> byKey =
                consumeGroupedByKey(Map.of(orderId.toString(), 2), Duration.ofSeconds(20));
        List<ConsumerRecord<String, String>> records = byKey.get(orderId.toString());
        assertThat(records).hasSize(2);
        List<String> eventIds = records.stream().map(r -> readEventId(r.value())).toList();
        // Same eventId both times: one logical event duplicated, not two different events.
        assertThat(eventIds.get(0)).isEqualTo(eventIds.get(1));
    }

    // ------------------------------------------------------------------------------------------
    // (c) Transient publish failure: bounded retry, real backoff, attempts recorded, no false
    // publish, eventual success once the broker recovers.
    // ------------------------------------------------------------------------------------------
    @Test
    void transientPublishFailureRecordsAttemptsWithBoundedBackoffThenSucceedsOnRecovery() throws Exception {
        drainAllPending();
        UUID orderId = createOrder(750);

        pauseBroker();
        try {
            RelayOutcome first = relayWorker.relayNextEvent();
            assertThat(first).isEqualTo(RelayOutcome.PUBLISH_FAILED);

            Map<String, Object> afterFirst = jdbcTemplate.queryForMap(
                    "SELECT publish_attempts, published_at, last_error FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'",
                    orderId.toString());
            assertThat(((Number) afterFirst.get("publish_attempts")).intValue()).isEqualTo(1);
            assertThat(afterFirst.get("published_at")).isNull();
            assertThat(afterFirst.get("last_error")).isNotNull();

            // Immediately retrying, with the clock unchanged, must NOT attempt again — the row
            // isn't eligible until retryBackoff has elapsed (per the clock, not real time). This is
            // the "bounded" half of "bounded retry with backoff": exactly one publish attempt
            // happened above, and none here, even though nothing stops us from calling again.
            RelayOutcome immediateRetry = relayWorker.relayNextEvent();
            assertThat(immediateRetry).isEqualTo(RelayOutcome.NOTHING_TO_CLAIM);

            Map<String, Object> stillOne = jdbcTemplate.queryForMap(
                    "SELECT publish_attempts FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'", orderId.toString());
            assertThat(((Number) stillOne.get("publish_attempts")).intValue()).isEqualTo(1);

            // Advance the injected clock past the backoff window — this, not elapsed wall-clock
            // time, is what makes the row eligible again.
            mutableClock.advance(RETRY_BACKOFF.plusSeconds(1));

            RelayOutcome second = relayWorker.relayNextEvent();
            assertThat(second).isEqualTo(RelayOutcome.PUBLISH_FAILED);

            Map<String, Object> afterSecond = jdbcTemplate.queryForMap(
                    "SELECT publish_attempts, published_at FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'",
                    orderId.toString());
            assertThat(((Number) afterSecond.get("publish_attempts")).intValue()).isEqualTo(2);
            assertThat(afterSecond.get("published_at")).isNull();
        } finally {
            unpauseBroker();
        }

        // Keep advancing past backoff and retrying — bounded by the guard counter, never by
        // elapsed real time — mirroring scenario (a)'s robustness even though pause/unpause
        // shouldn't need more than one retry in practice (same container, same port, no state
        // lost).
        RelayOutcome third = null;
        int guard = 0;
        mutableClock.advance(RETRY_BACKOFF.plusSeconds(1));
        while (guard++ < 20 && third != RelayOutcome.PUBLISHED) {
            third = relayWorker.relayNextEvent();
            if (third != RelayOutcome.PUBLISHED) {
                mutableClock.advance(RETRY_BACKOFF.plusSeconds(1));
            }
        }
        assertThat(third).isEqualTo(RelayOutcome.PUBLISHED);

        Map<String, Object> afterThird = jdbcTemplate.queryForMap(
                "SELECT publish_attempts, published_at FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'",
                orderId.toString());
        assertThat(afterThird.get("published_at")).isNotNull();
        // At least the 2 recorded failed attempts plus this successful one — exactly 3 only if the
        // broker was immediately ready after restart, which isn't guaranteed (see above).
        assertThat(((Number) afterThird.get("publish_attempts")).intValue()).isGreaterThanOrEqualTo(3);
    }

    // ------------------------------------------------------------------------------------------
    // (d) Restart mid-drain: resumes and drains completely, per-aggregate order preserved.
    // ------------------------------------------------------------------------------------------
    @Test
    void restartMidDrainResumesAndPreservesPerAggregateOrder() throws Exception {
        drainAllPending();
        String aggregateId = "restart-drain-" + UUID.randomUUID();
        int totalEvents = 5;
        for (int seq = 1; seq <= totalEvents; seq++) {
            insertSyntheticOutboxRow(aggregateId, seq);
        }

        // "Before restart": process part of the backlog.
        for (int i = 0; i < 2; i++) {
            assertThat(relayWorker.relayNextEvent()).isEqualTo(RelayOutcome.PUBLISHED);
        }

        // "Restart": as in scenario (b), the worker holds no in-memory state between calls, so
        // simply calling it again is exactly what a freshly-started process resuming the backlog
        // looks like — no new object is needed to prove that; that statelessness is the point.
        int guard = 0;
        while (relayWorker.relayNextEvent() == RelayOutcome.PUBLISHED && guard++ < totalEvents + 2) {
            // drain the rest
        }

        Map<String, List<ConsumerRecord<String, String>>> byKey =
                consumeGroupedByKey(Map.of(aggregateId, totalEvents), Duration.ofSeconds(30));
        List<ConsumerRecord<String, String>> records = byKey.get(aggregateId);
        assertThat(records).hasSize(totalEvents);

        List<Integer> observedSeq = records.stream().map(r -> readSeq(r.value())).toList();
        assertThat(observedSeq).containsExactly(1, 2, 3, 4, 5);
    }

    // ------------------------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------------------------

    private UUID createOrder(long amountCents) throws Exception {
        String responseJson = mockMvc.perform(MockMvcRequestBuilders.post("/orders")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(mapper.writeValueAsString(new CreateOrderRequest(amountCents, null, null))))
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andReturn()
                .getResponse()
                .getContentAsString();
        return mapper.readValue(responseJson, OrderResponse.class).orderId();
    }

    private void drainAllPending() {
        int guard = 0;
        while (relayWorker.relayNextEvent() != RelayOutcome.NOTHING_TO_CLAIM && guard++ < 500) {
            // keep draining whatever is currently eligible
        }
    }

    private void insertSyntheticOutboxRow(String aggregateId, int seq) {
        UUID eventId = UUID.randomUUID();
        jdbcTemplate.update(
                """
                INSERT INTO outbox_events (
                    event_id, aggregate_type, aggregate_id, aggregate_sequence,
                    event_type, schema_version, correlation_id, causation_id,
                    traceparent, tracestate, payload, occurred_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?)
                """,
                eventId,
                "TestAggregate",
                aggregateId,
                (long) seq,
                "TestEvent",
                1,
                eventId,
                null,
                null,
                null,
                "{\"seq\":" + seq + "}",
                Timestamp.from(mutableClock.instant()));
    }

    private String readEventId(String envelopeJson) {
        try {
            return mapper.readValue(envelopeJson, EventEnvelope.class).eventId().toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private int readSeq(String envelopeJson) {
        try {
            return mapper.readValue(envelopeJson, EventEnvelope.class).payload().get("seq").asInt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Polls the topic until every key in {@code minCountByKey} has been seen at least that many
     * times, or the timeout elapses (a safety net, not something any assertion times) — real
     * Kafka consumption is genuinely asynchronous I/O and can't be made synchronous.
     */
    private Map<String, List<ConsumerRecord<String, String>>> consumeGroupedByKey(
            Map<String, Integer> minCountByKey, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "crash-window-test-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        Map<String, List<ConsumerRecord<String, String>>> byKey = new HashMap<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(Collections.singletonList(TOPIC));
            long deadline = System.currentTimeMillis() + timeout.toMillis();
            while (System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
                for (ConsumerRecord<String, String> record : records) {
                    if (minCountByKey.containsKey(record.key())) {
                        byKey.computeIfAbsent(record.key(), k -> new ArrayList<>()).add(record);
                    }
                }
                boolean satisfied = minCountByKey.entrySet().stream()
                        .allMatch(e -> byKey.getOrDefault(e.getKey(), List.of()).size() >= e.getValue());
                if (satisfied) {
                    break;
                }
            }
        }
        return byKey;
    }

    /** A {@link Clock} tests advance explicitly rather than waiting on real elapsed time. */
    static final class MutableClock extends Clock {
        private volatile Instant instant;

        MutableClock(Instant initial) {
            this.instant = initial;
        }

        void advance(Duration duration) {
            instant = instant.plus(duration);
        }

        void reset(Instant newInstant) {
            instant = newInstant;
        }

        @Override
        public ZoneId getZone() {
            return ZoneOffset.UTC;
        }

        @Override
        public Clock withZone(ZoneId zone) {
            return this;
        }

        @Override
        public Instant instant() {
            return instant;
        }
    }

    @TestConfiguration
    static class ClockConfig {
        @Bean
        @Primary
        MutableClock mutableClock() {
            return new MutableClock(Instant.now());
        }
    }
}
