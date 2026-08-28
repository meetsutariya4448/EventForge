package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.outbox.OutboxRelayWorker;
import com.eventforge.events.outbox.RelayOutcome;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * M1 addendum: does the head-row claim design (ADR-0010) actually hold up under real concurrent
 * relay workers, or was T1 only proven safe for the single-worker case? This launches N threads
 * all calling the same {@link OutboxRelayWorker} bean concurrently — a faithful stand-in for N
 * separate worker processes, since the bean is stateless and {@code @Transactional} gives each
 * invocation its own transaction regardless of which thread calls it — and asserts that
 * per-aggregate publish order survives real, contended concurrency, not just sequential calls.
 *
 * <p>No lease-based claiming is introduced here regardless of outcome — this test only measures
 * what the current design already does.
 */
@SpringBootTest
class MultiWorkerRelayOrderingIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final String TOPIC = "orders.events";
    private static final int AGGREGATE_COUNT = 6;
    private static final int EVENTS_PER_AGGREGATE = 8;
    // Deliberately oversubscribed relative to AGGREGATE_COUNT so most claim attempts contend for
    // an already-locked head row (exercising FOR UPDATE SKIP LOCKED under real pressure), not just
    // running WORKER_COUNT workers in parallel with plenty of aggregates to go around. Verified at
    // WORKER_COUNT=20 across multiple runs during development with the same result; kept at 10
    // here to keep routine runs quick without giving up meaningful contention.
    private static final int WORKER_COUNT = 10;

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        // Same reasoning as the crash-window tests: the background @Scheduled poller must not
        // fire mid-test and race the explicitly-launched worker threads.
        registry.add("eventforge.outbox.relay.poll-interval-ms", () -> "3600000");
        // WORKER_COUNT concurrent relay transactions, plus the test's own JDBC calls, need more
        // headroom than Hikari's default pool size to avoid a false negative from pool exhaustion
        // rather than an actual claiming bug.
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> String.valueOf(WORKER_COUNT + 5));
    }

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private OutboxRelayWorker relayWorker;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void concurrentWorkersPreservePerAggregateOrderAndClaimEachRowExactlyOnce() throws Exception {
        drainAllPending();

        List<String> aggregateIds = new ArrayList<>();
        for (int a = 0; a < AGGREGATE_COUNT; a++) {
            String aggregateId = "multi-worker-" + UUID.randomUUID();
            aggregateIds.add(aggregateId);
            for (int seq = 1; seq <= EVENTS_PER_AGGREGATE; seq++) {
                insertSyntheticOutboxRow(aggregateId, seq);
            }
        }
        int totalEvents = AGGREGATE_COUNT * EVENTS_PER_AGGREGATE;

        AtomicInteger published = new AtomicInteger(0);
        ExecutorService executor = Executors.newFixedThreadPool(WORKER_COUNT);
        try {
            List<Callable<Void>> workers = new ArrayList<>();
            for (int w = 0; w < WORKER_COUNT; w++) {
                workers.add(() -> {
                    // Each worker drains until the shared counter says the whole backlog is done —
                    // not until this thread individually sees NOTHING_TO_CLAIM, since that can be
                    // transiently true for one thread while others are still mid-transaction.
                    while (published.get() < totalEvents) {
                        RelayOutcome outcome = relayWorker.relayNextEvent();
                        if (outcome == RelayOutcome.PUBLISHED) {
                            published.incrementAndGet();
                        }
                    }
                    return null;
                });
            }

            // A bounded safety net against a genuine hang, not a correctness assertion — nothing
            // here depends on how long draining actually takes.
            List<Future<Void>> futures = new ArrayList<>();
            for (Callable<Void> worker : workers) {
                futures.add(executor.submit(worker));
            }
            for (Future<Void> future : futures) {
                future.get(60, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertThat(published.get()).isEqualTo(totalEvents);

        Map<String, Integer> expectedCounts = new HashMap<>();
        for (String aggregateId : aggregateIds) {
            expectedCounts.put(aggregateId, EVENTS_PER_AGGREGATE);
        }
        Map<String, List<ConsumerRecord<String, String>>> byKey =
                consumeGroupedByKey(expectedCounts, Duration.ofSeconds(30));

        for (String aggregateId : aggregateIds) {
            List<ConsumerRecord<String, String>> records = byKey.get(aggregateId);
            // Exactly one copy of each event: no lost claims (every row published), and no
            // double-claims (nothing published twice) even under WORKER_COUNT-way contention.
            assertThat(records).hasSize(EVENTS_PER_AGGREGATE);

            List<Integer> observedSeq = records.stream().map(r -> readSeq(r.value())).toList();
            assertThat(observedSeq)
                    .as("per-aggregate publish order for %s", aggregateId)
                    .containsExactlyElementsOf(java.util.stream.IntStream.rangeClosed(1, EVENTS_PER_AGGREGATE)
                            .boxed()
                            .toList());
        }
    }

    private void drainAllPending() {
        int guard = 0;
        while (relayWorker.relayNextEvent() != RelayOutcome.NOTHING_TO_CLAIM && guard++ < 500) {
            // keep draining leftover unpublished rows from other tests in this shared container
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
                Timestamp.from(Instant.now()));
    }

    private int readSeq(String envelopeJson) {
        try {
            return mapper.readValue(envelopeJson, EventEnvelope.class).payload().get("seq").asInt();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, List<ConsumerRecord<String, String>>> consumeGroupedByKey(
            Map<String, Integer> minCountByKey, Duration timeout) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "multi-worker-test-" + UUID.randomUUID());
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
}
