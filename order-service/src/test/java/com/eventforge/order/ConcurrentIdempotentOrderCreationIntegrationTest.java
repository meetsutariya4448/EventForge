package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.order.api.CreateOrderRequest;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The completion evidence for WS1: the same request issued concurrently creates exactly one
 * order. Sequential retry testing does not establish this — it is the simultaneous case, where
 * every worker's claim lands inside every other worker's open transaction, that the design has
 * to survive.
 *
 * <p>Mirrors {@code ConcurrentDuplicateDeliveryIntegrationTest}'s shape (a {@link CountDownLatch}
 * releasing all workers together, so they contend rather than trickle in, and a Hikari pool sized
 * for them). It differs in one respect deliberately: it drives real HTTP rather than calling the
 * service directly, because a blocked loser holds a <em>servlet thread</em> as well as a
 * connection, and that cost is part of what is being demonstrated. {@code TestRestTemplate} is
 * absent from Spring Boot 4.1.1 (ADR-0009), so this uses {@link HttpClient} the way
 * {@code TraceContinuityIntegrationTest} does.
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
class ConcurrentIdempotentOrderCreationIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final int WORKER_COUNT = 16;

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        registry.add("eventforge.idempotency.reaper-enabled", () -> "false");
        // Every loser blocks on the winner's claim for as long as the winner's whole transaction
        // takes (order row, OrderCreated, saga_instance, AuthorizePayment). Generous enough that
        // the losers replay rather than time out, so this test exercises the intended path; the
        // timeout path itself is IdempotencyLockTimeoutIntegrationTest's job.
        registry.add("eventforge.idempotency.lock-timeout", () -> "10s");
        // One pooled connection per blocked worker, plus headroom — without this the workers
        // starve each other on the pool instead of contending on the key, and the test would
        // prove nothing about idempotency.
        registry.add("spring.datasource.hikari.maximum-pool-size", () -> String.valueOf(WORKER_COUNT + 5));
    }

    @LocalServerPort
    private int port;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void sixteenSimultaneousRequestsUnderOneKeyCreateExactlyOneOrder() throws Exception {
        String key = "concurrent-" + UUID.randomUUID();
        String body = mapper.writeValueAsString(new CreateOrderRequest(4200, "SKU-CONCURRENT", 1L));

        HttpClient client = HttpClient.newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();
        CountDownLatch startingGate = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(WORKER_COUNT);

        List<HttpResponse<String>> responses = new ArrayList<>();
        try {
            List<Callable<HttpResponse<String>>> tasks = new ArrayList<>();
            for (int i = 0; i < WORKER_COUNT; i++) {
                tasks.add(() -> {
                    HttpRequest request = HttpRequest.newBuilder()
                            .uri(URI.create("http://localhost:" + port + "/orders"))
                            .header("Content-Type", "application/json")
                            .header("Idempotency-Key", key)
                            .timeout(Duration.ofSeconds(30))
                            .POST(HttpRequest.BodyPublishers.ofString(body))
                            .build();
                    startingGate.await();
                    return client.send(request, HttpResponse.BodyHandlers.ofString());
                });
            }

            List<Future<HttpResponse<String>>> futures = new ArrayList<>();
            for (Callable<HttpResponse<String>> task : tasks) {
                futures.add(executor.submit(task));
            }
            startingGate.countDown();
            for (Future<HttpResponse<String>> future : futures) {
                responses.add(future.get(60, TimeUnit.SECONDS));
            }
        } finally {
            executor.shutdownNow();
        }

        // The invariant that matters, stated first: one order, one saga, one OrderCreated —
        // regardless of which worker won.
        String orderId = mapper.readTree(responses.get(0).body()).get("orderId").asText();
        assertThat(countOrders()).as("exactly one order for %d concurrent requests", WORKER_COUNT).isEqualTo(1);
        assertThat(countSagas(orderId)).isEqualTo(1);
        assertThat(countOrderCreatedEvents(orderId)).isEqualTo(1);

        // Every caller got a usable answer; none saw a server error.
        assertThat(responses).allSatisfy(response -> assertThat(response.statusCode())
                .as("every concurrent caller must get 201, not an error")
                .isEqualTo(201));

        // Exactly one of them actually did the work; the rest replayed that same response.
        long created = responses.stream().filter(r -> !isReplayed(r)).count();
        long replayed = responses.stream().filter(this::isReplayed).count();
        assertThat(created).as("exactly one request performs the write").isEqualTo(1);
        assertThat(replayed).isEqualTo(WORKER_COUNT - 1);

        // And every caller was told about the same order, byte for byte.
        assertThat(responses).allSatisfy(response ->
                assertThat(response.body()).isEqualTo(responses.get(0).body()));
    }

    private boolean isReplayed(HttpResponse<String> response) {
        return response.headers().firstValue("Idempotency-Replayed").map("true"::equals).orElse(false);
    }

    private Integer countOrders() {
        return jdbcTemplate.queryForObject("SELECT count(*) FROM orders", Integer.class);
    }

    private Integer countSagas(String orderId) {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM saga_instance WHERE order_id = ?::uuid", Integer.class, orderId);
    }

    private Integer countOrderCreatedEvents(String orderId) {
        return jdbcTemplate.queryForObject(
                "SELECT count(*) FROM outbox_events WHERE aggregate_id = ? AND event_type = 'OrderCreated'",
                Integer.class,
                orderId);
    }
}
