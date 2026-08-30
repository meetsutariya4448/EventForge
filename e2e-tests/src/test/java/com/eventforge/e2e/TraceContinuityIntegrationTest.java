package com.eventforge.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.inventory.InventoryServiceApplication;
import com.eventforge.order.OrderServiceApplication;
import com.eventforge.payment.PaymentServiceApplication;
import io.opentelemetry.sdk.trace.data.SpanData;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

/**
 * Constitution M4 item 6: "an automated assertion, not a screenshot." Issues ONE real HTTP request
 * against a really-running order-service, lets the saga run to completion through really-running
 * payment-service and inventory-service (three independent Spring Boot contexts booted in this one
 * JVM — see {@link SharedTracingTestConfig}), and asserts on the exported spans directly: one trace
 * ID covers the whole chain, and every hop is a correctly-parented child of the one before it.
 *
 * <p>This is the one test in the whole project that spans real HTTP -&gt; order-service -&gt; outbox
 * -&gt; relay -&gt; Kafka -&gt; payment-service -&gt; Kafka -&gt; order-service (the orchestrator's own
 * consumer) -&gt; outbox -&gt; relay -&gt; Kafka -&gt; inventory-service, matching the happy path
 * exactly (constitution item 3). Everywhere else in this project deliberately tests hop-by-hop
 * (M2/M3's established discipline) — this module exists because item 6 specifically names three
 * real services cooperating in one trace, which no single service's own suite can prove alone.
 *
 * <p><b>Why every service's Kafka topic, not just its database and port, is overridden below.</b>
 * All three services' compiled {@code application.yml} resources land on this ONE test's shared
 * classpath (each is {@code testImplementation(project(...))}), each at the identical relative path
 * {@code application.yml}. Spring Boot's default config loading resolves that path via a single,
 * first-match classpath lookup, not a merge — so which service's file actually wins for a given
 * context depends on classpath ordering, not on which JAR that context's own main class came from.
 * Observed directly: payment-service's context read {@code eventforge.outbox.relay.topic} as
 * {@code orders.events} (order-service's value) instead of its own {@code payments.events}, so its
 * relay published {@code PaymentAuthorized} to the wrong topic — invisible to any consumer of
 * {@code payments.events}, including order-service's own saga listener. Same root cause, same fix
 * shape as the pre-existing Flyway {@code db/migration} classpath collision immediately below:
 * command-line overrides for every value a given context must NOT inherit from a sibling service's
 * file. This is a test-harness-only artifact of co-locating three real Spring Boot apps in one JVM;
 * no service's own {@code application.yml} changes, and it cannot happen in production, where each
 * service is its own process with its own isolated classpath.
 */
@Testcontainers
class TraceContinuityIntegrationTest {

    @Container
    static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1");

    @Container
    static final PostgreSQLContainer<?> orderDb = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static final PostgreSQLContainer<?> paymentDb = new PostgreSQLContainer<>("postgres:16-alpine");

    @Container
    static final PostgreSQLContainer<?> inventoryDb = new PostgreSQLContainer<>("postgres:16-alpine");

    private static ConfigurableApplicationContext orderCtx;
    private static ConfigurableApplicationContext paymentCtx;
    private static ConfigurableApplicationContext inventoryCtx;

    @BeforeAll
    static void startServices() {
        // If any of the three fails to start, close whatever DID start before rethrowing - a
        // half-started attempt left open would otherwise leak a bound port (JUnit does not call
        // @AfterAll when @BeforeAll itself throws) into whatever runs next.
        try {
            orderCtx = new SpringApplicationBuilder(OrderServiceApplication.class)
                    .sources(SharedTracingTestConfig.class)
                    .run(overridesFor(orderDb, "order-service", "orders.events"));
            paymentCtx = new SpringApplicationBuilder(PaymentServiceApplication.class)
                    .sources(SharedTracingTestConfig.class)
                    .run(overridesFor(paymentDb, "payment-service", "payments.events"));
            inventoryCtx = new SpringApplicationBuilder(InventoryServiceApplication.class)
                    .sources(SharedTracingTestConfig.class)
                    .run(overridesFor(inventoryDb, "inventory-service", "inventory.events"));
        } catch (RuntimeException e) {
            stopServices();
            throw e;
        }
    }

    @AfterAll
    static void stopServices() {
        if (inventoryCtx != null) inventoryCtx.close();
        if (paymentCtx != null) paymentCtx.close();
        if (orderCtx != null) orderCtx.close();
    }

    @BeforeEach
    void resetSpans() {
        SharedTracingTestConfig.EXPORTER.reset();
    }

    // Command-line-style args, not SpringApplicationBuilder.properties(...): that method sets
    // "default properties," Spring Boot's LOWEST-precedence source - it cannot override the
    // fixed local-dev values each service's own application.yml hardcodes (e.g.
    // jdbc:postgresql://localhost:5433/order). Command-line args are the HIGHEST-precedence
    // source, which is what's actually needed to redirect a real, already-configured service at
    // these Testcontainers-assigned hosts/ports.
    private static String[] overridesFor(PostgreSQLContainer<?> db, String moduleName, String ownTopic) {
        return new String[] {
            "--spring.application.name=" + moduleName,
            "--spring.datasource.url=" + db.getJdbcUrl(),
            "--spring.datasource.username=" + db.getUsername(),
            "--spring.datasource.password=" + db.getPassword(),
            "--spring.kafka.bootstrap-servers=" + kafka.getBootstrapServers(),
            // Random port, not each service's own fixed local-dev port (8081/8082/8083): three
            // real embedded servers booting in one JVM has no business assuming those ports are
            // free, and a leaked Tomcat from an earlier failed run (Spring's own cleanup on a
            // startup exception isn't always immediate) would otherwise collide with this run.
            // order-service's actual assigned port is read back via local.server.port below.
            "--server.port=0",
            // The production SDK (a real OTLP exporter with nothing listening in this test) is
            // disabled; SharedTracingTestConfig's beans are @Primary regardless, but this also
            // avoids a second, pointless OpenTelemetrySdk instance trying to export in the background.
            "--eventforge.tracing.enabled=false",
            // See the class Javadoc: each service's own application.yml is not reliably the one
            // that wins on this shared test classpath, so its own relay topic must be forced
            // explicitly rather than trusted to come from the right file.
            "--eventforge.outbox.relay.topic=" + ownTopic,
            // All three services' compiled resources land on this ONE test's shared classpath
            // (testImplementation on all three project(...)s), and every service's Flyway
            // migrations live at the SAME relative path (db/migration) - Flyway's default
            // classpath:db/migration scan can't tell which JAR a given V3 file came from and
            // fails on the resulting duplicate-version collision. Pointing each context at its own
            // migration directory on disk, by module name, sidesteps classpath scanning entirely -
            // this is a test-harness concern only; no service's own Flyway config changes.
            "--spring.flyway.locations=filesystem:" + migrationDirFor(moduleName),
            // Generous, not tuned: three freshly-booted JVMs' first real Kafka round-trip (topic
            // metadata, producer/consumer connection warmup) can eat into the production 10s
            // default enough to make the saga's OWN timeout/compensation machinery fire before the
            // happy path ever gets a fair chance - which it did once, self-healing exactly as
            // designed (see ADR-0015), but that's not what THIS test exists to prove. Precise
            // timeout behavior is order-service's own SagaTimeoutIntegrationTest's job.
            "--eventforge.saga.authorize-payment-timeout-ms=60000",
            "--eventforge.saga.reserve-inventory-timeout-ms=60000",
            "--eventforge.saga.refund-payment-timeout-ms=60000"
        };
    }

    private static String migrationDirFor(String moduleName) {
        Path dir = repoRoot().resolve(moduleName).resolve("src/main/resources/db/migration");
        if (!dir.toFile().isDirectory()) {
            throw new IllegalStateException("No migration directory found at " + dir);
        }
        return dir.toAbsolutePath().toString();
    }

    private static Path repoRoot() {
        Path dir = Paths.get("").toAbsolutePath();
        while (dir != null && !dir.resolve("settings.gradle.kts").toFile().isFile()) {
            dir = dir.getParent();
        }
        if (dir == null) {
            throw new IllegalStateException("Could not locate repo root (settings.gradle.kts) from " + Paths.get("").toAbsolutePath());
        }
        return dir;
    }

    @Test
    void oneHttpRequestProducesOneTraceAcrossAllThreeRealServices() throws Exception {
        JdbcTemplate orderJdbc = orderCtx.getBean(JdbcTemplate.class);

        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:" + orderCtx.getEnvironment().getProperty("local.server.port") + "/orders"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"amountCents\":2500}"))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(201);

        String orderId = extractOrderId(response.body());
        waitForSagaState(orderJdbc, orderId, "COMPLETED", Duration.ofSeconds(30));

        List<SpanData> spans = SharedTracingTestConfig.EXPORTER.getFinishedSpanItems();

        // A single trace ID covers HTTP -> order-service's outbox/relay -> payment-service ->
        // order-service's own saga listener -> inventory-service. If any hop had started a NEW
        // trace instead of continuing the propagated one (the exact bug a verbatim header copy, or
        // a missing extract, would cause), this assertion fails immediately.
        Set<String> traceIds = spans.stream().map(SpanData::getTraceId).collect(Collectors.toSet());
        assertThat(traceIds)
                .as("every span exported by any of the three services must belong to the same trace: %s", spans)
                .hasSize(1);

        Map<String, SpanData> bySpanId = spans.stream().collect(Collectors.toMap(SpanData::getSpanId, s -> s));

        SpanData httpSpan = findOne(spans, "POST /orders");
        SpanData authorizeRelaySpan = findOne(spans, "relay.publish orders.events AuthorizePayment");
        SpanData paymentSpan = findOne(spans, "payment.AuthorizePayment");
        SpanData paymentRelaySpan = findOne(spans, "relay.publish payments.events PaymentAuthorized");
        SpanData sagaFactSpan = findOne(spans, "saga.PaymentAuthorized");
        SpanData reserveRelaySpan = findOne(spans, "relay.publish orders.events ReserveInventory");
        SpanData inventorySpan = findOne(spans, "inventory.reserve");

        // The parent-child chain constitution item 3 asks to be proven, not eyeballed: each hop's
        // parent is specifically the span that dispatched it, not the HTTP span directly (which
        // would mean the relay stayed invisible) and not "no parent" (which would mean a fresh,
        // disconnected trace started somewhere along the way).
        assertParent(bySpanId, authorizeRelaySpan, httpSpan);
        assertParent(bySpanId, paymentSpan, authorizeRelaySpan);
        assertParent(bySpanId, paymentRelaySpan, paymentSpan);
        assertParent(bySpanId, sagaFactSpan, paymentRelaySpan);
        assertParent(bySpanId, reserveRelaySpan, sagaFactSpan);
        assertParent(bySpanId, inventorySpan, reserveRelaySpan);
    }

    private static void assertParent(Map<String, SpanData> bySpanId, SpanData child, SpanData expectedParent) {
        assertThat(child.getParentSpanId())
                .as("%s must be a direct child of %s, not a sibling or a disconnected root", child.getName(), expectedParent.getName())
                .isEqualTo(expectedParent.getSpanId());
        assertThat(bySpanId).containsKey(child.getParentSpanId());
    }

    private static SpanData findOne(List<SpanData> spans, String name) {
        List<SpanData> matches = spans.stream().filter(s -> s.getName().equals(name)).toList();
        assertThat(matches).as("expected exactly one span named '%s' among %s", name, spans.stream().map(SpanData::getName).toList()).hasSize(1);
        return matches.get(0);
    }

    private static String extractOrderId(String responseBody) {
        // {"orderId":"...","status":"PENDING","amountCents":2500} - avoiding a Jackson dependency
        // here since this module has no other JSON-parsing need.
        int start = responseBody.indexOf("\"orderId\":\"") + "\"orderId\":\"".length();
        int end = responseBody.indexOf('"', start);
        return responseBody.substring(start, end);
    }

    private static void waitForSagaState(JdbcTemplate jdbcTemplate, String orderId, String expected, Duration timeout)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        String last = null;
        while (System.currentTimeMillis() < deadline) {
            List<String> rows = jdbcTemplate.queryForList(
                    "SELECT state FROM saga_instance WHERE order_id = ?::uuid", String.class, orderId);
            if (!rows.isEmpty()) {
                last = rows.get(0);
                if (expected.equals(last)) {
                    return;
                }
            }
            Thread.sleep(200);
        }
        throw new AssertionError(
                "Timed out waiting for saga on order " + orderId + " to reach " + expected + " (was " + last + ")");
    }
}
