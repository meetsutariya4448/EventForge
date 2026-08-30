package com.eventforge.testing.containers;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import java.io.IOException;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Like {@link AbstractPostgresKafkaIntegrationTest}, but Kafka traffic is routed through a real
 * Toxiproxy proxy, so tests can inject connection-level failures a container pause/stop can't
 * represent: {@link #kafkaProxy}{@code .disable()} for genuine connection-refused (new connection
 * attempts are rejected immediately, unlike {@code OutboxRelayCrashWindowIntegrationTest}'s pause,
 * which just makes the broker unresponsive), and {@code kafkaProxy.toxics().latency(...)} for
 * injected network latency. Built for M7's slow-broker measurement work — this class only proves
 * the seam works; it implements no benchmark itself.
 *
 * <p>Kafka is given an additional advertised listener whose address is a {@link java.util.function.Supplier}
 * resolving to Toxiproxy's mapped port, per the pattern Testcontainers' own {@code KafkaContainer}
 * test suite uses for exactly this case ({@code testUsageWithListenerFromProxy}). Because that
 * supplier is only evaluated when Kafka starts, and it depends on a proxy that must already exist
 * in Toxiproxy's own config, container startup order matters here in a way the other shared base
 * classes don't have to worry about: Toxiproxy must be running and the proxy registered with it
 * before Kafka starts. That ordering is handled explicitly below rather than left to field
 * declaration order, which the {@code @Testcontainers} JUnit extension does not guarantee matches
 * source order for manually-managed startup logic like this.
 */
@Testcontainers
public abstract class AbstractToxicKafkaIntegrationTest {

    private static final int KAFKA_PROXY_PORT = 8666;
    private static final String KAFKA_NETWORK_ALIAS = "kafka";
    private static final int KAFKA_INTERNAL_PORT = 19092;

    protected static final Network network = Network.newNetwork();

    // Same digest as docker/docker-compose.yml and AbstractPostgresKafkaIntegrationTest; see that
    // class's comment for why asCompatibleSubstituteFor("postgres") is required, not optional.
    private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse(
                    "postgres:16-alpine@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685")
            .asCompatibleSubstituteFor("postgres");

    // Explicit "postgres" name required — see AbstractPostgresKafkaIntegrationTest's comment:
    // Spring Boot's own @ServiceConnection name deduction throws on a digest-suffixed image name
    // unless told the name explicitly, independent of Testcontainers' own substitution above.
    @Container
    @ServiceConnection("postgres")
    protected static final PostgreSQLContainer postgres = new PostgreSQLContainer(POSTGRES_IMAGE);

    // Not @Container-managed: its startup must happen, in order, before kafka's — see the class
    // Javadoc. Still cleaned up at JVM exit via Testcontainers' Ryuk reaper like any other
    // container it creates, the same as the @Container-managed fields elsewhere in this project.
    protected static final ToxiproxyContainer toxiproxy =
            new ToxiproxyContainer("ghcr.io/shopify/toxiproxy:2.5.0").withNetwork(network);

    protected static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1")
            .withNetwork(network)
            .withNetworkAliases(KAFKA_NETWORK_ALIAS)
            .withListener(
                    KAFKA_NETWORK_ALIAS + ":" + KAFKA_INTERNAL_PORT,
                    () -> toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(KAFKA_PROXY_PORT));

    /** The Toxiproxy proxy in front of Kafka. Call {@code .disable()}/{@code .enable()} for
     * connection-refused, or {@code .toxics().latency(...)} for injected latency. */
    protected static final Proxy kafkaProxy;

    static {
        toxiproxy.start();
        try {
            ToxiproxyClient client = new ToxiproxyClient(toxiproxy.getHost(), toxiproxy.getControlPort());
            kafkaProxy = client.createProxy(
                    "kafka", "0.0.0.0:" + KAFKA_PROXY_PORT, KAFKA_NETWORK_ALIAS + ":" + KAFKA_INTERNAL_PORT);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to register the Toxiproxy proxy for Kafka", e);
        }
        kafka.start();
    }

    /** The bootstrap-servers address a client must use to actually go through the proxy. */
    protected static String bootstrapServersThroughProxy() {
        return toxiproxy.getHost() + ":" + toxiproxy.getMappedPort(KAFKA_PROXY_PORT);
    }

    // Deliberately not @ServiceConnection on kafka itself: that would wire Spring straight to
    // Kafka's own directly-exposed port, bypassing the proxy entirely. The application must go
    // through Toxiproxy for injected failures to have any effect on it.
    @DynamicPropertySource
    static void kafkaThroughProxy(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", AbstractToxicKafkaIntegrationTest::bootstrapServersThroughProxy);
    }
}
