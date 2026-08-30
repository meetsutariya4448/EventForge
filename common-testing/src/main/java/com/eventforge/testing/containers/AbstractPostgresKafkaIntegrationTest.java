package com.eventforge.testing.containers;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Shared base for integration tests that need real Postgres and real Kafka (never mocked, per
 * R5). Container connection details are wired into the Spring context automatically via
 * {@code @ServiceConnection} — no manual {@code @DynamicPropertySource} boilerplate.
 *
 * <p>These containers are started fresh by Testcontainers for the test JVM; they have no
 * relationship to {@code docker/docker-compose.yml}; see ADR-0008.
 */
@Testcontainers
public abstract class AbstractPostgresKafkaIntegrationTest {

    // Pinned by digest, not just the mutable "postgres:16-alpine" tag — same digest as
    // docker/docker-compose.yml; re-pin both together after a deliberate, verified upgrade.
    // asCompatibleSubstituteFor("postgres") is required here: Testcontainers' own image-name
    // compatibility check does not recognize a digest-suffixed reference as a substitute for
    // "postgres" without being told explicitly (it throws IllegalStateException otherwise) — this
    // is Testcontainers' documented mechanism for exactly this case, not a workaround.
    private static final DockerImageName POSTGRES_IMAGE = DockerImageName.parse(
                    "postgres:16-alpine@sha256:cf78e76683b9ca8c5733cbbdce6c9262b45b6767934dd0a95e671f9a0fc20685")
            .asCompatibleSubstituteFor("postgres");

    // The explicit "postgres" name is required, not decorative: without it, Spring Boot's own
    // @ServiceConnection deduces the connection name by parsing the image name itself
    // (ContainerConnectionSource.getOrDeduceConnectionName), which throws
    // IllegalArgumentException on a digest-suffixed reference even though Testcontainers' own
    // asCompatibleSubstituteFor(...) above already made the container itself start correctly —
    // two independent parsers, only one of which was told about the substitution.
    @Container
    @ServiceConnection("postgres")
    protected static final PostgreSQLContainer postgres = new PostgreSQLContainer(POSTGRES_IMAGE);

    @Container
    @ServiceConnection
    protected static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1");
}
