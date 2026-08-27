package com.eventforge.testing.containers;

import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

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

    @Container
    @ServiceConnection
    protected static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

    @Container
    @ServiceConnection
    protected static final KafkaContainer kafka = new KafkaContainer("apache/kafka:4.3.1");
}
