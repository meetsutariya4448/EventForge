plugins {
    id("eventforge.java-conventions")
}

dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:4.1.1"))
    api(platform("org.testcontainers:testcontainers-bom:2.0.5"))

    api(project(":common-events"))

    api("org.springframework.boot:spring-boot-starter-test")
    api("org.springframework.boot:spring-boot-testcontainers")
    api("org.springframework.boot:spring-boot-starter-kafka-test")

    api("org.testcontainers:testcontainers-junit-jupiter")
    api("org.testcontainers:testcontainers-postgresql")
    api("org.testcontainers:testcontainers-kafka")
    // Fronts Kafka with a real network proxy for M7's slow-broker measurement work — lets tests
    // inject connection-refused and latency, which a container pause/stop can't represent.
    api("org.testcontainers:testcontainers-toxiproxy")

    // M3 item 7: the shared rule that fails the build if a service publishes to Kafka outside
    // OutboxWriter/OutboxRelayWorker. One well-established tool for exactly this job, not a
    // hand-rolled reflection scanner.
    api("com.tngtech.archunit:archunit-junit5:1.3.0")

    // M4: InMemorySpanExporter — the automated trace-continuity test (constitution item 6) queries
    // this directly rather than a real Jaeger, exactly the "test, not a screenshot" discipline the
    // milestone demands. Version comes from common-events' opentelemetry-bom platform constraint.
    api("io.opentelemetry:opentelemetry-sdk-testing")

    runtimeOnly("org.junit.platform:junit-platform-launcher")
}
