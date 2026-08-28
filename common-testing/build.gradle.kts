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

    runtimeOnly("org.junit.platform:junit-platform-launcher")
}
