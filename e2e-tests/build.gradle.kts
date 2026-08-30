plugins {
    id("eventforge.java-conventions")
}

// M4 item 6: "an integration test that issues one HTTP request... and asserts a single trace ID
// spans HTTP -> order-service -> outbox -> relay -> Kafka -> payment-service -> inventory-service"
// cannot be satisfied by any ONE service's own test suite — M2/M3's hop-by-hop testing discipline
// (each service's suite proves its own hop for real, using synthetic upstream facts) is correct
// and deliberate for everything up to now, but this specific acceptance criterion names three real
// services' real consumers cooperating in one trace. This module exists for exactly that one test:
// it boots order-service, payment-service, and inventory-service as three independent, real Spring
// Boot application contexts in one JVM (via SpringApplicationBuilder, not @SpringBootTest, since
// @SpringBootTest manages exactly one context) against shared Testcontainers infra.
dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:4.1.1"))

    testImplementation(project(":order-service"))
    testImplementation(project(":payment-service"))
    testImplementation(project(":inventory-service"))
    testImplementation(project(":common-testing"))
    // The service modules declare spring-boot-starter-jdbc as `implementation`, not `api`, so it
    // doesn't propagate transitively here - this test reads saga_instance directly via JdbcTemplate.
    testImplementation("org.springframework.boot:spring-boot-starter-jdbc")

    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
