plugins {
    id("eventforge.spring-service-conventions")
}

dependencies {
    implementation(project(":common-events"))
    // First real business entity in this service (Payment) - see ADR-0007: JPA deferred until
    // there was one. outbox_events/processed_events access stays JdbcTemplate-only.
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    testImplementation(project(":common-testing"))
}
