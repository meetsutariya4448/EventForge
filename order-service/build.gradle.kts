plugins {
    id("eventforge.spring-service-conventions")
}

dependencies {
    implementation(project(":common-events"))
    // The first real business entity in the project (Order) — see ADR-0007: JPA was deliberately
    // deferred until there was one. outbox_events/processed_events access stays JdbcTemplate-only.
    implementation("org.springframework.boot:spring-boot-starter-data-jpa")
    testImplementation(project(":common-testing"))
    // MockMvc + @AutoConfigureMockMvc moved out of spring-boot-starter-test in Boot 4's
    // modularization (see ADR-0009) into this dedicated module.
    testImplementation("org.springframework.boot:spring-boot-webmvc-test")
}
