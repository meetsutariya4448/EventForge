plugins {
    id("eventforge.java-conventions")
    id("org.springframework.boot")
}

dependencies {
    "implementation"(platform("org.springframework.boot:spring-boot-dependencies:4.1.1"))
    "implementation"("org.springframework.boot:spring-boot-starter-web")
    "implementation"("org.springframework.boot:spring-boot-starter-actuator")
    // v2: every service authenticates, via common-events' EventForgeSecurityAutoConfiguration.
    "implementation"("org.springframework.boot:spring-boot-starter-security")
    // Lets tests drive the REAL filter chain (httpBasic() request post-processors) rather than
    // bypassing it with mocked authentication — the point is to exercise what ships.
    "testImplementation"("org.springframework.security:spring-security-test")
    // v2 WS2: serves the OpenAPI description at /v3/api-docs, which the console's TypeScript types
    // are generated from rather than hand-written — a hand-written client is a second, silently
    // drifting definition of the same contract. EventForgeSecurityAutoConfiguration already
    // permits these paths; until now nothing served them, so those permits described an endpoint
    // that did not exist.
    //
    // Version pinned deliberately: springdoc 3.x is the Spring Boot 4 line (2.8.x/2.9.x target
    // Boot 3). Maven Central's search API still reports 2.8.6 as latest, which is stale — read
    // from maven-metadata.xml instead if this ever needs re-checking.
    "implementation"("org.springdoc:springdoc-openapi-starter-webmvc-ui:3.1.1")
    "implementation"("org.springframework.boot:spring-boot-starter-jdbc")
    "implementation"("org.springframework.boot:spring-boot-starter-kafka")
    "implementation"("org.springframework.boot:spring-boot-starter-flyway")
    "implementation"("org.flywaydb:flyway-database-postgresql")
    "runtimeOnly"("org.postgresql:postgresql")
}
