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
    "implementation"("org.springframework.boot:spring-boot-starter-jdbc")
    "implementation"("org.springframework.boot:spring-boot-starter-kafka")
    "implementation"("org.springframework.boot:spring-boot-starter-flyway")
    "implementation"("org.flywaydb:flyway-database-postgresql")
    "runtimeOnly"("org.postgresql:postgresql")
}
