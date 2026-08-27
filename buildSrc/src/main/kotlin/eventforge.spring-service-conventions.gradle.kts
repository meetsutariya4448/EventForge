plugins {
    id("eventforge.java-conventions")
    id("org.springframework.boot")
}

dependencies {
    "implementation"(platform("org.springframework.boot:spring-boot-dependencies:4.1.1"))
    "implementation"("org.springframework.boot:spring-boot-starter-web")
    "implementation"("org.springframework.boot:spring-boot-starter-actuator")
    "implementation"("org.springframework.boot:spring-boot-starter-jdbc")
    "implementation"("org.springframework.boot:spring-boot-starter-kafka")
    "implementation"("org.springframework.boot:spring-boot-starter-flyway")
    "implementation"("org.flywaydb:flyway-database-postgresql")
    "runtimeOnly"("org.postgresql:postgresql")
}
