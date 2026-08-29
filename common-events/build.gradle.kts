plugins {
    id("eventforge.java-conventions")
}

dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:4.1.1"))

    api("com.fasterxml.jackson.core:jackson-databind")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // Auto-configuration support for the FaultInjector/OutboxWriter/OutboxRelay default beans.
    // compileOnly: every consuming Spring Boot service already brings these at runtime via its
    // own starters (spring-boot-starter-jdbc, spring-boot-starter-kafka); common-events must not
    // force any of this onto a non-Spring or non-Kafka consumer.
    compileOnly("org.springframework.boot:spring-boot-autoconfigure")
    compileOnly("org.springframework:spring-context")
    compileOnly("org.springframework:spring-tx")
    compileOnly("org.springframework:spring-jdbc")
    compileOnly("org.springframework.boot:spring-boot-jdbc")
    compileOnly("org.springframework.kafka:spring-kafka")
    compileOnly("org.springframework.boot:spring-boot-kafka")
    compileOnly("org.apache.kafka:kafka-clients")
    compileOnly("org.slf4j:slf4j-api")

    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
