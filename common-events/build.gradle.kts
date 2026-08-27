plugins {
    id("eventforge.java-conventions")
}

dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:4.1.1"))

    api("com.fasterxml.jackson.core:jackson-databind")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // Auto-configuration support for the FaultInjector no-op default bean.
    // compileOnly: every consuming Spring Boot service already brings these at runtime
    // via spring-boot-starter; common-events must not force Spring onto non-Spring consumers.
    compileOnly("org.springframework.boot:spring-boot-autoconfigure")
    compileOnly("org.springframework:spring-context")

    testImplementation("org.junit.jupiter:junit-jupiter")
    testImplementation("org.assertj:assertj-core")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}
