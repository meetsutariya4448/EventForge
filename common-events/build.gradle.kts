plugins {
    id("eventforge.java-conventions")
}

dependencies {
    implementation(platform("org.springframework.boot:spring-boot-dependencies:4.1.1"))
    api(platform("io.opentelemetry:opentelemetry-bom:1.65.0"))

    api("com.fasterxml.jackson.core:jackson-databind")
    api("com.fasterxml.jackson.datatype:jackson-datatype-jsr310")

    // M4: EventForgeTracer/SpanHandle expose OTel API types (Tracer, Span, SpanKind) directly in
    // their own public signatures, so every consuming service needs these on its own compile
    // classpath too — api, not compileOnly. The SDK/exporter/semconv artifacts are only needed to
    // BUILD the SdkTracerProvider in TracingAutoConfiguration; consumers never reference SDK
    // classes directly, so those stay implementation.
    api("io.opentelemetry:opentelemetry-api")
    api("io.opentelemetry:opentelemetry-context")
    implementation("io.opentelemetry:opentelemetry-sdk")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp")
    // Not in the opentelemetry-bom: semantic conventions version independently of the SDK, and
    // moved group ID from io.opentelemetry to io.opentelemetry.semconv.
    implementation("io.opentelemetry.semconv:opentelemetry-semconv:1.41.1")

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
