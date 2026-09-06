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

// Forces a fresh JVM per test class, and therefore fresh static Testcontainers containers, since
// AbstractPostgresKafkaIntegrationTest/AbstractToxicKafkaIntegrationTest declare Postgres/Kafka as
// `protected static final` — a single field shared by every subclass in one JVM, initialized once
// and never reset between classes. With no forkEvery, all of this module's test classes ran in one
// JVM sharing one Postgres and one Kafka: leftover rows, topics, and consumer-group offsets from
// an earlier class were visible to a later one.
//
// Chosen on evidence, not by default: order-service alone failed a different Testcontainers class
// in all 3 of 3 isolated runs before this change (never the same class twice). After forkEvery = 1,
// full-suite runs went 2 green out of 3 genuinely fresh re-executions, and every recurrence of the
// previously-flaky classes (MultiWorkerRelayOrderingIntegrationTest,
// OutboxRelayCrashWindowIntegrationTest, RelayPublishSpanIntegrationTest,
// RelayUnderToxicNetworkIntegrationTest) stopped entirely. The remaining, separate failure
// (RelayAsyncGapTraceIntegrationTest) is a confirmed, different bug — a race against the
// background OutboxRelayScheduler's own first tick, not shared container state — see the README's
// "Known limitations" for the evidence and why forkEvery = 1 does not and should not fix it.
//
// Measured cost: order-service alone, shared containers (no forkEvery), averaged ~6m47s across 3
// runs; order-service alone with forkEvery = 1 ran in 7m00s — a ~13s difference, within this
// module's own run-to-run variance (the shared-container baseline itself ranged from 6m32s to
// 7m14s). Container startup (a few seconds per Postgres+Kafka pair) is not the dominant cost here;
// most of this suite's wall-clock time is inside the tests themselves (broker pauses, real sleeps,
// concurrent operations), so paying it once per class instead of once per JVM barely registers.
tasks.test {
    forkEvery = 1

    // Tests run in a FORKED JVM, so a -D on the Gradle command line reaches the daemon and not the
    // test — OpenApiSpecSnapshotTest silently kept asserting instead of regenerating until this
    // was forwarded explicitly. Declared as an input so changing it re-runs the task rather than
    // being served an UP-TO-DATE result that ignored the flag.
    val updateOpenApiSpec = providers.systemProperty("updateOpenApiSpec").orElse("false")
    inputs.property("updateOpenApiSpec", updateOpenApiSpec)
    systemProperty("updateOpenApiSpec", updateOpenApiSpec.get())
}
