# ADR-0009: Dependency versions, and Spring Boot 4's per-technology starter modularization

## Context

R10 requires verifying current library/platform behavior rather than recalling it. Pinning
versions for a brand-new repo touches Spring Boot, Gradle, Testcontainers, and the Kafka Docker
image — all of which had moved since general training-data-era knowledge, in ways that broke the
build until diagnosed. This ADR records what was verified and why, so later milestones don't
silently regress onto stale assumptions when adding new dependencies.

## Versions pinned (verified against current documentation/Maven Central during M0, August 2026)

- **Spring Boot 4.1.1** (current stable; Spring Boot 4 requires Java 17 minimum, with first-class
  support through Java 25 — Java 21 is fully supported).
- **Gradle 9.7.1** (current stable).
- **Testcontainers 2.0.5** (current stable; matches the version Spring Boot 4.1.1's own reference
  docs use in their `@ServiceConnection` examples).
- **`apache/kafka:4.3.1`** Docker image (current stable; native KRaft support, no ZooKeeper mode
  exists in this image at all).

## Non-obvious finding: Spring Boot 4 split per-technology autoconfiguration into separate starters

Spring Boot 4 modularized what used to be one large `spring-boot-autoconfigure` jar into focused,
per-technology modules. Concretely, for a service that needs Kafka and Flyway:

- Adding `org.springframework.kafka:spring-kafka` directly (the pre-Boot-4 pattern) is **not
  enough** to get Spring Boot's Kafka autoconfiguration or its Testcontainers
  `@ServiceConnection` support — `spring-boot-autoconfigure-4.1.1.jar` has no `kafka` package at
  all anymore, and `spring-boot-testcontainers-4.1.1.jar` ships no per-technology connection-details
  factories (not even Postgres's). The correct dependency is
  **`org.springframework.boot:spring-boot-starter-kafka`** (test equivalent:
  **`spring-boot-starter-kafka-test`**), which pulls in the separate `spring-boot-kafka` module
  (`org.springframework.boot.kafka` package) that actually contains `KafkaAutoConfiguration` and
  the Kafka `ConnectionDetailsFactory`.
- The same pattern applies to Flyway: `org.flywaydb:flyway-database-postgresql` alone does not
  trigger Spring Boot's Flyway autoconfiguration in Boot 4. The correct dependency is
  **`org.springframework.boot:spring-boot-starter-flyway`**, alongside the Flyway driver artifact.
- Confirmed the inverse is *not* true for JDBC/Postgres: `spring-boot-starter-jdbc` +
  `spring-boot-testcontainers` + `org.testcontainers:testcontainers-postgresql` was sufficient for
  `@ServiceConnection` to wire up Postgres correctly — no extra Boot-side starter was needed there.
- This was discovered empirically: using the pre-Boot-4 dependency pattern compiled cleanly but
  failed at runtime with `ConnectionDetailsNotFoundException: No ConnectionDetails found for
  source '...kafka'` (Kafka) and `BadSqlGrammarException: relation "outbox_events" does not exist`
  (Flyway migrations silently never ran). Both were root-caused by inspecting the actual jar
  contents (`unzip -l`) rather than assuming the old artifact coordinates still worked, then
  confirmed against the Spring Boot 4.0 Migration Guide's starter-mapping table.

## Non-obvious finding: Testcontainers 2.x renamed module artifacts

Testcontainers 2.x prefixes module artifact IDs with `testcontainers-`: `org.testcontainers:kafka`
became `org.testcontainers:testcontainers-kafka`, `org.testcontainers:postgresql` became
`org.testcontainers:testcontainers-postgresql`, `org.testcontainers:junit-jupiter` became
`org.testcontainers:testcontainers-junit-jupiter`. The old unprefixed artifact IDs still exist on
Maven Central but are stuck at the 1.21.4 (1.x) line — depending on them alongside a 2.x BOM
produces a confusing "could not find :.'" (empty version) resolution failure rather than a clear
"wrong artifact" error, because the BOM has no version constraint for an artifact ID it doesn't
recognize.

Also: `org.testcontainers.kafka.KafkaContainer` and `org.testcontainers.postgresql.PostgreSQLContainer`
(the 2.x classes, distinct from the deprecated 1.x classes under `org.testcontainers.containers`)
are **not** generic — `new PostgreSQLContainer<>(...)` with the diamond operator no longer
compiles; it's `new PostgreSQLContainer(...)`.

## Non-obvious finding: `spring-boot-starter` alone does not include an embedded web server

A service depending on `spring-boot-starter` (rather than `spring-boot-starter-web`) has no
Tomcat/embedded server, so Spring Boot infers `WebApplicationType.NONE`. Such a service starts,
finishes context initialization, and then — with no non-daemon thread keeping the JVM alive —
exits immediately after `main()` returns, seconds after logging `Started ... Application`. This
surfaced as services failing local health-check polling with no error in their logs, only a
`Shutdown initiated` line moments after `Started`. Fixed by depending on
`spring-boot-starter-web` (which transitively includes `spring-boot-starter`) in every service.

## Non-obvious finding: the `apache/kafka` Docker image's own bin scripts aren't on `PATH`

`KAFKA_HOME=/opt/kafka` is set, but `/opt/kafka/bin` is not appended to `PATH` in this image
version — `kafka-broker-api-versions.sh` (used for the Compose healthcheck) must be invoked by its
full path, `/opt/kafka/bin/kafka-broker-api-versions.sh`, not assumed to be resolvable bare.
Verified directly via `docker exec ... which kafka-broker-api-versions.sh` (not found) vs.
`docker exec ... find / -iname kafka-broker-api-versions.sh` (found at the full path).

## Non-obvious finding (M1): `@ConditionalOnBean` in auto-configuration needs `@AutoConfigureAfter`

`OutboxWriterAutoConfiguration`'s bean method was gated with `@ConditionalOnBean(JdbcTemplate.class)`
with no ordering hint. This is a documented Spring Boot footgun, not a Boot-4-specific change: Boot
doesn't guarantee one auto-configuration class runs after another just because the second depends
on a bean the first would create, so `@ConditionalOnBean` can evaluate before the target bean's
definition exists — silently skipping the conditional bean rather than erroring. Symptom was a
`NoSuchBeanDefinitionException: No qualifying bean of type OutboxWriter` at a completely unrelated
injection point, with no indication the real cause was condition-ordering. Fixed by adding
`@AutoConfiguration(after = JdbcTemplateAutoConfiguration.class)` alongside the class-level
`@ConditionalOnBean`. `OutboxRelayAutoConfiguration` didn't need this fix: its `@Bean` methods take
`JdbcTemplate`/`KafkaTemplate` as plain constructor-style parameters (resolved at instantiation
time, after all definitions are registered), not as `@ConditionalOnBean` conditions (resolved
early, at definition-registration time) — the two mechanisms have different timing, which is easy
to conflate.

## Non-obvious finding (M1): Spring Boot 4's default Jackson auto-configuration targets Jackson 3

`spring-boot-jackson:4.1.1` (the module behind `JacksonAutoConfiguration`) depends on
`tools.jackson.core:jackson-databind` (Jackson 3's relocated package), not
`com.fasterxml.jackson.core:jackson-databind` (Jackson 2, "classic" Jackson — still the version
`common-events`' `EventEnvelope` contract is built on, and still present on the classpath as a
direct dependency). The practical effect: there is no Spring-managed
`com.fasterxml.jackson.databind.ObjectMapper` bean to `@Autowired` by default in this project's
Boot 4.1.1 setup, even with `spring-boot-starter-web` present. Surfaced as
`NoSuchBeanDefinitionException: No qualifying bean of type 'com.fasterxml.jackson.databind.ObjectMapper'`
when `OrderService` tried to inject one. Fixed by not depending on an ambient Spring bean at all —
every class that needs to (de)serialize an `EventEnvelope` or its payload constructs its own mapper
via `EventEnvelopeMapper.create()`, which was already the pattern `OutboxRelayWorker` and the M0
round-trip test used. This is arguably the more correct design regardless of the Jackson-3 finding:
the event contract's serialization rules (ignore-unknown-properties, `JavaTimeModule`) shouldn't
depend on whatever a generic autowired mapper happens to be configured with.

## Non-obvious finding (M1): `MockMvc`/`@AutoConfigureMockMvc` moved out of `spring-boot-starter-test`

Testing a `@RestController` with `MockMvc` used to need only `spring-boot-starter-test`. In Boot
4.1.1, `spring-boot-starter-test`'s POM no longer pulls in anything containing `MockMvc` or
`@AutoConfigureMockMvc` — both now live in a dedicated `spring-boot-webmvc-test` module
(`org.springframework.boot.webmvc.test.autoconfigure` package). Also tried `TestRestTemplate` first
(the historical alternative for full-stack HTTP tests); it isn't present in any currently-resolved
Spring Boot 4.1.1 jar either, and a reasonably thorough search didn't turn up which module (if any)
still ships it — `MockMvc` via `spring-boot-webmvc-test` was confirmed working and used instead.
Fixed by adding `testImplementation("org.springframework.boot:spring-boot-webmvc-test")` explicitly.

## Decision

Pin the versions listed above; use the per-technology Boot 4 starters
(`spring-boot-starter-kafka`, `spring-boot-starter-kafka-test`, `spring-boot-starter-flyway`,
`spring-boot-starter-web`) rather than the pre-Boot-4 raw-library pattern; use the `testcontainers-`
prefixed 2.x artifact IDs; use full paths for Docker healthcheck commands rather than assuming
image `PATH` contents.

## Consequences

Every future milestone adding a new Spring Boot technology (e.g. a future `spring-boot-starter-*`
for something not yet used) should assume the same modularized pattern applies — check for a
dedicated starter first, and verify empirically (does `@ServiceConnection`/autoconfiguration
actually activate at runtime, not just "does it compile") rather than carrying forward pre-Boot-4
dependency habits.

## Revisit if

Spring Boot or Testcontainers ship a compatibility shim that restores the old artifact/behavior,
making this note obsolete for new code (unlikely to be worth un-documenting even so, since old
services would still need to be aware of which pattern they were built under).
