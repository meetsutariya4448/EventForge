# ADR-0007: No Hibernate `ddl-auto` anywhere, including tests — Flyway-only, JPA deferred

## Context

R3 of the project's hard rules: no Hibernate `ddl-auto` anywhere, tests included; Flyway
migrations only. R4 additionally requires hand-written SQL (via `JdbcTemplate`) wherever database
semantics are the point — outbox claiming, dedupe inserts, locking, constraint behavior — rather
than hiding that behind an ORM. This needs to be true structurally, not just as a discipline
developers have to remember.

## Options considered

**Include `spring-boot-starter-data-jpa` now, with `ddl-auto=validate` (or `none`) explicitly set
everywhere, including test configuration.** Works, but it's a rule someone has to remember to keep
true in every `application.yml` and every test properties file — one accidental
`ddl-auto=update` in a test profile silently reintroduces the exact thing R3 forbids, and it would
be easy to miss in review since it only shows up in configuration, not code.

**Chosen: don't put JPA/Hibernate on the classpath at all in M0.** There is no business entity yet
— no `Order`, no `Payment`, nothing an ORM would map. `outbox_events` and `processed_events` are
accessed via `JdbcTemplate` with hand-written SQL by design (R4), which is exactly the access
pattern M0 actually needs. Since Hibernate isn't present, there's no `ddl-auto` setting to get
wrong, in any environment, structurally — not because everyone remembered to set it correctly.

## Decision

None of the four services depend on `spring-boot-starter-data-jpa` in M0. Services get
`spring-boot-starter-web`, `spring-boot-starter-actuator`, `spring-boot-starter-jdbc`,
`spring-boot-starter-kafka`, `spring-boot-starter-flyway` + `flyway-database-postgresql`, and the
`postgresql` runtime driver. `spring-boot-starter-jdbc` is what actually matters here — it's what
brings the `DataSource`/`JdbcTemplate` machinery R4 requires, independent of whether JPA is ever
added.

JPA is deferred until a milestone actually introduces a real business entity (e.g. an `Order` row
with business columns, as opposed to the outbox/dedupe infrastructure tables). When that happens,
`ddl-auto` must be explicitly `validate` or `none` in every profile, with Flyway owning all schema
changes — this ADR's decision doesn't change, only the dependency set does.

## Consequences

- Every schema change, in every environment including Testcontainers-backed integration tests,
  goes through a Flyway migration file. The M0 round-trip integration test proves this directly:
  it boots a real Testcontainers Postgres with no manual schema setup, and `order-service`'s real
  `V1`/`V2` migrations run automatically via Flyway autoconfiguration before the test touches the
  database.
- No H2 or other in-memory database appears anywhere — Testcontainers-backed real Postgres is used
  even for schema-validation tests (R5's spirit extended to schema, not just delivery/ordering
  guarantees).

## Revisit if

A future milestone introduces a genuine business entity that JPA would meaningfully simplify
mapping for — at that point, add the JPA starter with `ddl-auto` explicitly disabled everywhere,
rather than relying on JPA's mere absence.
