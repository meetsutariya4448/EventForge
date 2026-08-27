# EventForge

Event-driven distributed transaction platform (Java 21 / Spring Boot / Kafka / PostgreSQL). See
[docs/architecture.md](docs/architecture.md) for the target architecture and
[docs/adr/](docs/adr/) for design decisions. Currently at Milestone 0 (foundation & event
contracts) — see the milestone map in the project constitution for what comes next.

## Prerequisites

- Docker (with Compose v2+; `docker compose version` should report v2 or later).

That's it. Java 21 and Gradle are self-provisioned by the Gradle wrapper on first build — you do
not need a JDK or Gradle installed beforehand. (Development was verified with Docker 29.6.2 /
Compose v5.3.1 on macOS; Gradle wrapper pins Gradle 9.7.1 and toolchains pin JDK 21.)

## Commands

```
make up      # starts Kafka (KRaft) + one Postgres per service via Docker Compose,
             # builds the 4 services, and runs them as local JVM processes,
             # waiting until each reports healthy via /actuator/health
make test    # runs the full test suite, including Testcontainers-backed integration
             # tests against real Kafka and real Postgres (independent of `make up` —
             # see docs/adr/0008-compose-scope-vs-testcontainers.md)
make down    # stops the local service processes and tears down the Compose infra
```

Service ports (local `make up`): order-service `8081`, payment-service `8082`,
inventory-service `8083`, notification-service `8084`. Each exposes `/actuator/health`.

## Modules

- `common-events` — the event envelope, fault-injection seam, and shared Jackson config.
- `common-testing` — shared Testcontainers base class and the fault-injection test double.
- `order-service`, `payment-service`, `inventory-service`, `notification-service` — Spring Boot
  services, each with its own Postgres database and Flyway migrations.
