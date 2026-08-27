# ADR-0008: Docker Compose scope vs. Testcontainers — infra-only Compose, tests never depend on it

## Context

The M0 work order specifies Compose as "Kafka in KRaft mode, one Postgres per service, and nothing
else yet" — read literally, that excludes the four Spring Boot services themselves. But the M0
acceptance criteria require "clean clone → one command → all four services healthy against real
Kafka + Postgres," which needs the services running *somewhere*. Separately, item 7 requires a
Testcontainers-based integration test that boots real Kafka and real Postgres itself. Two different
"real infrastructure" needs, and it matters whether they're the same infrastructure or not.

## Options considered

**Compose also runs the four services as containers**, via Spring Boot's Paketo buildpacks
(`bootBuildImage`) with `depends_on: condition: service_healthy` chains. More end-to-end
"production-like," but adds buildpacks configuration and container-to-container health-check
chaining that the work order's "nothing else yet" phrasing reads as deferred, and that M0 doesn't
otherwise need.

**Chosen: Compose is infra-only** (Kafka + four Postgres containers, matching the work order
literally). The four services run as local JVM processes (`java -jar`, one per service), started
by `make up` after `docker compose up -d --wait` brings the infra up and `./gradlew bootJar` builds
the jars. `scripts/start-services.sh` launches them and polls each `/actuator/health` endpoint
until `UP`. This was raised with and confirmed by the project owner before implementation.

**Testcontainers-managed containers are a separate, ephemeral set from Compose's.** Every
integration test that extends `AbstractPostgresKafkaIntegrationTest` starts its own Postgres and
Kafka containers via the Testcontainers Java API — not the ones `docker compose` manages. This is
deliberate, not an oversight: `make test` must be runnable standalone (only the Docker *daemon*
needs to be running, not `docker-compose.yml`), and tests must never be order-dependent on `make
up` having been run first. Conflating the two would violate the spirit of R5 (real infra, not
faked) by making "real" mean "whatever happens to already be running," which is a much weaker
guarantee than "this test brings up its own known-good infrastructure every time."

## Decision

Two independent lifecycles:

1. **`docker/docker-compose.yml`** — Kafka (KRaft, `apache/kafka:4.3.1`) + `order-db`,
   `payment-db`, `inventory-db`, `notification-db` (`postgres:16-alpine`), each with a real
   healthcheck (`kafka-broker-api-versions.sh` for Kafka, `pg_isready` for Postgres) gating
   `docker compose up -d --wait`. Used by `make up`/`make down` for the local "run the actual
   services" development loop. No services, no Schema Registry, no Redis, no UI/metrics
   containers.
2. **Testcontainers**, via `common-testing`'s `AbstractPostgresKafkaIntegrationTest` — its own
   Postgres and Kafka containers, started and torn down per test JVM, with zero relationship to
   Compose. Used by `make test` (`./gradlew test`), which works with only the Docker daemon
   running and `docker-compose.yml` never started.

## Consequences

- `make test` and `make up` can be run independently, in either order, without interfering with
  each other — they use disjoint containers on disjoint ports (Compose's Postgres instances are on
  5433-5436; Testcontainers assigns its own container's mapped ports dynamically).
- No Dockerfiles or buildpacks configuration exist for the four services in M0 — they run as plain
  JVM processes locally. Packaging them as containers is a natural candidate for a later milestone
  if the project wants that, but it is out of scope here.
- Verified directly: `./gradlew test` was run successfully with the Docker daemon up but
  `docker-compose.yml` never started, confirming the independence this ADR claims.

## Revisit if

A later milestone (e.g. M6's KEDA work on `kind`) needs the services running as containers — at
that point, containerizing them is a new, explicit decision, not a quiet expansion of this
Compose file's scope.
