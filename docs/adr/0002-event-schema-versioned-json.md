# ADR-0002: Event schema evolution — versioned JSON over Avro + Schema Registry

## Context

Every service publishes and consumes `EventEnvelope`-wrapped events. Schemas will change over the
project's life (new fields on `OrderCreated`, new event types entirely). EventForge needs to be
able to *demonstrate* — not just assert — that an older consumer can still read a newer event.

## Options considered

**Avro + Confluent Schema Registry.** Strong compile-time schema enforcement, compact binary
encoding, registry-enforced compatibility modes (BACKWARD/FORWARD/FULL). But it adds a stateful
infrastructure component (the registry) that isn't otherwise part of this architecture, requires
subject registration and compatibility-mode configuration, and its compatibility guarantee is
enforced by the registry rejecting an incompatible schema at publish time — which is a different
thing to demonstrate than "here is a passing test."

**Versioned JSON**, using a `schemaVersion` field on the envelope plus a documented additive-only
evolution rule, with Jackson configured to ignore unknown properties on deserialization
(`FAIL_ON_UNKNOWN_PROPERTIES = false`). The "old consumer reads new event" claim becomes a plain
JUnit test: deserialize a JSON string containing fields the current class doesn't declare, assert
it still parses and the known fields are intact. See
`common-events/src/test/java/com/eventforge/events/envelope/EventEnvelopeCompatibilityTest.java`.

## Decision

Versioned JSON. The evolution rule: schema changes within a `schemaVersion` must be additive only
(new optional fields); anything that removes or repurposes a field bumps `schemaVersion` and is
treated as a new, separately-handled shape by consumers that care. This is weaker than a registry's
enforced compatibility modes, but it is directly testable in-process, with no additional
infrastructure — which matters more here than binary compactness, since compactness isn't one of
the four claims this project exists to prove (see Part 1 of the constitution).

## Consequences

- No Schema Registry container in `docker-compose.yml` or in Testcontainers-based tests.
- Schema compatibility is a discipline enforced by code review and the compatibility test, not by
  a registry rejecting a bad publish. A producer *can* ship a breaking change without registry
  pushback; the safety net is the test suite, not the infrastructure.
- `EventEnvelopeMapper` centralizes the `FAIL_ON_UNKNOWN_PROPERTIES = false` configuration so every
  service's (de)serialization behaves consistently without each service reconfiguring Jackson
  itself.

## Revisit if

The project needs registry-enforced compatibility gates as a selling point, or binary payload size
becomes a measured concern (e.g. once M7's performance benchmarking exists to show a number).
