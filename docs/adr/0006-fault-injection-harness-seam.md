# ADR-0006: Fault-injection harness — seam placement and no-op-by-default

## Context

Later milestones need to crash a process at precise points to test transactional/delivery
guarantees under failure — after the outbox DB transaction commits but before the relay publishes
to Kafka; after Kafka publish but before the outbox row is marked published; after a consumer's
business transaction commits but before it acknowledges the Kafka offset. Retrofitting these call
sites into already-written relay/consumer code in M2 is exactly the kind of thing that's easy to
get wrong under time pressure — better to build the seam now, while there's no relay code yet to
awkwardly instrument.

The work order (item 6) assigns the whole harness to `common-testing`. But production code (the
future M1 relay, M2 consumers) needs a real `FaultInjector` bean on its runtime classpath, and
`common-testing` necessarily depends on Testcontainers and JUnit for its container base classes.

## Options considered

**Everything in `common-testing`, as literally specified.** Simplest to explain, matches the work
order's wording exactly. But it means every service would need `common-testing` — and therefore
Testcontainers and JUnit — as a `main`/runtime dependency, not just a test dependency, just to get
a working no-op `FaultInjector` bean. That leaks test-framework dependencies into what should be
production artifacts.

**Chosen: split the interface from the test double.** `FaultInjector`, `FaultInjectionPoint`, and
`NoOpFaultInjector` (plus a tiny Spring auto-configuration registering the no-op as the default
bean) live in `common-events` — already a runtime dependency of every service, with zero
test-framework dependencies of its own. Only the configurable test double
(`ConfigurableFaultInjector`) and its JUnit-facing `@TestConfiguration` live in `common-testing`.
This was raised with and confirmed by the project owner before implementation (see the M0 plan's
"ambiguities resolved" section) as a deliberate deviation from the work order's literal module
assignment.

## Decision

- `common-events`: `FaultInjectionPoint` (enum, additive-only), `FaultInjector` (interface),
  `NoOpFaultInjector` (the production default), `FaultInjectorAutoConfiguration`
  (`@ConditionalOnMissingBean` registration via
  `META-INF/spring/org.springframework.boot.autoconfigure.AutoConfiguration.imports`).
- `common-testing`: `ConfigurableFaultInjector` (a test double that runs a registered `Runnable`
  per injection point, no-op if nothing's registered for that point) and
  `FaultInjectionTestConfiguration` (a `@TestConfiguration` that overrides the no-op bean with a
  `@Primary` `ConfigurableFaultInjector` when imported into a test).

Production code will call `faultInjector.inject(FaultInjectionPoint.X)` unconditionally at each
real seam once that code exists (M1/M2) — the call is always safe because the default bean is a
no-op. M0 ships only the seam itself, proven by a fast, container-free unit test
(`ConfigurableFaultInjectorTest`) that exercises both the no-op path and the armed-action path. No
relay or consumer code is invented in M0 just to give `inject()` a real call site.

## Consequences

- Every service gets a working, harmless `FaultInjector` bean automatically via Spring Boot
  auto-configuration, with no explicit wiring required.
- A test that wants to arm a fault imports `FaultInjectionTestConfiguration`, which overrides the
  bean; it never needs to know how the no-op default is wired.
- The enum is additive-only by convention — later milestones add new `FaultInjectionPoint` values
  without breaking any existing call site or test.

## Revisit if

A future milestone needs per-point configuration (e.g. probabilistic injection, injection that
fires only after N calls) that the current `Runnable`-per-point model can't express cleanly.
