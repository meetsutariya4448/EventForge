# ADR-0017: OpenTelemetry SDK wired by hand, not via spring-boot-starter-opentelemetry

## Context

M4 replaces M1's hand-rolled `TraceContextCapture` (ADR-0011) with real OpenTelemetry. Spring Boot
4 ships `spring-boot-starter-opentelemetry`, which auto-instruments Spring MVC/Kafka and exposes
configuration under `management.opentelemetry.*` / `management.tracing.*`. The alternative is
building the SDK by hand (`OpenTelemetrySdk.builder()`, an explicit `SdkTracerProvider`, an
explicit `OtlpGrpcSpanExporter`) and exposing `Tracer`/`TextMapPropagator` beans directly.

## Options considered

**`spring-boot-starter-opentelemetry`.** Gets a working tracer and OTLP export with almost no code.
But this project's tracing requirements are not "instrument the framework automatically" — they are
three specific, hand-written operations named directly in the constitution: capture the ACTIVE
context into two durable database columns at write time; on the relay, *extract* the stored
context, start a *child* span, and *inject* that child's context into Kafka headers (not a
verbatim header copy); on the consumer, extract from Kafka headers and continue. None of these are
things automatic instrumentation does for you — they need direct, predictable access to a
`Tracer` and a `TextMapPropagator`, and the starter's own auto-instrumentation wraps the SDK
behind Micrometer's `Tracer`/`Propagator` abstraction, which is a different API shape than the
Instructions describe (extract/inject, `Context`, `SpanBuilder`) and would mean translating between
two tracing abstractions for no benefit. It also biases toward Spring MVC request instrumentation,
which doesn't help payment-service or inventory-service — neither has an inbound HTTP request to
instrument; they are Kafka-only.

**Chosen: build the SDK manually** (`TracingAutoConfiguration` in `common-events`) and expose
`Tracer`/`TextMapPropagator` directly, wrapped in one purpose-built class, `EventForgeTracer`, that
exposes exactly the four operations this project needs (`captureCurrentContext`,
`startServerSpan`, `startConsumerSpan`, `startRelayPublishSpan`) rather than the full OTel API
surface. Works identically whether a service has an HTTP entry point or not — every seam is driven
by explicit method calls, not framework auto-instrumentation guessing where a trace should begin.

## Decision

`common-events` depends on `opentelemetry-api`/`opentelemetry-context` (exposed `api`, since
`EventForgeTracer`'s own public signatures use these types) and `opentelemetry-sdk`/
`opentelemetry-exporter-otlp`/`opentelemetry-semconv` (kept `implementation`, since only
`TracingAutoConfiguration` itself references SDK-concrete classes — every consuming service
interacts exclusively through `EventForgeTracer`). `TracingAutoConfiguration` is
`@ConditionalOnProperty(eventforge.tracing.enabled, matchIfMissing=true)`, falling back to
`OpenTelemetry.noop()` when disabled — the same off-by-default-in-some-contexts posture every
other auto-configuration in this project follows (`OutboxRelayAutoConfiguration`,
`FaultInjectorAutoConfiguration`).

## Consequences

- No dependency on Micrometer Tracing at all. `logging.structured.format.console=ecs` (Spring
  Boot's built-in structured logging, independent of any tracing bridge) picks up `trace_id`/
  `span_id` from SLF4J's MDC because `SpanHandle` puts them there directly — see item 7 and the
  Javadoc on `SpanHandle`.
- If a future milestone wants Spring MVC/Kafka auto-instrumentation for free (metrics, additional
  spans this project doesn't hand-write), adopting the starter later is straightforward: the
  underlying `OpenTelemetrySdk`/propagator shape is the same, only the bootstrapping bean changes.
- `opentelemetry-semconv` is not part of `opentelemetry-bom` — it versions semantic conventions
  independently and changed group ID from `io.opentelemetry` to `io.opentelemetry.semconv`; pinned
  explicitly (1.41.1) rather than assumed, per R10.

## Revisit if

This project ever needs automatic instrumentation of frameworks beyond what's hand-wired here
(e.g. HTTP client calls, JDBC statement spans) — at that point evaluate
`spring-boot-starter-opentelemetry` again for the parts genuinely worth automating, while keeping
`EventForgeTracer`'s explicit seams for the outbox/relay/consumer mechanics this ADR exists for.
