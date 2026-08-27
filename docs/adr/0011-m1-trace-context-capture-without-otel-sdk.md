# ADR-0011: M1 captures W3C trace context by hand — no OpenTelemetry SDK yet

## Context

Trap T5 requires the relay to *restore* the propagation context captured at outbox-write time, not
start a fresh trace. M1's acceptance bar (per the project owner's own scheduling note) is that
outbox rows are populated with a *valid* W3C trace context and the relay restores it into Kafka
headers unchanged — not that the project has tracing visualization yet. M4 ("cross-outbox
distributed tracing") is the milestone that owns collectors, exporters, and dashboards.

## Options considered

**Pull in `spring-boot-starter-opentelemetry` now** (Spring Boot 4's bundled Micrometer
Tracing + OTel bridge starter) to get real span creation, W3C propagation, and a `Tracer` bean to
read the current context from. This is the "proper" long-term mechanism — but current Spring Boot
4.1 documentation states this starter auto-configures an OTLP span exporter that expects a
reachable collector endpoint (`management.opentelemetry.tracing.export.otlp.endpoint`); standing
one up (or even just confirming graceful no-op behavior without one) is exactly the infrastructure
work M4 is scoped to own. Pulling it into M1 risks either a real dependency on a collector that
doesn't exist yet, or quietly doing M4's job early — both are the kind of milestone-boundary creep
R1 exists to prevent.

**Chosen: hand-generate and hand-continue W3C `traceparent` values**, with no tracing SDK at all.
`TraceContextCapture` (`common-events`) implements exactly the W3C continuation rule: given an
inbound `traceparent`, keep its trace-id (the trace stays the same one) and mint a fresh span-id
for this hop; given no valid inbound context, originate a new trace entirely. `tracestate` is
passed through unchanged (the simpler, honest option — a fully spec-compliant implementation would
also let this hop update its own vendor entry, which isn't needed for M1 to prove the storage and
transport claim and would be over-engineering ahead of any real consumer of `tracestate`).

## Decision

No OpenTelemetry SDK, no exporter, no collector in M1. `TraceContextCapture.continueOrStart(...)`
is called at the one point that matters for this milestone: `OrderService.createOrder`, right
before writing the outbox row, using the inbound HTTP `traceparent`/`tracestate` headers (if any).
The relay restores the stored value verbatim into Kafka headers — copying bytes, not regenerating
them, which is what "restoration" actually means per ADR-0005.

This is format-correct and spec-compliant as far as it goes (validated by
`TraceContextCaptureTest` and the integration tests' round-trip assertions), but it is not a real
tracer: there's no span timing, no in-process context propagation across threads, no exporter, and
nothing to look at in a UI. That gap is real and is M4's to close, not M1's.

## Consequences

- `EventEnvelope` still carries no trace fields (unchanged from ADR-0005) — `traceparent`/
  `tracestate` exist only as outbox columns and Kafka headers.
- When M3 (saga orchestration) needs payment-service or inventory-service to continue a trace from
  a consumed Kafka event into their own outbox writes, the same `TraceContextCapture.continueOrStart`
  method is reusable as-is — it doesn't care whether the inbound context came from an HTTP header or
  a Kafka header, just that it's a string.
- When M4 introduces a real OTel SDK, `TraceContextCapture` either gets replaced by the SDK's own
  propagator (likely a small, mechanical swap, since the storage/transport shape doesn't change)
  or continues to exist alongside it for services that don't need full span export — that decision
  belongs to M4.

## Revisit if

M4 lands and needs `TraceContextCapture`'s call sites for real span creation instead of string
manipulation — expected, not a surprise; this ADR's "no SDK yet" framing is explicitly temporary.
