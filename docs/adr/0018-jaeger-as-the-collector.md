# ADR-0018: Jaeger's own OTLP receiver is the "collector" — no separate otel-collector container

## Context

The M4 work order asks for services "exporting to a collector, with Jaeger (or equivalent) in
Docker Compose." Read literally, that could mean a three-tier pipeline: services → a dedicated
`otel/opentelemetry-collector` container → Jaeger. Modern Jaeger (the `jaegertracing/all-in-one`
image, 1.35+) accepts OTLP directly on its own gRPC (4317) and HTTP (4318) ports — it *is* an OTLP
collector as far as any exporter is concerned, with no separate hop needed.

## Options considered

**A standalone `otel-collector` container between the services and Jaeger**, with its own YAML
pipeline config (receivers/processors/exporters). This is the real-world production topology when
you need fan-out to multiple backends, sampling/batching decisions centralized outside every
service, or protocol translation. None of that applies here: this project has one trace backend
(Jaeger) and no fan-out requirement, so a collector in front of it would receive OTLP and forward
OTLP to Jaeger, unchanged — an extra container and an extra YAML file whose entire job is passing
bytes through.

**Chosen: services export OTLP directly to Jaeger's own receiver.** One fewer moving part in
`docker-compose.yml`, one fewer thing that can be down independently of the thing that matters
(whether traces are visible), and it's the literal architecture the work order names ("Jaeger... in
Docker Compose") without inventing infrastructure the project doesn't need — consistent with this
project's established minimalism (no Redis, no circuit breaker library, no generic resilience
dashboard).

## Decision

`docker-compose.yml`'s `jaeger` service runs `jaegertracing/all-in-one:1.62.0` with
`COLLECTOR_OTLP_ENABLED=true`, exposing 16686 (UI), 4317 (OTLP gRPC — what every service's
`OtlpGrpcSpanExporter` targets), and 4318 (OTLP HTTP, unused by this project but part of the same
receiver). `eventforge.tracing.otlp-endpoint` defaults to `http://localhost:4317` in every
service's `application.yml`.

## Consequences

- If Jaeger is down, span export fails silently in the background (`BatchSpanProcessor` logs and
  retries/drops) — it does not fail requests, does not fail the outbox write, does not fail
  anything business-critical. Tracing is observability tooling, not a dependency the system's
  correctness relies on — see ADR-0020's distinction between `correlationId` and trace context.
- Adding a real `otel-collector` later (for a second backend, or centralized sampling policy) is a
  config-only addition — every service already speaks standard OTLP; nothing about the SDK wiring
  in ADR-0017 needs to change, only the `otlp-endpoint` value and the new container.

## Revisit if

A second trace backend is ever needed simultaneously (e.g. Jaeger for local dev, a hosted vendor
for anything else), or centralized head-based sampling policy needs to live outside every service —
both are exactly the fan-out/policy-centralization problems a real `otel-collector` solves.
