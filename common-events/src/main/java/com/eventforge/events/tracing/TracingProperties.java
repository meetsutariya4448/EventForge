package com.eventforge.events.tracing;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * M4: real OpenTelemetry SDK config, replacing M1's hand-rolled {@code TraceContextCapture}.
 *
 * <p>{@code samplingProbability} defaults to 1.0 (always sample) — this is a portfolio/demo
 * project whose whole point is having a trace to look at, not a production service under load
 * where head-based sampling trims cost. Stated as a deliberate policy, not left implicit — see
 * ADR-0019.
 */
@ConfigurationProperties(prefix = "eventforge.tracing")
public record TracingProperties(
        @DefaultValue("true") boolean enabled,
        @DefaultValue("http://localhost:4317") String otlpEndpoint,
        @DefaultValue("1.0") double samplingProbability,
        String serviceName) {}
