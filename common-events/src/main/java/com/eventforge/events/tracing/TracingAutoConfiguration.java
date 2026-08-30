package com.eventforge.events.tracing;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import io.opentelemetry.semconv.ServiceAttributes;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.env.Environment;

/**
 * Builds the OTel SDK by hand rather than via {@code spring-boot-starter-opentelemetry}
 * (ADR-0017): this project's own tracing seams (write-side capture into DB columns, the relay's
 * extract-then-child-span-then-inject, consumer extract-then-continue) all need direct access to
 * a {@link Tracer} and {@link TextMapPropagator}, which the starter's auto-instrumentation doesn't
 * expose as cleanly, and manual wiring works identically for HTTP services and Kafka-only
 * services (payment-service, inventory-service have no inbound HTTP request to auto-instrument).
 *
 * <p>Sampler is {@link Sampler#parentBased} over a ratio sampler (constitution item 8, ADR-0019):
 * a child span always respects whatever sampling decision its parent already made — critical for
 * "a sampling decision made at the HTTP entry point propagates correctly through the outbox and
 * relay" to actually hold, since the decision is encoded in the propagated {@code traceparent}'s
 * flags byte and every downstream span must honor it, not re-roll its own.
 */
@AutoConfiguration
@EnableConfigurationProperties(TracingProperties.class)
public class TracingAutoConfiguration {

    @Bean(destroyMethod = "close")
    @ConditionalOnProperty(prefix = "eventforge.tracing", name = "enabled", havingValue = "true", matchIfMissing = true)
    public OpenTelemetrySdk openTelemetrySdk(TracingProperties properties, Environment environment) {
        String serviceName = properties.serviceName() != null
                ? properties.serviceName()
                : environment.getProperty("spring.application.name", "eventforge-service");

        Resource resource =
                Resource.getDefault().toBuilder().put(ServiceAttributes.SERVICE_NAME, serviceName).build();

        OtlpGrpcSpanExporter exporter =
                OtlpGrpcSpanExporter.builder().setEndpoint(properties.otlpEndpoint()).build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setResource(resource)
                .setSampler(Sampler.parentBased(Sampler.traceIdRatioBased(properties.samplingProbability())))
                .addSpanProcessor(BatchSpanProcessor.builder(exporter).build())
                .build();

        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }

    @Bean
    @ConditionalOnMissingBean(OpenTelemetry.class)
    public OpenTelemetry noopOpenTelemetry() {
        return OpenTelemetry.noop();
    }

    @Bean
    @ConditionalOnMissingBean(Tracer.class)
    public Tracer tracer(OpenTelemetry openTelemetry) {
        return openTelemetry.getTracer("eventforge");
    }

    @Bean
    @ConditionalOnMissingBean(EventForgeTracer.class)
    public EventForgeTracer eventForgeTracer(Tracer tracer, OpenTelemetry openTelemetry) {
        return new EventForgeTracer(tracer, openTelemetry.getPropagators().getTextMapPropagator());
    }
}
