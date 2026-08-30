package com.eventforge.testing.tracing;

import com.eventforge.events.tracing.EventForgeTracer;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

/**
 * Constitution M4 item 6: "an automated assertion, not a screenshot." Import this into a test to
 * get a real {@link EventForgeTracer} backed by a real {@link SdkTracerProvider}, exporting
 * synchronously (via {@link SimpleSpanProcessor}, not the production {@code BatchSpanProcessor})
 * to an {@link InMemorySpanExporter} the test can query directly with
 * {@code exporter.getFinishedSpanItems()} — no real Jaeger, no export delay to wait out.
 *
 * <p><b>Also set {@code eventforge.tracing.enabled=false}</b> (e.g. via
 * {@code @DynamicPropertySource}) in any test that imports this. {@code TracingAutoConfiguration}'s
 * own {@code openTelemetrySdk} bean is unconditional whenever tracing is enabled — it has no
 * {@code @ConditionalOnMissingBean} guard on itself, since it's the root everything else in that
 * class conditions on — so leaving tracing enabled here would construct a second, real SDK
 * instance alongside this one, pointlessly attempting to export to an OTLP endpoint nothing is
 * listening on in a test. Every other bean in this class ({@link Tracer}, {@link EventForgeTracer})
 * is genuinely {@code @Primary} and correctly overrides its production counterpart regardless.
 */
@TestConfiguration
public class TracingTestConfiguration {

    @Bean
    @Primary
    public InMemorySpanExporter inMemorySpanExporter() {
        return InMemorySpanExporter.create();
    }

    @Bean(destroyMethod = "close")
    public OpenTelemetrySdk testOpenTelemetrySdk(InMemorySpanExporter exporter) {
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                // ParentBased(alwaysOn), not a bare alwaysOn(): a bare AlwaysOnSampler ignores
                // ANY parent context's sampled flag and forces every span to sampled=true, which
                // silently defeats SamplingPropagationIntegrationTest's whole premise (a
                // not-sampled inbound traceparent must stay not-sampled all the way through the
                // outbox and the relay — constitution item 8; see ADR-0019). ParentBased still
                // samples every ROOT span unconditionally (the alwaysOn() delegate applies exactly
                // when there's no valid parent to defer to), which is what every OTHER test
                // importing this class relies on for full trace-structure recording — only a span
                // with a real propagated parent now defers to that parent's own decision.
                .setSampler(Sampler.parentBased(Sampler.alwaysOn()))
                .addSpanProcessor(SimpleSpanProcessor.create(exporter))
                .build();
        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }

    @Bean
    @Primary
    public Tracer tracer(OpenTelemetrySdk testOpenTelemetrySdk) {
        return testOpenTelemetrySdk.getTracer("eventforge-test");
    }

    @Bean
    @Primary
    public EventForgeTracer eventForgeTracer(Tracer tracer, OpenTelemetrySdk testOpenTelemetrySdk) {
        return new EventForgeTracer(tracer, testOpenTelemetrySdk.getPropagators().getTextMapPropagator());
    }
}
