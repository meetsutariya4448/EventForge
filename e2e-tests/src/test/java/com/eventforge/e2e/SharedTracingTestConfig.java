package com.eventforge.e2e;

import com.eventforge.events.tracing.EventForgeTracer;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.testing.exporter.InMemorySpanExporter;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

/**
 * Unlike {@code TracingTestConfiguration} (common-testing), which gives each {@code @SpringBootTest}
 * context its OWN isolated {@link InMemorySpanExporter}, this module boots THREE independent, real
 * application contexts (order-service, payment-service, inventory-service — see
 * {@link TraceContinuityIntegrationTest}) that must all export into the SAME exporter, or there
 * would be no single place to observe the whole cross-service trace from. {@link #EXPORTER} is a
 * static field precisely so all three contexts' {@code testOpenTelemetrySdk} beans (each built from
 * a separate invocation of this same {@code @Configuration} class, passed explicitly via
 * {@code SpringApplicationBuilder.sources(...)}) reference the identical exporter instance.
 *
 * <p>Passed to {@code SpringApplicationBuilder.sources(...)}, not {@code @Import} on a
 * {@code @SpringBootTest} — these are real application contexts started imperatively, not
 * JUnit-managed test contexts, so ordinary {@code @Configuration} (not {@code @TestConfiguration})
 * is what actually gets picked up.
 */
@Configuration
public class SharedTracingTestConfig {

    static final InMemorySpanExporter EXPORTER = InMemorySpanExporter.create();

    @Bean
    @Primary
    public InMemorySpanExporter inMemorySpanExporter() {
        return EXPORTER;
    }

    @Bean(destroyMethod = "close")
    public OpenTelemetrySdk testOpenTelemetrySdk() {
        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .setSampler(Sampler.alwaysOn())
                .addSpanProcessor(SimpleSpanProcessor.create(EXPORTER))
                .build();
        return OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
                .build();
    }

    @Bean
    @Primary
    public Tracer tracer(OpenTelemetrySdk testOpenTelemetrySdk) {
        return testOpenTelemetrySdk.getTracer("eventforge-e2e-test");
    }

    @Bean
    @Primary
    public EventForgeTracer eventForgeTracer(Tracer tracer, OpenTelemetrySdk testOpenTelemetrySdk) {
        return new EventForgeTracer(tracer, testOpenTelemetrySdk.getPropagators().getTextMapPropagator());
    }
}
