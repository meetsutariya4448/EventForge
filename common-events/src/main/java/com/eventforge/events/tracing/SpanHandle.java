package com.eventforge.events.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;
import org.slf4j.MDC;

/**
 * A started span, made {@link Span#makeCurrent() current} for the duration it's open, with
 * {@code trace_id}/{@code span_id} mirrored into SLF4J's MDC so every log line emitted while this
 * handle is open carries them (constitution item 7) — without depending on Micrometer Tracing's
 * own MDC bridge, since this project wires the OTel SDK directly (see ADR-0017).
 *
 * <p>Always use in try-with-resources. {@link #recordException} before the block exits via an
 * exception, so the exported span reflects the failure — {@link #close()} alone does not infer
 * error status from a thrown exception.
 */
public final class SpanHandle implements AutoCloseable {

    private final Span span;
    private final Scope scope;

    private SpanHandle(Span span, Scope scope) {
        this.span = span;
        this.scope = scope;
    }

    static SpanHandle open(Span span) {
        Scope scope = span.makeCurrent();
        MDC.put("trace_id", span.getSpanContext().getTraceId());
        MDC.put("span_id", span.getSpanContext().getSpanId());
        return new SpanHandle(span, scope);
    }

    public Span span() {
        return span;
    }

    public void recordException(Throwable t) {
        span.recordException(t);
        span.setStatus(StatusCode.ERROR, t.getMessage() == null ? t.getClass().getSimpleName() : t.getMessage());
    }

    @Override
    public void close() {
        span.end();
        scope.close();
        MDC.remove("trace_id");
        MDC.remove("span_id");
    }
}
