package com.eventforge.events.tracing;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;
import io.opentelemetry.context.propagation.TextMapPropagator;
import io.opentelemetry.context.propagation.TextMapSetter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.header.Headers;

/**
 * The one seam every write/relay/consume call site in this project uses for tracing — not a raw
 * passthrough of the OTel API, but the specific handful of operations constitution M4 items 2-4
 * name explicitly. Replaces M1's hand-rolled {@code TraceContextCapture} (ADR-0011): the stored
 * {@code traceparent}/{@code tracestate} column format is unchanged, only how those strings are
 * produced changes (a real W3C propagator instead of string surgery).
 *
 * <p>Three shapes of span, matching the three places a trace can begin or continue in this system:
 *
 * <ul>
 *   <li>{@link #startServerSpan} — HTTP entry (order-service's {@code POST /orders}). Extracts an
 *       inbound HTTP {@code traceparent}/{@code tracestate} if present, continuing that trace;
 *       starts a fresh one otherwise.
 *   <li>{@link #startConsumerSpan} — Kafka consumer entry. Extracts from the consumed record's
 *       headers, always continuing the publisher's trace (the relay never sends without a stored
 *       context — see {@link #startRelayPublishSpan}).
 *   <li>{@link #startRelayPublishSpan} — the relay's own publish, extracting the context stored on
 *       the outbox row (not whatever happens to be "current" on the relay's polling thread — trap
 *       T5) and starting a CHILD span under it, which is then injected into the outgoing Kafka
 *       record. This is what makes the relay a real hop in the trace instead of an invisible one
 *       (constitution item 3) — see ADR-0005's M4 note.
 * </ul>
 *
 * <p>{@link #captureCurrentContext()} is the write-side operation (constitution item 2): whatever
 * span is ACTIVE (current) on the calling thread — the server span for order-service's own write,
 * or the consumer span for every other service's next-outbox-write — gets injected into a durable
 * pair of strings for the outbox columns. It never fabricates a context and never reaches into the
 * relay's own context, because it only ever reads {@link Context#current()}.
 */
public class EventForgeTracer {

    private static final TextMapSetter<Map<String, String>> MAP_SETTER = Map::put;

    private static final TextMapGetter<Map<String, String>> MAP_GETTER = new TextMapGetter<>() {
        @Override
        public Iterable<String> keys(Map<String, String> carrier) {
            return carrier.keySet();
        }

        @Override
        public String get(Map<String, String> carrier, String key) {
            return carrier == null ? null : carrier.get(key);
        }
    };

    private static final TextMapSetter<Headers> KAFKA_HEADERS_SETTER =
            (headers, key, value) -> headers.add(key, value.getBytes(StandardCharsets.UTF_8));

    private final Tracer tracer;
    private final TextMapPropagator propagator;

    public EventForgeTracer(Tracer tracer, TextMapPropagator propagator) {
        this.tracer = tracer;
        this.propagator = propagator;
    }

    /** HTTP entry point: continue an inbound trace if present, otherwise start a new one. */
    public SpanHandle startServerSpan(String name, String inboundTraceparent, String inboundTracestate) {
        Context parent = extract(inboundTraceparent, inboundTracestate);
        Span span = tracer.spanBuilder(name).setParent(parent).setSpanKind(SpanKind.SERVER).startSpan();
        return SpanHandle.open(span);
    }

    /** Kafka consumer entry point: continue the trace carried in the consumed record's headers. */
    public SpanHandle startConsumerSpan(String name, String inboundTraceparent, String inboundTracestate) {
        Context parent = extract(inboundTraceparent, inboundTracestate);
        Span span = tracer.spanBuilder(name).setParent(parent).setSpanKind(SpanKind.CONSUMER).startSpan();
        return SpanHandle.open(span);
    }

    /**
     * A clock-driven dispatch with no inbound message to continue (the saga timeout sweep) — no
     * parent context exists, so this deliberately starts a brand-new trace rather than pretending
     * one exists.
     */
    public SpanHandle startRootSpan(String name, SpanKind kind) {
        Span span = tracer.spanBuilder(name).setNoParent().setSpanKind(kind).startSpan();
        return SpanHandle.open(span);
    }

    /**
     * The relay's publish span: extracts the context STORED on the outbox row as parent (never the
     * relay polling loop's own ambient context — trap T5) and starts a CHILD span under it. Inject
     * this span's own context into the outgoing record via {@link #injectCurrentContextIntoKafkaHeaders}
     * while this handle is open — that is what makes the relay a real, visible hop rather than a
     * verbatim header copy (constitution item 3).
     */
    public SpanHandle startRelayPublishSpan(String name, String storedTraceparent, String storedTracestate) {
        Context parent = extract(storedTraceparent, storedTracestate);
        Span span = tracer.spanBuilder(name).setParent(parent).setSpanKind(SpanKind.PRODUCER).startSpan();
        return SpanHandle.open(span);
    }

    /** Write-side capture (constitution item 2): the ACTIVE context, injected into durable strings. */
    public CapturedContext captureCurrentContext() {
        Map<String, String> carrier = new HashMap<>();
        propagator.inject(Context.current(), carrier, MAP_SETTER);
        return new CapturedContext(carrier.get("traceparent"), carrier.get("tracestate"));
    }

    /** Injects whatever span is currently open (a {@link SpanHandle} from this class) into Kafka headers. */
    public void injectCurrentContextIntoKafkaHeaders(Headers headers) {
        propagator.inject(Context.current(), headers, KAFKA_HEADERS_SETTER);
    }

    private Context extract(String traceparent, String tracestate) {
        Map<String, String> carrier = new HashMap<>();
        if (traceparent != null) {
            carrier.put("traceparent", traceparent);
        }
        if (tracestate != null) {
            carrier.put("tracestate", tracestate);
        }
        return propagator.extract(Context.root(), carrier, MAP_GETTER);
    }

    public record CapturedContext(String traceparent, String tracestate) {}
}
