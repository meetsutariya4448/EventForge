package com.eventforge.events.trace;

import java.security.SecureRandom;
import java.util.regex.Pattern;

/**
 * Generates and continues W3C Trace Context ({@code traceparent}) values by hand, without a
 * tracing SDK. M1 only needs a valid, correctly-continued trace context to store on the outbox
 * row and restore into Kafka headers (ADR-0005) — a full OpenTelemetry SDK, exporter, and
 * collector are visualization/instrumentation concerns that belong to M4, not this milestone.
 *
 * <p>Continuation rule, per the W3C spec: an inbound {@code traceparent}'s trace-id is preserved
 * (the trace stays the same one), but this hop always mints its own fresh parent-id — a span-id
 * representing this service's own processing of the request. If there is no valid inbound
 * context, this hop originates a new trace entirely.
 */
public final class TraceContextCapture {

    private static final Pattern TRACEPARENT_PATTERN =
            Pattern.compile("^00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}$");

    private static final SecureRandom RANDOM = new SecureRandom();

    private TraceContextCapture() {}

    /**
     * Continues the given inbound {@code traceparent} (reusing its trace-id, minting a new
     * parent-id for this hop) if it's valid, or originates a brand-new trace if it isn't present
     * or isn't well-formed.
     */
    public static String continueOrStart(String inboundTraceparent) {
        if (isValid(inboundTraceparent)) {
            String traceId = inboundTraceparent.substring(3, 35);
            String flags = inboundTraceparent.substring(53, 55);
            return "00-" + traceId + "-" + hex(8) + "-" + flags;
        }
        return "00-" + hex(16) + "-" + hex(8) + "-01";
    }

    public static boolean isValid(String traceparent) {
        return traceparent != null && TRACEPARENT_PATTERN.matcher(traceparent).matches();
    }

    private static String hex(int numBytes) {
        byte[] bytes = new byte[numBytes];
        RANDOM.nextBytes(bytes);
        StringBuilder sb = new StringBuilder(numBytes * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }
}
