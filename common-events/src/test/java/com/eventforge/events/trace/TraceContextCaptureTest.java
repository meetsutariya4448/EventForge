package com.eventforge.events.trace;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class TraceContextCaptureTest {

    @Test
    void startsANewTraceWhenNoInboundContext() {
        String traceparent = TraceContextCapture.continueOrStart(null);

        assertThat(TraceContextCapture.isValid(traceparent)).isTrue();
    }

    @Test
    void startsANewTraceWhenInboundContextIsMalformed() {
        String traceparent = TraceContextCapture.continueOrStart("not-a-real-traceparent");

        assertThat(TraceContextCapture.isValid(traceparent)).isTrue();
    }

    @Test
    void continuesTheSameTraceIdButMintsAFreshSpanId() {
        String inbound = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

        String continued = TraceContextCapture.continueOrStart(inbound);

        assertThat(TraceContextCapture.isValid(continued)).isTrue();
        assertThat(continued).startsWith("00-4bf92f3577b34da6a3ce929d0e0e4736-");
        assertThat(continued).doesNotContain("00f067aa0ba902b7");
        assertThat(continued).endsWith("-01");
    }

    @Test
    void twoIndependentCallsNeverProduceTheSameTraceId() {
        String first = TraceContextCapture.continueOrStart(null);
        String second = TraceContextCapture.continueOrStart(null);

        assertThat(first).isNotEqualTo(second);
    }
}
