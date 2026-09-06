package com.eventforge.events.failure;

import java.time.Instant;
import java.util.UUID;

/**
 * One captured failure, as stored.
 *
 * @param payload the original record value, byte for byte. Replay republishes exactly this, which
 *     is what keeps {@code eventId} — and therefore the consumer-side dedupe that makes a double
 *     replay harmless — intact.
 * @param headersJson the original Kafka headers as JSON, including {@code traceparent}, so a
 *     replayed message continues its original trace rather than starting a new one.
 * @param eventId null when the envelope could not be parsed, which is a common reason for a
 *     record to be here in the first place.
 */
public record FailedMessageRow(
        UUID failedMessageId,
        String consumerGroup,
        String topic,
        int partition,
        long offset,
        String messageKey,
        String payload,
        String headersJson,
        UUID eventId,
        String eventType,
        String failureReason,
        FailedMessageStatus status,
        Instant capturedAt,
        int replayAttempts,
        Instant lastReplayAt,
        String lastReplayError) {}
