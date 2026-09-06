package com.eventforge.events.failure;

import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Republishes a captured failure to the topic it came from.
 *
 * <h2>Why replaying twice is safe, without anything new being built for it</h2>
 *
 * The stored payload is republished byte for byte, so the envelope's {@code eventId} is preserved.
 * The consuming service's {@code ProcessedEventStore} then does what it already does for every
 * duplicate: {@code INSERT ... ON CONFLICT (consumer_group, event_id) DO NOTHING} inside the same
 * transaction as the business write, so a second replay inserts zero rows and is a clean no-op.
 * The precondition that makes this coherent is worth stating: the message reached the failure
 * store <em>because its business transaction rolled back</em>, which rolled back its
 * {@code processed_events} row too — so the first successful replay is genuinely the first mark.
 *
 * <p>A replay that arrives too late is equally harmless, and again for a reason that already
 * exists: {@code SagaOrchestrator}'s handlers each check the state they expect before acting, so
 * a {@code PaymentAuthorized} replayed into a saga that has already completed is logged and
 * ignored rather than double-applied.
 *
 * <h2>Ordering</h2>
 *
 * Claim and mark are two transactions with the publish between them, exactly as
 * {@code OutboxRelayWorker} does it. A crash after the broker acknowledges but before the row is
 * marked leaves it {@code REPLAY_REQUESTED}, so it can be replayed again — producing a duplicate
 * delivery, which is the case the paragraph above makes harmless. The alternative ordering (mark
 * first, then publish) would risk the opposite and much worse outcome: a row marked replayed
 * whose message was never actually sent.
 */
public class FailedMessageReplayer {

    private final FailedMessageStore store;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final TransactionTemplate transactionTemplate;
    private final Duration sendTimeout;
    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    public FailedMessageReplayer(
            FailedMessageStore store,
            KafkaTemplate<String, String> kafkaTemplate,
            PlatformTransactionManager transactionManager,
            Duration sendTimeout) {
        this.store = store;
        this.kafkaTemplate = kafkaTemplate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.sendTimeout = sendTimeout;
    }

    /**
     * @return the outcome, so a caller can distinguish "nothing to replay" from "published" without
     *     relying on exceptions for ordinary control flow.
     */
    public ReplayOutcome replay(UUID failedMessageId) {
        Optional<FailedMessageRow> claimed =
                transactionTemplate.execute(status -> store.claimForReplay(failedMessageId));

        if (claimed == null || claimed.isEmpty()) {
            return ReplayOutcome.NOT_CLAIMABLE;
        }
        FailedMessageRow row = claimed.get();

        try {
            kafkaTemplate.send(toRecord(row)).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            recordFailure(failedMessageId, e);
            return ReplayOutcome.PUBLISH_FAILED;
        } catch (Exception e) {
            recordFailure(failedMessageId, e);
            return ReplayOutcome.PUBLISH_FAILED;
        }

        transactionTemplate.executeWithoutResult(status -> store.markReplayed(failedMessageId));
        return ReplayOutcome.REPLAYED;
    }

    /**
     * The original topic, key, payload and headers — nothing regenerated. Minting a fresh
     * {@code eventId} here would defeat the downstream dedupe that makes a repeated replay safe.
     */
    private ProducerRecord<String, String> toRecord(FailedMessageRow row) {
        ProducerRecord<String, String> record =
                new ProducerRecord<>(row.topic(), row.messageKey(), row.payload());
        if (row.headersJson() != null && !row.headersJson().isBlank()) {
            try {
                JsonNode headers = mapper.readTree(row.headersJson());
                headers.properties().forEach(field -> {
                    if (!field.getValue().isNull()) {
                        record.headers()
                                .add(field.getKey(), field.getValue().asText().getBytes(StandardCharsets.UTF_8));
                    }
                });
            } catch (Exception e) {
                throw new IllegalStateException("Stored headers for " + row.failedMessageId() + " are unreadable", e);
            }
        }
        return record;
    }

    private void recordFailure(UUID failedMessageId, Exception e) {
        String message = e.getClass().getName() + ": " + e.getMessage();
        transactionTemplate.executeWithoutResult(status -> store.markReplayFailed(failedMessageId, message));
    }

    /** What a replay attempt did. */
    public enum ReplayOutcome {
        /** Published to the original topic and marked. */
        REPLAYED,
        /** No such row, or it is already replayed, abandoned, or being claimed concurrently. */
        NOT_CLAIMABLE,
        /** Claimed, but the broker did not accept it; the row stays eligible for another attempt. */
        PUBLISH_FAILED
    }
}
