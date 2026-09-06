package com.eventforge.events.failure;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.UUID;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.listener.ConsumerAwareRecordRecoverer;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Captures a record that exhausted its retries, durably, before the consumer offset is allowed to
 * move past it.
 *
 * <p>This replaces log-and-skip. Previously such a record produced an ERROR line and was gone;
 * now it is a row an operator can find and replay.
 *
 * <h2>The ordering, and why it is this way round</h2>
 *
 * {@link #accept} writes the capture row and commits it, and only then returns. Spring Kafka
 * commits the offset after the recoverer returns — so the durable record always precedes the
 * offset advance. If the capture throws, the exception propagates out of {@code accept}, the
 * record is not treated as recovered, and the offset is not committed: the partition stalls
 * loudly and the record is redelivered, rather than being skipped with nothing remembering it.
 * Stalling is the correct failure direction here; silently losing the message is not.
 *
 * <p>That framework behaviour is not assumed. {@code RecovererFailureLeavesOffsetUncommitted
 * IntegrationTest} measured it before this class existed: with a positive control proving the
 * group does commit on that partition, a throwing recoverer left the committed offset pointing
 * at the failed record rather than past it, and the record was redelivered.
 *
 * <p>The capture runs in its own {@code REQUIRES_NEW} transaction because the listener's
 * transaction has already rolled back by the time an error handler runs — there is nothing left
 * to join.
 *
 * <p>Composes {@link com.eventforge.events.consumer.LoggingConsumerRecordRecoverer}'s behaviour
 * rather than replacing it: the ERROR line and the counter are still what an operator and the
 * existing poison-message test respectively rely on.
 *
 * <h2>Where the consumer group comes from</h2>
 *
 * A {@link ConsumerAwareRecordRecoverer}, so {@code DefaultErrorHandler} hands over the
 * {@link Consumer} that actually failed and the group is read from its own
 * {@code groupMetadata()}. The alternative — a configured group name — could silently drift from
 * the {@code @KafkaListener}'s {@code groupId}, and the group is not decoration here: it is half
 * of the {@code processed_events} dedupe key {@code (consumer_group, event_id)} that makes a
 * replay safe, and it is what tells an operator which service's failure they are looking at. A
 * configured fallback covers only the case where Spring Kafka passes no consumer.
 */
public class DurableFailureRecoverer implements ConsumerAwareRecordRecoverer {

    private final FailedMessageStore store;
    private final TransactionTemplate transactionTemplate;
    private final FaultInjector faultInjector;
    private final Clock clock;
    private final String fallbackConsumerGroup;
    private final ConsumerRecordRecoverer delegate;
    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    public DurableFailureRecoverer(
            FailedMessageStore store,
            PlatformTransactionManager transactionManager,
            FaultInjector faultInjector,
            Clock clock,
            String fallbackConsumerGroup,
            ConsumerRecordRecoverer delegate) {
        this.store = store;
        this.faultInjector = faultInjector;
        this.clock = clock;
        this.fallbackConsumerGroup = fallbackConsumerGroup;
        this.delegate = delegate;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    @Override
    public void accept(ConsumerRecord<?, ?> record, Consumer<?, ?> consumer, Exception exception) {
        faultInjector.inject(FaultInjectionPoint.BEFORE_FAILURE_CAPTURE);

        // Committed here, before this method returns and therefore before the offset advances.
        String consumerGroup = groupOf(consumer);
        transactionTemplate.executeWithoutResult(status -> store.capture(toRow(record, consumerGroup, exception)));

        faultInjector.inject(FaultInjectionPoint.AFTER_FAILURE_CAPTURE_BEFORE_OFFSET_ADVANCE);

        delegate.accept(record, exception);
    }

    private String groupOf(Consumer<?, ?> consumer) {
        if (consumer == null) {
            return fallbackConsumerGroup;
        }
        try {
            String groupId = consumer.groupMetadata().groupId();
            return groupId == null || groupId.isBlank() ? fallbackConsumerGroup : groupId;
        } catch (Exception e) {
            return fallbackConsumerGroup;
        }
    }

    private FailedMessageRow toRow(ConsumerRecord<?, ?> record, String consumerGroup, Exception exception) {
        String payload = record.value() == null ? "" : record.value().toString();
        UUID eventId = null;
        String eventType = null;
        try {
            EventEnvelope envelope = mapper.readValue(payload, EventEnvelope.class);
            eventId = envelope.eventId();
            eventType = envelope.eventType();
        } catch (Exception ignored) {
            // Expected, not exceptional: an unparseable record is one of the main reasons a
            // message ends up here at all. It is still captured — identified by its physical
            // coordinates rather than by an event id it does not have.
        }

        return new FailedMessageRow(
                UUID.randomUUID(),
                consumerGroup,
                record.topic(),
                record.partition(),
                record.offset(),
                record.key() == null ? null : record.key().toString(),
                payload,
                headersAsJson(record),
                eventId,
                eventType,
                describe(exception),
                FailedMessageStatus.CAPTURED,
                clock.instant(),
                0,
                null,
                null);
    }

    /** Headers preserved verbatim so a replay carries the original traceparent, not a new one. */
    private String headersAsJson(ConsumerRecord<?, ?> record) {
        ObjectNode node = mapper.createObjectNode();
        for (Header header : record.headers()) {
            node.put(header.key(), header.value() == null ? null : new String(header.value(), StandardCharsets.UTF_8));
        }
        try {
            return mapper.writeValueAsString(node);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to serialize Kafka headers for capture", e);
        }
    }

    private static String describe(Exception exception) {
        Throwable root = exception;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }
        return exception.getClass().getName() + ": " + exception.getMessage() + " (root cause: "
                + root.getClass().getName() + ": " + root.getMessage() + ")";
    }
}
