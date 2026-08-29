package com.eventforge.notification;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.envelope.EventEnvelope;
import com.eventforge.events.envelope.EventEnvelopeMapper;
import com.eventforge.notification.consumer.NotificationEventListener;
import com.eventforge.notification.domain.NotificationSendingService;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.support.MessageBuilder;

/**
 * Item 6: notification-service's whole justification for existing (constitution item 6) — proves
 * independent consumer-group offsets on a shared topic. {@code notification-service}'s own group
 * resets to earliest and replays the full topic while a second, independently-simulated
 * {@code payment-service} group (a plain {@link KafkaConsumer}, not the real payment-service
 * module — a different Gradle module entirely — but a faithful stand-in for "another group holding
 * its own position on this topic") holds position, unaffected.
 *
 * <p>Also demonstrates, and states plainly, what replay implies for a consumer whose effect is
 * external (constitution item 6's closing question): replay does NOT produce duplicate
 * notifications here, because {@code processed_events} dedupes the redelivered events regardless
 * of why they were redelivered — but that protection depends entirely on those dedupe rows still
 * existing, which is exactly why the retention-window ADR matters for this service specifically.
 */
@SpringBootTest
class IndependentConsumerGroupOffsetsIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    private static final String TOPIC = "orders.events";
    private static final String SIMULATED_PAYMENT_GROUP = "payment-service";
    private static final int EVENT_COUNT = 5;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    private final ObjectMapper mapper = EventEnvelopeMapper.create();

    @Test
    void replayingOneGroupDoesNotAffectAnIndependentGroupsPosition() throws Exception {
        List<String> orderIds = new ArrayList<>();
        for (int i = 0; i < EVENT_COUNT; i++) {
            String orderId = "offsets-" + UUID.randomUUID();
            orderIds.add(orderId);
            publishOrderCreated(orderId, 1000 + i);
        }

        // A second, independent group consumes and commits its own position on the same topic -
        // standing in for payment-service, which is a separate service/module, not something this
        // test can boot directly.
        try (KafkaConsumer<String, String> simulatedPaymentGroup = newConsumer(SIMULATED_PAYMENT_GROUP)) {
            simulatedPaymentGroup.subscribe(List.of(TOPIC));
            pollUntil(simulatedPaymentGroup, EVENT_COUNT, Duration.ofSeconds(30));
            simulatedPaymentGroup.commitSync();
        }

        // notification-service's real consumer processes all EVENT_COUNT events normally first.
        waitForNotificationCount(orderIds, EVENT_COUNT, Duration.ofSeconds(30));
        int sentBeforeReplay = countSentNotifications(orderIds);
        assertThat(sentBeforeReplay).isEqualTo(EVENT_COUNT);

        try (Admin admin = Admin.create(adminProps())) {
            Map<TopicPartition, Long> paymentOffsetsBeforeReplay = committedOffsets(admin, SIMULATED_PAYMENT_GROUP);
            Map<TopicPartition, Long> notificationOffsetsBeforeReset = committedOffsets(admin, NotificationSendingService.CONSUMER_GROUP);

            // Reset notification-service's group to earliest and replay the whole topic - the
            // container must be stopped first so the group has no active member while its offsets
            // are altered.
            MessageListenerContainer container = registry.getListenerContainer(NotificationEventListener.LISTENER_ID);
            container.stop();
            resetGroupToEarliest(admin, NotificationSendingService.CONSUMER_GROUP, notificationOffsetsBeforeReset.keySet());
            container.start();

            // Replay completeness: the group's committed offsets climb back to at least where
            // they were before the reset - proof it re-read every message from position 0, not
            // just proof the table didn't change (which dedupe would mask either way).
            waitForOffsetsToCatchUp(admin, NotificationSendingService.CONSUMER_GROUP, notificationOffsetsBeforeReset, Duration.ofSeconds(30));

            // Non-interference: the independent group's own committed position never moved.
            Map<TopicPartition, Long> paymentOffsetsAfterReplay = committedOffsets(admin, SIMULATED_PAYMENT_GROUP);
            assertThat(paymentOffsetsAfterReplay).isEqualTo(paymentOffsetsBeforeReplay);
        }

        // What replay implies for an external effect: no duplicate notifications, because
        // processed_events dedupes the redelivered events regardless of why they were redelivered
        // - but see the retention-window ADR for the boundary of that protection.
        int sentAfterReplay = countSentNotifications(orderIds);
        assertThat(sentAfterReplay).isEqualTo(sentBeforeReplay);
    }

    private void publishOrderCreated(String orderId, long amountCents) throws Exception {
        EventEnvelope envelope = new EventEnvelope(
                UUID.randomUUID(),
                "OrderCreated",
                1,
                orderId,
                UUID.randomUUID(),
                null,
                Instant.now(),
                mapper.createObjectNode().put("orderId", orderId).put("amountCents", amountCents));
        kafkaTemplate
                .send(MessageBuilder.withPayload(mapper.writeValueAsString(envelope))
                        .setHeader(KafkaHeaders.TOPIC, TOPIC)
                        .setHeader(KafkaHeaders.KEY, orderId)
                        .build())
                .get();
    }

    private int countSentNotifications(List<String> orderIds) {
        Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM sent_notifications WHERE order_id = ANY(?)",
                Integer.class,
                (Object) orderIds.toArray(new String[0]));
        return count == null ? 0 : count;
    }

    private void waitForNotificationCount(List<String> orderIds, int expected, Duration timeout) throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            if (countSentNotifications(orderIds) >= expected) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Timed out waiting for " + expected + " notifications to be sent");
    }

    private KafkaConsumer<String, String> newConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    private void pollUntil(KafkaConsumer<String, String> consumer, int minCount, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        int seen = 0;
        while (seen < minCount && System.currentTimeMillis() < deadline) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
            for (ConsumerRecord<String, String> record : records) {
                seen++;
            }
        }
        if (seen < minCount) {
            throw new AssertionError("Timed out waiting for " + minCount + " records, saw " + seen);
        }
    }

    private Properties adminProps() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        return props;
    }

    private Map<TopicPartition, Long> committedOffsets(Admin admin, String groupId) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> raw =
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata().get();
        Map<TopicPartition, Long> result = new HashMap<>();
        raw.forEach((tp, om) -> result.put(tp, om.offset()));
        return result;
    }

    private void resetGroupToEarliest(Admin admin, String groupId, java.util.Set<TopicPartition> partitions) throws Exception {
        Map<TopicPartition, OffsetAndMetadata> reset = new HashMap<>();
        for (TopicPartition tp : partitions) {
            reset.put(tp, new OffsetAndMetadata(0L));
        }
        admin.alterConsumerGroupOffsets(groupId, reset).all().get();
    }

    private void waitForOffsetsToCatchUp(
            Admin admin, String groupId, Map<TopicPartition, Long> targetOrBetter, Duration timeout) throws Exception {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            Map<TopicPartition, Long> current = committedOffsets(admin, groupId);
            boolean caughtUp = targetOrBetter.entrySet().stream()
                    .allMatch(e -> current.getOrDefault(e.getKey(), -1L) >= e.getValue());
            if (caughtUp) {
                return;
            }
            Thread.sleep(200);
        }
        throw new AssertionError("Timed out waiting for " + groupId + " to replay back to its prior committed position");
    }
}
