package com.eventforge.events.failure;

import com.eventforge.events.consumer.LoggingConsumerRecordRecoverer;
import com.eventforge.events.fault.FaultInjector;
import java.time.Clock;
import java.time.Duration;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnWebApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * Wires durable failure capture and replay (v2 WS3) for any service that has a {@link JdbcTemplate}
 * and Spring Kafka's listener infrastructure — which is all four services, each against its own
 * {@code failed_messages} table.
 *
 * <p>Enabled by default, unlike the outbox relay. The relay is off by default because a service
 * with nothing to publish should not poll an empty table; failure capture is the opposite case —
 * a service that consumes and does not capture silently discards poison records, so opting in
 * would be the wrong default. {@code eventforge.failure-capture.enabled=false} restores the old
 * log-and-skip behaviour exactly.
 *
 * <p>{@link DurableFailureRecoverer} is registered {@code @Primary} so
 * {@code KafkaConsumerResilienceAutoConfiguration}'s error handler picks it over the plain
 * {@link LoggingConsumerRecordRecoverer}, which stays a bean and becomes its delegate rather than
 * being replaced — the ERROR line an operator reads and the counter {@code
 * PoisonMessageIntegrationTest} asserts on both still happen.
 */
@AutoConfiguration(after = JdbcTemplateAutoConfiguration.class)
@EnableConfigurationProperties(FailureCaptureProperties.class)
@ConditionalOnClass(ConsumerRecordRecoverer.class)
@ConditionalOnBean(JdbcTemplate.class)
@ConditionalOnProperty(
        prefix = "eventforge.failure-capture",
        name = "enabled",
        havingValue = "true",
        matchIfMissing = true)
public class FailureCaptureAutoConfiguration {

    // Named distinctly from OutboxRelayAutoConfiguration's clock() so the two never collide as
    // bean definitions; whichever is evaluated first wins on type, and the other is skipped.
    @Bean
    @ConditionalOnMissingBean(Clock.class)
    public Clock failureCaptureClock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnMissingBean(FailedMessageStore.class)
    public FailedMessageStore failedMessageStore(JdbcTemplate jdbcTemplate, Clock clock) {
        return new FailedMessageStore(jdbcTemplate, clock);
    }

    @Bean
    @Primary
    @ConditionalOnMissingBean(DurableFailureRecoverer.class)
    public DurableFailureRecoverer durableFailureRecoverer(
            FailedMessageStore store,
            PlatformTransactionManager transactionManager,
            FaultInjector faultInjector,
            Clock clock,
            FailureCaptureProperties properties,
            LoggingConsumerRecordRecoverer delegate) {
        return new DurableFailureRecoverer(
                store, transactionManager, faultInjector, clock, properties.fallbackConsumerGroup(), delegate);
    }

    @Bean
    @ConditionalOnMissingBean(FailedMessageReplayer.class)
    public FailedMessageReplayer failedMessageReplayer(
            FailedMessageStore store,
            KafkaTemplate<String, String> kafkaTemplate,
            PlatformTransactionManager transactionManager,
            FailureCaptureProperties properties) {
        return new FailedMessageReplayer(
                store, kafkaTemplate, transactionManager, Duration.ofMillis(properties.kafkaSendTimeoutMs()));
    }

    // Only where there is an HTTP layer to serve it from. The endpoints are an operator surface,
    // not part of capture itself: a service running without a web server still captures.
    @Bean
    @ConditionalOnWebApplication(type = ConditionalOnWebApplication.Type.SERVLET)
    @ConditionalOnMissingBean(FailedMessageController.class)
    public FailedMessageController failedMessageController(FailedMessageStore store, FailedMessageReplayer replayer) {
        return new FailedMessageController(store, replayer);
    }
}
