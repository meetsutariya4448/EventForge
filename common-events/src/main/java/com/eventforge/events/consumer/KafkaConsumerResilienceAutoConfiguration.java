package com.eventforge.events.consumer;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ConsumerRecordRecoverer;
import org.springframework.kafka.listener.ContainerPausingBackOffHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ListenerContainerPauseService;
import org.springframework.kafka.listener.ListenerContainerRegistry;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Downstream failure handling without a circuit breaker library (constitution item 5): bounded
 * retry with backoff, then — for a genuinely poisoned record — a visible, logged skip that lets
 * the partition keep moving ({@link LoggingConsumerRecordRecoverer}); for sustained failure, the
 * container pauses for the backoff duration and resumes automatically
 * ({@link ContainerPausingBackOffHandler}), rather than hammering a struggling downstream in a
 * tight redelivery loop. Both are Spring Kafka's own native mechanisms — no Resilience4j.
 *
 * <p>Applies to every service with Spring Kafka's listener infrastructure on the classpath; each
 * service's own {@code @KafkaListener} beans pick this error handler up automatically via Spring
 * Boot's auto-configured container factory (Boot wires any user-supplied {@code CommonErrorHandler}
 * bean in without further configuration).
 */
@AutoConfiguration
@EnableConfigurationProperties(ConsumerResilienceProperties.class)
@ConditionalOnClass(DefaultErrorHandler.class)
public class KafkaConsumerResilienceAutoConfiguration {

    // Conditional on its own concrete type, not on ConsumerRecordRecoverer: as of v2 WS3 this is
    // no longer necessarily the recoverer the error handler uses. FailureCaptureAutoConfiguration
    // registers a @Primary DurableFailureRecoverer that composes this one, so both beans exist and
    // this stays the thing that writes the ERROR line and counts. Conditioning on the interface
    // would have made this bean disappear the moment the durable recoverer arrived, silently
    // removing the log line and breaking the counter PoisonMessageIntegrationTest asserts on.
    @Bean
    @ConditionalOnMissingBean(LoggingConsumerRecordRecoverer.class)
    public LoggingConsumerRecordRecoverer consumerRecordRecoverer() {
        return new LoggingConsumerRecordRecoverer();
    }

    // ContainerPausingBackOffHandler needs a TaskScheduler to schedule the resume; not every
    // service that consumes Kafka also has @EnableScheduling active (only ones with the outbox
    // relay enabled do — see OutboxRelayAutoConfiguration), so this can't be assumed to already
    // exist. Provided here as a fallback rather than left as an implicit dependency on an
    // unrelated feature being turned on.
    @Bean
    @ConditionalOnMissingBean(TaskScheduler.class)
    public TaskScheduler consumerResilienceTaskScheduler() {
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setPoolSize(1);
        scheduler.setThreadNamePrefix("kafka-pause-resume-");
        scheduler.initialize();
        return scheduler;
    }

    @Bean
    @ConditionalOnMissingBean(ListenerContainerPauseService.class)
    public ListenerContainerPauseService listenerContainerPauseService(
            ListenerContainerRegistry registry, TaskScheduler taskScheduler) {
        return new ListenerContainerPauseService(registry, taskScheduler);
    }

    @Bean
    @ConditionalOnMissingBean(CommonErrorHandler.class)
    public DefaultErrorHandler kafkaConsumerErrorHandler(
            ConsumerRecordRecoverer recoverer,
            ListenerContainerPauseService pauseService,
            ConsumerResilienceProperties properties) {
        return new DefaultErrorHandler(
                recoverer,
                new FixedBackOff(properties.backoffMs(), properties.maxRetries()),
                new ContainerPausingBackOffHandler(pauseService));
    }
}
