package com.eventforge.payment;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

/**
 * Manual ack only, never auto-commit (constitution item 2) — asserted against the real,
 * Spring-Boot-auto-configured factories, not just trusted from {@code application.yml}. Same
 * discipline as M1's {@code ProducerIdempotenceConfigTest}.
 */
@SpringBootTest
class ConsumerManualAckConfigTest extends AbstractPostgresKafkaIntegrationTest {

    @Autowired
    private ConsumerFactory<?, ?> consumerFactory;

    @Autowired
    private ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory;

    @Test
    void autoCommitIsDisabledOnTheRealConsumerFactory() {
        Object enableAutoCommit =
                consumerFactory.getConfigurationProperties().get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG);
        assertThat(String.valueOf(enableAutoCommit)).isEqualTo("false");
    }

    @Test
    void ackModeIsManualImmediateOnTheRealContainerFactory() {
        assertThat(kafkaListenerContainerFactory.getContainerProperties().getAckMode())
                .isEqualTo(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
    }
}
