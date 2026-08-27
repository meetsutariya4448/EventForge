package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.ProducerFactory;

/**
 * Trap T2: per-partition ordering under retries requires enable.idempotence=true. Asserts the
 * actual configured producer, not just that the property is present in application.yml.
 */
@SpringBootTest
class ProducerIdempotenceConfigTest extends AbstractPostgresKafkaIntegrationTest {

    @Autowired
    private ProducerFactory<?, ?> producerFactory;

    @Test
    void producerIsConfiguredForIdempotenceAndAcksAll() {
        Map<String, Object> config = producerFactory.getConfigurationProperties();

        assertThat(String.valueOf(config.get("enable.idempotence"))).isEqualTo("true");
        assertThat(String.valueOf(config.get("acks"))).isEqualTo("all");
    }
}
