package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;

import com.eventforge.events.outbox.OutboxRelayScheduler;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

/**
 * {@code eventforge.outbox.relay.scheduler-enabled} exists so tests that drive
 * {@code OutboxRelayWorker} directly can turn the background poller off without touching
 * production config — every other test in this project that sets it does so explicitly, in its
 * own {@code @DynamicPropertySource}. This class is the one that asserts the other half: with no
 * override at all (this project's actual production shape, since order-service is the one
 * service with the relay enabled), the scheduler bean exists and would poll on its own schedule.
 * Production behavior is unchanged by the flag's addition — proven directly, not assumed from
 * {@code matchIfMissing = true} reading correctly in {@code OutboxRelayAutoConfiguration}.
 */
@SpringBootTest
class OutboxRelaySchedulerEnabledByDefaultTest extends AbstractPostgresKafkaIntegrationTest {

    @Autowired
    private OutboxRelayScheduler outboxRelayScheduler;

    @Test
    void theBackgroundSchedulerBeanExistsWithNoOverride() {
        assertThat(outboxRelayScheduler).isNotNull();
    }
}
