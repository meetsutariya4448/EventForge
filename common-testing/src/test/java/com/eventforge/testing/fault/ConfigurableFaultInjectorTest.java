package com.eventforge.testing.fault;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.eventforge.events.fault.FaultInjectionPoint;
import org.junit.jupiter.api.Test;

/**
 * Proves the fault-injection seam itself: an unregistered point stays a no-op, and a registered
 * point runs the action a test supplied. No containers here — this is the fast unit test for the
 * harness, distinct from any future test that uses it to crash a real relay/consumer.
 */
class ConfigurableFaultInjectorTest {

    @Test
    void unregisteredPointIsANoOp() {
        ConfigurableFaultInjector injector = new ConfigurableFaultInjector();

        injector.inject(FaultInjectionPoint.AFTER_DB_COMMIT_BEFORE_KAFKA_PUBLISH);
        // no exception, nothing observable — the point was never armed
    }

    @Test
    void registeredPointRunsTheArmedAction() {
        ConfigurableFaultInjector injector = new ConfigurableFaultInjector();
        injector.registerAction(
                FaultInjectionPoint.AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED,
                () -> {
                    throw new IllegalStateException("simulated crash after publish, before mark-published");
                });

        assertThatThrownBy(() -> injector.inject(FaultInjectionPoint.AFTER_KAFKA_PUBLISH_BEFORE_MARK_PUBLISHED))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("simulated crash");
    }

    @Test
    void clearRemovesPreviouslyRegisteredActions() {
        ConfigurableFaultInjector injector = new ConfigurableFaultInjector();
        injector.registerAction(FaultInjectionPoint.AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK, () -> {
            throw new IllegalStateException("should not run after clear()");
        });

        injector.clear();

        injector.inject(FaultInjectionPoint.AFTER_BUSINESS_COMMIT_BEFORE_OFFSET_ACK);
        // no exception: clear() removed the armed action
        assertThat(true).isTrue();
    }
}
