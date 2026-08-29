package com.eventforge.order.saga;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Periodically finds sagas whose persisted {@code deadline_at} has passed and resolves them
 * (constitution item 5). Deliberately stateless — a fresh instance of this class, reading only
 * from {@code saga_instance}, produces the same outcome as the instance that dispatched the
 * command in the first place, which is exactly what "restart-safe" means here and what
 * {@code SagaStrandedByDeadConsumerIntegrationTest} proves directly, the same way M1's crash-window
 * tests proved a relay "restart" by simply calling a fresh {@code OutboxRelayWorker} again.
 */
@Component
public class SagaTimeoutSweeper {

    private final SagaOrchestrator orchestrator;

    public SagaTimeoutSweeper(SagaOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    @Scheduled(fixedDelayString = "${eventforge.saga.sweep-interval-ms:1000}")
    public void sweep() {
        orchestrator.sweepTimedOutSagas();
    }
}
