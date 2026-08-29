package com.eventforge.order.saga;

import java.util.List;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaStepRepository extends JpaRepository<SagaStep, UUID> {

    List<SagaStep> findBySagaIdOrderByDispatchedAtAsc(UUID sagaId);

    long countBySagaIdAndStepName(UUID sagaId, String stepName);
}
