package com.eventforge.order.saga;

import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;

public interface SagaInstanceRepository extends JpaRepository<SagaInstance, UUID> {

    Optional<SagaInstance> findByOrderId(UUID orderId);

    List<SagaInstance> findByStateInAndDeadlineAtLessThanEqual(List<SagaState> states, Instant deadline);
}
