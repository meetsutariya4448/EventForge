package com.eventforge.order.saga;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;

/**
 * One row per dispatched saga step — the audit trail that makes orchestration's SQL-inspectability
 * claim (constitution item 1) more than just "the current state": what was dispatched, when, and
 * how it resolved, kept even after the saga moves on.
 */
@Entity
@Table(name = "saga_step")
public class SagaStep {

    @Id
    @Column(name = "saga_step_id")
    private UUID sagaStepId;

    @Column(name = "saga_id", nullable = false)
    private UUID sagaId;

    @Column(name = "step_name", nullable = false)
    private String stepName;

    @Column(name = "status", nullable = false)
    private String status;

    @Column(name = "dispatched_at", nullable = false)
    private Instant dispatchedAt;

    @Column(name = "completed_at")
    private Instant completedAt;

    @Column(name = "detail")
    private String detail;

    protected SagaStep() {}

    public SagaStep(UUID sagaStepId, UUID sagaId, String stepName, String status, Instant dispatchedAt, String detail) {
        this.sagaStepId = sagaStepId;
        this.sagaId = sagaId;
        this.stepName = stepName;
        this.status = status;
        this.dispatchedAt = dispatchedAt;
        this.detail = detail;
    }

    public UUID getSagaStepId() {
        return sagaStepId;
    }

    public UUID getSagaId() {
        return sagaId;
    }

    public String getStepName() {
        return stepName;
    }

    public String getStatus() {
        return status;
    }

    public Instant getDispatchedAt() {
        return dispatchedAt;
    }

    public Instant getCompletedAt() {
        return completedAt;
    }

    public String getDetail() {
        return detail;
    }

    public void complete(String status, Instant completedAt, String detail) {
        this.status = status;
        this.completedAt = completedAt;
        this.detail = detail;
    }
}
