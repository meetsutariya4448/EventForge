package com.eventforge.events.audit;

import com.eventforge.events.failure.FailedMessageReplayer;
import com.eventforge.events.failure.FailedMessageReplayer.ReplayOutcome;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import java.util.UUID;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Replay, with a durable record of who asked for it and what happened.
 *
 * <h2>Two phases, in this order, for the same reason the saga uses them</h2>
 *
 * The audit row is written and <b>committed</b> before {@link FailedMessageReplayer#replay} is
 * called, then resolved afterwards. Recording only the outcome would be simpler and would lose
 * exactly the case worth auditing: an action that started and then crashed. With this ordering
 * that action leaves a {@code DISPATCHED} row with no {@code completed_at} — visibly unresolved,
 * the same way a dispatched saga step whose participant never answered is visibly unresolved.
 *
 * <p>Each phase is its own {@code REQUIRES_NEW} transaction. They cannot share one with the
 * replay: the replay already runs two transactions of its own around a Kafka publish, and an
 * enclosing transaction would either hold a connection open across that network call or roll the
 * audit row back on failure — destroying the record of the very attempt that failed.
 *
 * <p>The consequence is stated rather than hidden, in the migration and in the runbook: a
 * {@code DISPATCHED} row does not mean the replay did not happen. It means nothing recorded
 * whether it did, and reconciling it means reading the {@code failed_messages} row it targeted.
 */
public class AuditedFailedMessageReplayer {

    private final FailedMessageReplayer replayer;
    private final OperatorActionStore auditStore;
    private final FaultInjector faultInjector;
    private final TransactionTemplate transactionTemplate;

    public AuditedFailedMessageReplayer(
            FailedMessageReplayer replayer,
            OperatorActionStore auditStore,
            FaultInjector faultInjector,
            PlatformTransactionManager transactionManager) {
        this.replayer = replayer;
        this.auditStore = auditStore;
        this.faultInjector = faultInjector;
        this.transactionTemplate = new TransactionTemplate(transactionManager);
        this.transactionTemplate.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
    }

    /**
     * @param actor the authenticated principal's name. Supplied by the controller from the
     *     security context, never from anything the caller sent.
     */
    public ReplayOutcome replay(UUID failedMessageId, String actor) {
        UUID auditId = transactionTemplate.execute(status -> auditStore.recordDispatched(
                actor,
                OperatorAction.ACTION_REPLAY_FAILED_MESSAGE,
                OperatorAction.TARGET_FAILED_MESSAGE,
                failedMessageId.toString()));

        faultInjector.inject(FaultInjectionPoint.AFTER_AUDIT_DISPATCH_BEFORE_ACTION);

        ReplayOutcome outcome;
        try {
            outcome = replayer.replay(failedMessageId);
        } catch (RuntimeException e) {
            // An unexpected failure is still an outcome worth recording. Without this the row
            // would stay DISPATCHED and be indistinguishable from a crash, when in fact we know
            // exactly what went wrong.
            resolve(auditId, OperatorActionStatus.FAILED, e.getClass().getName() + ": " + e.getMessage());
            throw e;
        }

        resolve(auditId, statusFor(outcome), outcome.name());
        return outcome;
    }

    /**
     * NOT_CLAIMABLE is SUCCEEDED, not FAILED: the action was carried out and correctly declined —
     * the row was already replayed, abandoned, or being claimed by someone else. Recording that as
     * a failure would fill an operator's attention list with actions that need none.
     *
     * <p>PUBLISH_FAILED is FAILED, because it genuinely is: the broker did not accept the message
     * and the failed_messages row remains eligible for another attempt. Collapsing it into the
     * same bucket as a clean decline would hide the one outcome that wants a human.
     */
    private static OperatorActionStatus statusFor(ReplayOutcome outcome) {
        return switch (outcome) {
            case REPLAYED, NOT_CLAIMABLE -> OperatorActionStatus.SUCCEEDED;
            case PUBLISH_FAILED -> OperatorActionStatus.FAILED;
        };
    }

    private void resolve(UUID auditId, OperatorActionStatus status, String detail) {
        if (auditId == null) {
            return;
        }
        transactionTemplate.executeWithoutResult(tx -> auditStore.resolve(auditId, status, detail));
    }
}
