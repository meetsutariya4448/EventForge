package com.eventforge.order;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.eventforge.events.audit.AuditedFailedMessageReplayer;
import com.eventforge.events.audit.OperatorAction;
import com.eventforge.events.audit.OperatorActionStatus;
import com.eventforge.events.audit.OperatorActionStore;
import com.eventforge.events.failure.FailedMessageRow;
import com.eventforge.events.failure.FailedMessageStatus;
import com.eventforge.events.failure.FailedMessageStore;
import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import com.eventforge.order.saga.SagaOrchestrator;
import com.eventforge.testing.containers.AbstractPostgresKafkaIntegrationTest;
import com.eventforge.testing.fault.ConfigurableFaultInjector;
import com.eventforge.testing.fault.FaultInjectionTestConfiguration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * The audit trail's actual claim: an operator action is recorded before it runs, so an action that
 * crashes halfway is visibly unresolved rather than invisibly absent.
 *
 * <p>Recording only outcomes would pass a happy-path test and lose exactly the case worth
 * auditing. So the crash case is tested first-class here, with real fault injection at
 * {@link FaultInjectionPoint#AFTER_AUDIT_DISPATCH_BEFORE_ACTION}, not reasoned about in a comment.
 */
@SpringBootTest
@Import(FaultInjectionTestConfiguration.class)
class OperatorActionAuditIntegrationTest extends AbstractPostgresKafkaIntegrationTest {

    @DynamicPropertySource
    static void tuning(DynamicPropertyRegistry registry) {
        registry.add("eventforge.outbox.relay.scheduler-enabled", () -> "false");
        registry.add("eventforge.idempotency.reaper-enabled", () -> "false");
    }

    @Autowired
    private FailedMessageStore failedMessageStore;

    @Autowired
    private OperatorActionStore auditStore;

    @Autowired
    private AuditedFailedMessageReplayer replayer;

    @Autowired
    private FaultInjector faultInjector;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private UUID failedMessageId;

    @BeforeEach
    void captureOneFailure() {
        jdbcTemplate.update("TRUNCATE TABLE operator_action");
        jdbcTemplate.update("TRUNCATE TABLE failed_messages");
        failedMessageId = capture();
    }

    @AfterEach
    void clearArmedFaults() {
        ((ConfigurableFaultInjector) faultInjector).clear();
    }

    @Test
    void asuccessfulReplayIsRecordedWithWhoDidItAndWhatHappened() {
        replayer.replay(failedMessageId, "operator");

        OperatorAction action = onlyAction();
        assertThat(action.actor()).isEqualTo("operator");
        assertThat(action.actionType()).isEqualTo(OperatorAction.ACTION_REPLAY_FAILED_MESSAGE);
        assertThat(action.targetType()).isEqualTo(OperatorAction.TARGET_FAILED_MESSAGE);
        assertThat(action.targetId()).isEqualTo(failedMessageId.toString());
        assertThat(action.status()).isEqualTo(OperatorActionStatus.SUCCEEDED);
        assertThat(action.completedAt()).isNotNull();
        assertThat(action.detail()).isEqualTo("REPLAYED");
    }

    /**
     * The case the two-phase shape exists for. Recording only the outcome would leave no row at
     * all here — the action would have happened, or half-happened, with nothing to show for it.
     */
    @Test
    void aCrashMidActionLeavesAVisiblyUnresolvedRowRatherThanNoRow() {
        ((ConfigurableFaultInjector) faultInjector)
                .registerAction(FaultInjectionPoint.AFTER_AUDIT_DISPATCH_BEFORE_ACTION, () -> {
                    throw new IllegalStateException("simulated crash after the audit row committed");
                });

        assertThatThrownBy(() -> replayer.replay(failedMessageId, "operator"))
                .isInstanceOf(IllegalStateException.class);

        OperatorAction action = onlyAction();
        assertThat(action.status())
                .as("the intent was committed before the action ran, so it survived the crash")
                .isEqualTo(OperatorActionStatus.DISPATCHED);
        assertThat(action.completedAt())
                .as("unresolved, which is what makes it findable — not silently absent")
                .isNull();
        assertThat(action.actor()).isEqualTo("operator");

        // And the honest reading of that row: it does not say the replay did not happen. Here it
        // genuinely did not, and the way to know is the target's own state, exactly as the runbook
        // says to reconcile it.
        assertThat(failedMessageStore.find(failedMessageId).orElseThrow().status())
                .isEqualTo(FailedMessageStatus.CAPTURED);
    }

    /** A declined replay is a completed action, not a failed one — it needs no human attention. */
    @Test
    void aDeclinedReplayIsRecordedAsSucceededWithTheReasonInDetail() {
        replayer.replay(failedMessageId, "operator");
        replayer.replay(failedMessageId, "operator");

        List<OperatorAction> actions =
                auditStore.findByTarget(OperatorAction.TARGET_FAILED_MESSAGE, failedMessageId.toString());
        assertThat(actions).hasSize(2);

        // findByTarget is newest-first, so the second attempt is first.
        assertThat(actions.get(0).status()).isEqualTo(OperatorActionStatus.SUCCEEDED);
        assertThat(actions.get(0).detail())
                .as("carried out and correctly declined; filing this as FAILED would fill the "
                        + "unresolved view with actions needing no attention")
                .isEqualTo("NOT_CLAIMABLE");
        assertThat(actions.get(1).detail()).isEqualTo("REPLAYED");
    }

    @Test
    void resolvingTwiceCannotOverwriteAnOutcomeAlreadyRecorded() {
        replayer.replay(failedMessageId, "operator");
        OperatorAction action = onlyAction();
        Instant firstCompletedAt = action.completedAt();

        auditStore.resolve(action.operatorActionId(), OperatorActionStatus.FAILED, "a later, wrong resolution");

        OperatorAction after = auditStore.find(action.operatorActionId()).orElseThrow();
        assertThat(after.status()).isEqualTo(OperatorActionStatus.SUCCEEDED);
        assertThat(after.detail()).isEqualTo("REPLAYED");
        assertThat(after.completedAt()).isEqualTo(firstCompletedAt);
    }

    private OperatorAction onlyAction() {
        List<OperatorAction> actions = auditStore.findRecent(10);
        assertThat(actions).hasSize(1);
        return actions.get(0);
    }

    private UUID capture() {
        UUID id = UUID.randomUUID();
        failedMessageStore.capture(new FailedMessageRow(
                id,
                SagaOrchestrator.CONSUMER_GROUP,
                "payments.events",
                0,
                ThreadLocalRandom.current().nextLong(1_000_000L, Long.MAX_VALUE),
                UUID.randomUUID().toString(),
                "{not-valid-json-at-all",
                null,
                null,
                null,
                "com.fasterxml.jackson.core.JsonParseException: unexpected character",
                FailedMessageStatus.CAPTURED,
                Instant.now(),
                0,
                null,
                null));
        return id;
    }
}
