package com.eventforge.events.audit;

import java.time.Instant;
import java.util.UUID;

/**
 * One thing an operator did, as stored.
 *
 * @param actor the authenticated principal's name, never a value the caller supplied — an audit
 *     trail a caller can sign someone else's name to is worse than none, because it looks
 *     authoritative.
 * @param completedAt null while the action is {@link OperatorActionStatus#DISPATCHED}.
 */
public record OperatorAction(
        UUID operatorActionId,
        String actor,
        String actionType,
        String targetType,
        String targetId,
        OperatorActionStatus status,
        Instant dispatchedAt,
        Instant completedAt,
        String detail) {

    /** The action type recorded when an operator republishes a captured failure. */
    public static final String ACTION_REPLAY_FAILED_MESSAGE = "REPLAY_FAILED_MESSAGE";

    /** The target type for {@link #ACTION_REPLAY_FAILED_MESSAGE}. */
    public static final String TARGET_FAILED_MESSAGE = "FAILED_MESSAGE";
}
