package com.eventforge.events.audit;

/** Lifecycle of a recorded operator action, mirroring {@code saga_step}'s own status vocabulary. */
public enum OperatorActionStatus {
    /**
     * Recorded and committed, the action not yet resolved. A row left in this state is not proof
     * the action failed — it is proof nothing recorded whether it succeeded, which is the case an
     * audit trail exists to make visible rather than silently absent.
     */
    DISPATCHED,
    SUCCEEDED,
    FAILED
}
