package com.eventforge.events.failure;

/**
 * Where a captured failure is in an operator's workflow. Stored as {@link #name()} in
 * {@code failed_messages.status}, so it is readable straight out of SQL.
 */
public enum FailedMessageStatus {

    /** Captured and awaiting a decision. The state everything arrives in. */
    CAPTURED,

    /**
     * A replay was claimed and is in flight. Distinct from {@link #REPLAYED} because the publish
     * happens outside the claiming transaction: a crash between the two leaves the row here, not
     * falsely marked as replayed, and it stays eligible for another attempt.
     */
    REPLAY_REQUESTED,

    /** Published back to its original topic. Terminal under normal operation. */
    REPLAYED,

    /**
     * Deliberately given up on by an operator. Terminal, and distinct from {@link #REPLAYED} so a
     * message nobody intends to recover is never mistaken for one that was.
     */
    ABANDONED
}
