-- Operator actions (v2 WS2). Deliberately the same two-phase shape as saga_step: a row is written
-- and committed when the action is DISPATCHED, and resolved to SUCCEEDED/FAILED afterwards.
--
-- The ordering is the reason this table is worth having. Recording only the outcome would mean an
-- action that crashed halfway leaves no trace at all — the one case an audit trail exists for. By
-- committing the intent first, a process that dies mid-replay leaves a DISPATCHED row with no
-- completed_at, which reads exactly like a saga step whose participant never answered: visibly
-- unresolved rather than invisibly absent.
--
-- Consequence, stated rather than hidden: a DISPATCHED row does NOT mean the action did not
-- happen. It means nothing recorded whether it did. Reconciling one means looking at what the
-- action targeted — for a replay, the failed_messages row's own status.
CREATE TABLE operator_action (
    operator_action_id UUID PRIMARY KEY,
    -- Who asked. Taken from the authenticated principal, never from a request body — a caller must
    -- not be able to name themselves.
    actor              VARCHAR(128) NOT NULL,
    action_type        VARCHAR(64)  NOT NULL,
    -- What it acted on, kept generic: replay is the only action today, and a table shaped around
    -- that one verb would have to be migrated for the second.
    target_type        VARCHAR(64)  NOT NULL,
    target_id          VARCHAR(128) NOT NULL,
    status             VARCHAR(32)  NOT NULL,
    dispatched_at      TIMESTAMPTZ  NOT NULL,
    completed_at       TIMESTAMPTZ,
    detail             TEXT
);

-- The console's default view: most recent first.
CREATE INDEX idx_operator_action_dispatched ON operator_action (dispatched_at DESC);

-- "What has been done to this thing" — the question asked when reconciling an unresolved action.
CREATE INDEX idx_operator_action_target ON operator_action (target_type, target_id);

-- Unresolved actions only: the set that needs human attention, sized to that rather than to
-- everything ever done (same shape as outbox_events' unpublished-only index).
CREATE INDEX idx_operator_action_unresolved ON operator_action (dispatched_at) WHERE completed_at IS NULL;
