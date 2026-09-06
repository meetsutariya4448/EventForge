package com.eventforge.events.audit;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Reads and writes {@code operator_action}.
 *
 * <p>{@code JdbcTemplate} and hand-written SQL for the same reason as the outbox, the dedupe ledger
 * and the failure store (ADR-0007): this is infrastructure, and its ordering guarantees are the
 * behaviour being relied on rather than a detail to hide behind an ORM.
 */
public class OperatorActionStore {

    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;

    public OperatorActionStore(JdbcTemplate jdbcTemplate, Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    /**
     * Records the intent to act, before the action runs. The caller must commit this before
     * starting the action — that ordering is the whole value of the table.
     *
     * @return the id of the row written, for the later {@link #resolve} call.
     */
    public UUID recordDispatched(String actor, String actionType, String targetType, String targetId) {
        UUID operatorActionId = UUID.randomUUID();
        jdbcTemplate.update(
                """
                INSERT INTO operator_action (
                    operator_action_id, actor, action_type, target_type, target_id, status, dispatched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                operatorActionId,
                actor,
                actionType,
                targetType,
                targetId,
                OperatorActionStatus.DISPATCHED.name(),
                Timestamp.from(clock.instant()));
        return operatorActionId;
    }

    /**
     * Resolves a dispatched action to its outcome.
     *
     * <p>Guarded on {@code completed_at IS NULL} so a resolution can never overwrite one already
     * recorded — the same refuse-to-double-transition discipline the saga handlers use.
     */
    public void resolve(UUID operatorActionId, OperatorActionStatus status, String detail) {
        jdbcTemplate.update(
                """
                UPDATE operator_action
                SET status = ?, completed_at = ?, detail = ?
                WHERE operator_action_id = ? AND completed_at IS NULL
                """,
                status.name(),
                Timestamp.from(clock.instant()),
                detail,
                operatorActionId);
    }

    public Optional<OperatorAction> find(UUID operatorActionId) {
        return jdbcTemplate
                .query(SELECT_ALL + " WHERE operator_action_id = ?", this::mapRow, operatorActionId)
                .stream()
                .findFirst();
    }

    /** The console's default view. */
    public List<OperatorAction> findRecent(int limit) {
        return jdbcTemplate.query(SELECT_ALL + " ORDER BY dispatched_at DESC LIMIT ?", this::mapRow, limit);
    }

    /** Everything done to one target — the question asked when reconciling an unresolved action. */
    public List<OperatorAction> findByTarget(String targetType, String targetId) {
        return jdbcTemplate.query(
                SELECT_ALL + " WHERE target_type = ? AND target_id = ? ORDER BY dispatched_at DESC",
                this::mapRow,
                targetType,
                targetId);
    }

    private static final String SELECT_ALL =
            """
            SELECT operator_action_id, actor, action_type, target_type, target_id,
                   status, dispatched_at, completed_at, detail
            FROM operator_action
            """;

    private OperatorAction mapRow(ResultSet rs, int rowNum) throws SQLException {
        Timestamp completedAt = rs.getTimestamp("completed_at");
        return new OperatorAction(
                UUID.fromString(rs.getString("operator_action_id")),
                rs.getString("actor"),
                rs.getString("action_type"),
                rs.getString("target_type"),
                rs.getString("target_id"),
                OperatorActionStatus.valueOf(rs.getString("status")),
                rs.getTimestamp("dispatched_at").toInstant(),
                completedAt == null ? null : completedAt.toInstant(),
                rs.getString("detail"));
    }
}
