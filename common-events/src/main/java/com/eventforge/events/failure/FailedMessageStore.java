package com.eventforge.events.failure;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Reads and writes {@code failed_messages}.
 *
 * <p>{@code JdbcTemplate} and hand-written SQL for the same reason the outbox and the dedupe
 * ledger use them (ADR-0007): this is infrastructure whose database semantics — conflict
 * handling, row locking — are the behaviour being relied on, not an implementation detail worth
 * hiding behind an ORM.
 */
public class FailedMessageStore {

    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;

    public FailedMessageStore(JdbcTemplate jdbcTemplate, Clock clock) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
    }

    /**
     * Records a record that exhausted its retries.
     *
     * <p>Idempotent on the record's physical coordinates, not on {@code eventId}: an unparseable
     * record has no event id, and the same record redelivered after a capture that failed must
     * collapse onto the existing row rather than accumulating a second one. Same
     * {@code ON CONFLICT DO NOTHING} idiom as {@code ProcessedEventStore}, for the same reason —
     * a conflict here is an expected outcome, not an error path.
     *
     * @return true if this call created the row; false if it was already captured.
     */
    public boolean capture(FailedMessageRow row) {
        int inserted = jdbcTemplate.update(
                """
                INSERT INTO failed_messages (
                    failed_message_id, consumer_group, topic, partition_id, record_offset,
                    message_key, payload, headers, event_id, event_type,
                    failure_reason, status, captured_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?::jsonb, ?, ?, ?, ?, ?)
                ON CONFLICT (consumer_group, topic, partition_id, record_offset) DO NOTHING
                """,
                row.failedMessageId(),
                row.consumerGroup(),
                row.topic(),
                row.partition(),
                row.offset(),
                row.messageKey(),
                row.payload(),
                row.headersJson(),
                row.eventId(),
                row.eventType(),
                row.failureReason(),
                row.status().name(),
                Timestamp.from(row.capturedAt()));
        return inserted == 1;
    }

    public Optional<FailedMessageRow> find(UUID failedMessageId) {
        return jdbcTemplate
                .query(SELECT_ALL + " WHERE failed_message_id = ?", this::mapRow, failedMessageId)
                .stream()
                .findFirst();
    }

    /** Everything still outstanding, newest first — the operator's working list. */
    public List<FailedMessageRow> findOpen(int limit) {
        return jdbcTemplate.query(
                SELECT_ALL + " WHERE status <> 'REPLAYED' ORDER BY captured_at DESC LIMIT ?", this::mapRow, limit);
    }

    public List<FailedMessageRow> findAll(int limit) {
        return jdbcTemplate.query(SELECT_ALL + " ORDER BY captured_at DESC LIMIT ?", this::mapRow, limit);
    }

    /**
     * Claims a captured failure for replay, if it is still claimable.
     *
     * <p>{@code FOR UPDATE SKIP LOCKED} — the same claim idiom {@code OutboxRelayWorker} uses —
     * so two operators clicking replay at the same moment cannot both publish it: the second
     * finds the row locked and skips rather than waiting to duplicate the work.
     *
     * @return the claimed row, or empty if it is missing, already replayed, abandoned, or being
     *     claimed concurrently.
     */
    public Optional<FailedMessageRow> claimForReplay(UUID failedMessageId) {
        List<FailedMessageRow> claimed = jdbcTemplate.query(
                SELECT_ALL
                        + """
                         WHERE failed_message_id = ?
                           AND status IN ('CAPTURED', 'REPLAY_REQUESTED')
                        FOR UPDATE SKIP LOCKED
                        """,
                this::mapRow,
                failedMessageId);
        if (claimed.isEmpty()) {
            return Optional.empty();
        }
        jdbcTemplate.update(
                """
                UPDATE failed_messages
                SET status = 'REPLAY_REQUESTED', replay_attempts = replay_attempts + 1, last_replay_at = ?
                WHERE failed_message_id = ?
                """,
                Timestamp.from(clock.instant()),
                failedMessageId);
        return Optional.of(claimed.get(0));
    }

    /** Marks a claim as published. Called only after the broker has acknowledged the send. */
    public void markReplayed(UUID failedMessageId) {
        jdbcTemplate.update(
                "UPDATE failed_messages SET status = 'REPLAYED', last_replay_error = NULL WHERE failed_message_id = ?",
                failedMessageId);
    }

    /**
     * Records why a replay attempt failed, leaving the row in {@code REPLAY_REQUESTED} so it stays
     * eligible for another attempt rather than being falsely closed.
     */
    public void markReplayFailed(UUID failedMessageId, String error) {
        jdbcTemplate.update(
                "UPDATE failed_messages SET last_replay_error = ? WHERE failed_message_id = ?", error, failedMessageId);
    }

    private static final String SELECT_ALL =
            """
            SELECT failed_message_id, consumer_group, topic, partition_id, record_offset,
                   message_key, payload, headers::text AS headers_text, event_id, event_type,
                   failure_reason, status, captured_at, replay_attempts, last_replay_at, last_replay_error
            FROM failed_messages
            """;

    private FailedMessageRow mapRow(ResultSet rs, int rowNum) throws SQLException {
        String eventId = rs.getString("event_id");
        Timestamp lastReplayAt = rs.getTimestamp("last_replay_at");
        return new FailedMessageRow(
                UUID.fromString(rs.getString("failed_message_id")),
                rs.getString("consumer_group"),
                rs.getString("topic"),
                rs.getInt("partition_id"),
                rs.getLong("record_offset"),
                rs.getString("message_key"),
                rs.getString("payload"),
                rs.getString("headers_text"),
                eventId == null ? null : UUID.fromString(eventId),
                rs.getString("event_type"),
                rs.getString("failure_reason"),
                FailedMessageStatus.valueOf(rs.getString("status")),
                rs.getTimestamp("captured_at").toInstant(),
                rs.getInt("replay_attempts"),
                lastReplayAt == null ? null : lastReplayAt.toInstant(),
                rs.getString("last_replay_error"));
    }
}
