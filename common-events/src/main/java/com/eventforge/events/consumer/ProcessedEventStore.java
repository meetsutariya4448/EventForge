package com.eventforge.events.consumer;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The dedupe half of the consumer transaction invariant (constitution Part 2): every service that
 * consumes events reuses this to decide, inside its own business transaction, whether an event has
 * already been handled by this consumer group.
 *
 * <p>Deliberately uses {@code INSERT ... ON CONFLICT DO NOTHING} rather than attempting the insert
 * and catching a constraint-violation exception. A caught unique-violation would still have marked
 * the surrounding Postgres transaction as aborted (any statement error does, regardless of whether
 * the exception is caught in Java), forcing either a {@code SAVEPOINT} dance or reliance on
 * Postgres silently downgrading a subsequent {@code COMMIT} to a rollback — both more fragile than
 * a statement that simply reports zero rows affected on a duplicate, with no error path at all.
 *
 * <p>The key is {@code (consumer_group, event_id)}, never {@code event_id} alone — two different
 * consumer groups must each process the same event exactly once, independently (see
 * {@code processed_events}' primary key, ADR-0004, and the dedupe-key ADR this milestone adds).
 */
public class ProcessedEventStore {

    private final JdbcTemplate jdbcTemplate;

    public ProcessedEventStore(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * @return true if this is the first time this consumer group has seen this event (the caller
     *     should proceed with the business mutation); false if it's a duplicate (the caller must
     *     treat this as a clean no-op — see {@link ConsumerOutcome#DUPLICATE}).
     */
    public boolean tryMarkProcessed(String consumerGroup, UUID eventId, String aggregateId) {
        int rowsInserted = jdbcTemplate.update(
                """
                INSERT INTO processed_events (consumer_group, event_id, aggregate_id)
                VALUES (?, ?, ?)
                ON CONFLICT (consumer_group, event_id) DO NOTHING
                """,
                consumerGroup,
                eventId,
                aggregateId);
        return rowsInserted == 1;
    }
}
