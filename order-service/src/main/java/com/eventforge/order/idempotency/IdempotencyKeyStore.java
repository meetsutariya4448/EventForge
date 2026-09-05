package com.eventforge.order.idempotency;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * The claim ledger behind {@code Idempotency-Key} on {@code POST /orders}.
 *
 * <p>{@code JdbcTemplate} with hand-written SQL rather than JPA, for the same reason
 * {@code outbox_events} and {@code processed_events} are (ADR-0007): this is an infrastructure
 * table whose exact database semantics — {@code ON CONFLICT DO NOTHING}, statement-level
 * visibility, lock timeouts — are the point, and an ORM would obscure them.
 *
 * <p>The claim reuses the {@code INSERT ... ON CONFLICT DO NOTHING} idiom
 * {@code ProcessedEventStore} already proves correct under real concurrent contention, for the
 * same reason it does: a caught unique-violation would still mark the surrounding transaction
 * aborted, whereas a statement that reports zero rows affected has no error path at all.
 */
public class IdempotencyKeyStore {

    /**
     * Postgres SQLSTATE for {@code lock_not_available}, which is what {@code lock_timeout} raises.
     */
    private static final String LOCK_NOT_AVAILABLE = "55P03";

    private final JdbcTemplate jdbcTemplate;
    private final Clock clock;
    private final Duration ttl;

    public IdempotencyKeyStore(JdbcTemplate jdbcTemplate, Clock clock, Duration ttl) {
        this.jdbcTemplate = jdbcTemplate;
        this.clock = clock;
        this.ttl = ttl;
    }

    /**
     * Bounds how long this transaction will block behind another transaction's in-flight claim
     * on the same key. Without it a caller blocks for the winner's entire duration; with it, a
     * pathological wait surfaces as a retryable 409 instead of an open-ended hang. Must be
     * called inside the transaction it applies to — {@code SET LOCAL} reverts on commit.
     *
     * <p>See ADR-0021 for why blocking at all is a deliberate, and costed, trade-off.
     */
    public void applyLockTimeout(Duration lockTimeout) {
        jdbcTemplate.execute("SET LOCAL lock_timeout = '" + lockTimeout.toMillis() + "ms'");
    }

    /**
     * Attempts to claim the key for this request.
     *
     * <p>Must be the first statement of the same transaction as the order write. When two
     * requests race, Postgres blocks the second on the winner's speculative insertion lock until
     * the winner commits or aborts:
     *
     * <ul>
     *   <li>winner commits → this returns false, and {@link #find} (a later statement, so a
     *       fresh READ COMMITTED snapshot) is guaranteed to see the committed row;
     *   <li>winner aborts → this insert succeeds and the caller legitimately becomes the winner.
     * </ul>
     *
     * So a false return strictly implies a committed row exists. There is no third case, which
     * is why this table needs no {@code IN_PROGRESS} state and no stuck-claim reaper.
     *
     * @return true if this request claimed the key and should perform the write.
     */
    public boolean tryClaim(
            String idempotencyKey, String requestFingerprint, UUID orderId, int responseStatus, String responseBody) {
        Timestamp now = Timestamp.from(clock.instant());
        Timestamp expiresAt = Timestamp.from(clock.instant().plus(ttl));
        int rowsInserted;
        try {
            rowsInserted = jdbcTemplate.update(
                    """
                    INSERT INTO idempotency_keys (
                        idempotency_key, request_fingerprint, order_id,
                        response_status, response_body, created_at, expires_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT (idempotency_key) DO NOTHING
                    """,
                    idempotencyKey,
                    requestFingerprint,
                    orderId,
                    responseStatus,
                    responseBody,
                    now,
                    expiresAt);
        } catch (DataAccessException e) {
            if (isLockTimeout(e)) {
                throw new IdempotencyClaimInFlightException(idempotencyKey, e);
            }
            throw e;
        }
        return rowsInserted == 1;
    }

    /**
     * Whether this failure is {@code lock_timeout} firing while blocked on another transaction's
     * uncommitted claim.
     *
     * <p>Matched on SQLSTATE rather than on an exception type because Spring's translator leaves
     * {@code 55P03} uncategorized — verified by
     * {@code IdempotencyLockTimeoutIntegrationTest}, which exists to fail loudly if that ever
     * changes. Reading the SQLSTATE off the innermost cause is what makes this precise instead of
     * catching every uncategorized SQL error and calling it a conflict.
     */
    private static boolean isLockTimeout(DataAccessException e) {
        return e.getMostSpecificCause() instanceof SQLException sqlException
                && LOCK_NOT_AVAILABLE.equals(sqlException.getSQLState());
    }

    /** The committed record for a key, if one exists. */
    public Optional<IdempotencyRecord> find(String idempotencyKey) {
        List<IdempotencyRecord> rows = jdbcTemplate.query(
                """
                SELECT idempotency_key, request_fingerprint, order_id, response_status, response_body
                FROM idempotency_keys
                WHERE idempotency_key = ?
                """,
                (rs, rowNum) -> new IdempotencyRecord(
                        rs.getString("idempotency_key"),
                        rs.getString("request_fingerprint"),
                        UUID.fromString(rs.getString("order_id")),
                        rs.getInt("response_status"),
                        rs.getString("response_body")),
                idempotencyKey);
        return rows.stream().findFirst();
    }

    /**
     * Deletes rows past their TTL. Evaluated against the injected {@link Clock}, not the
     * database's {@code now()}, so a test can move time forward deterministically instead of
     * waiting — the same discipline the outbox relay's retry backoff and the saga's deadline
     * sweep already follow.
     *
     * @return how many rows were removed.
     */
    public int deleteExpired() {
        return jdbcTemplate.update(
                "DELETE FROM idempotency_keys WHERE expires_at <= ?", Timestamp.from(clock.instant()));
    }
}
