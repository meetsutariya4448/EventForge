package com.eventforge.order.idempotency;

import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

/** Wires the {@code Idempotency-Key} machinery for {@code POST /orders}. */
@Configuration
@EnableConfigurationProperties(IdempotencyProperties.class)
public class IdempotencyConfiguration {

    /**
     * Takes the same {@link Clock} bean every other time-sensitive component here takes, so a
     * test that moves time forward moves it for the reaper too.
     */
    @Bean
    public IdempotencyKeyStore idempotencyKeyStore(
            JdbcTemplate jdbcTemplate, Clock clock, IdempotencyProperties properties) {
        return new IdempotencyKeyStore(jdbcTemplate, clock, properties.ttl());
    }

    /**
     * Conditional on the property rather than always registered: with it false the bean does not
     * exist, so no {@code @Scheduled} method is in the context to race a test's own explicit
     * call. See {@link IdempotencyKeyReaper}'s javadoc for the M4 incident that motivated this.
     */
    @Bean
    @ConditionalOnProperty(
            prefix = "eventforge.idempotency",
            name = "reaper-enabled",
            havingValue = "true",
            matchIfMissing = true)
    public IdempotencyKeyReaper idempotencyKeyReaper(IdempotencyKeyStore store) {
        return new IdempotencyKeyReaper(store);
    }
}
