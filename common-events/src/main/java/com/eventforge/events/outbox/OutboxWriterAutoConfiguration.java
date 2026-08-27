package com.eventforge.events.outbox;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Registers {@link OutboxWriter} automatically for any service that has a {@link JdbcTemplate}.
 * {@code @AutoConfigureAfter} (via {@code @AutoConfiguration(after = ...)}) is required here:
 * {@code @ConditionalOnBean} only sees bean definitions already registered by the time it's
 * evaluated, and Boot doesn't otherwise guarantee this class runs after
 * {@link JdbcTemplateAutoConfiguration} — without it, {@code JdbcTemplate} may not exist yet when
 * the condition runs, silently skipping bean creation.
 */
@AutoConfiguration(after = JdbcTemplateAutoConfiguration.class)
@ConditionalOnBean(JdbcTemplate.class)
public class OutboxWriterAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(OutboxWriter.class)
    public OutboxWriter outboxWriter(JdbcTemplate jdbcTemplate) {
        return new OutboxWriter(jdbcTemplate);
    }
}
