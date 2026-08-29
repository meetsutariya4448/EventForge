package com.eventforge.events.consumer;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.jdbc.autoconfigure.JdbcTemplateAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * Registers {@link ProcessedEventStore} automatically for any service that has a
 * {@link JdbcTemplate} — same pattern as {@code OutboxWriterAutoConfiguration}, including the
 * same {@code @AutoConfigureAfter} requirement: {@code @ConditionalOnBean} only sees bean
 * definitions already registered when it's evaluated, and Boot doesn't otherwise guarantee this
 * class runs after {@link JdbcTemplateAutoConfiguration}.
 */
@AutoConfiguration(after = JdbcTemplateAutoConfiguration.class)
@ConditionalOnBean(JdbcTemplate.class)
public class ProcessedEventStoreAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(ProcessedEventStore.class)
    public ProcessedEventStore processedEventStore(JdbcTemplate jdbcTemplate) {
        return new ProcessedEventStore(jdbcTemplate);
    }
}
