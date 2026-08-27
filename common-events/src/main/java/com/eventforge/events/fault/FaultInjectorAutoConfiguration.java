package com.eventforge.events.fault;

import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Registers the no-op {@link FaultInjector} for every service automatically. Tests override it by
 * providing their own {@code FaultInjector} bean (see {@code common-testing}'s
 * {@code ConfigurableFaultInjector}), which this configuration then backs off for.
 */
@AutoConfiguration
public class FaultInjectorAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean(FaultInjector.class)
    public FaultInjector faultInjector() {
        return NoOpFaultInjector.INSTANCE;
    }
}
