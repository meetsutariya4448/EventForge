package com.eventforge.testing.fault;

import com.eventforge.events.fault.FaultInjector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.boot.test.context.TestConfiguration;

/**
 * Import this into a test to override the production no-op {@link FaultInjector} bean with a
 * {@link ConfigurableFaultInjector} that the test can arm.
 */
@TestConfiguration
public class FaultInjectionTestConfiguration {

    @Bean
    @Primary
    public FaultInjector faultInjector() {
        return new ConfigurableFaultInjector();
    }
}
