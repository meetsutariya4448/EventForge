package com.eventforge.events.fault;

/** The production default: every injection point is a no-op. */
public final class NoOpFaultInjector implements FaultInjector {

    public static final NoOpFaultInjector INSTANCE = new NoOpFaultInjector();

    private NoOpFaultInjector() {}

    @Override
    public void inject(FaultInjectionPoint point) {
        // no-op
    }
}
