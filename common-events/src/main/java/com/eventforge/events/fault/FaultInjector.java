package com.eventforge.events.fault;

/**
 * A seam for later milestones to crash a process at a precise point in the outbox/relay/consumer
 * lifecycle. Production code calls {@link #inject(FaultInjectionPoint)} at each real seam
 * unconditionally; the default bean is a no-op, so production behavior is unaffected until a test
 * overrides it.
 */
public interface FaultInjector {
    void inject(FaultInjectionPoint point);
}
