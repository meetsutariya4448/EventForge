package com.eventforge.testing.fault;

import com.eventforge.events.fault.FaultInjectionPoint;
import com.eventforge.events.fault.FaultInjector;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A test double for {@link FaultInjector}: lets a test register an action to run when production
 * code hits a given {@link FaultInjectionPoint}, e.g. to throw and simulate a crash at that exact
 * seam. Unregistered points remain no-ops, matching production behavior.
 */
public class ConfigurableFaultInjector implements FaultInjector {

    private final Map<FaultInjectionPoint, Runnable> actions = new ConcurrentHashMap<>();

    public void registerAction(FaultInjectionPoint point, Runnable action) {
        actions.put(point, action);
    }

    public void clear() {
        actions.clear();
    }

    @Override
    public void inject(FaultInjectionPoint point) {
        Runnable action = actions.get(point);
        if (action != null) {
            action.run();
        }
    }
}
