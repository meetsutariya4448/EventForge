package com.eventforge.events.audit;

import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The audit trail, readable by any authenticated user.
 *
 * <p>Read access is deliberately not restricted to {@code OPERATOR}. A record of who did what is
 * worth less if only the people who can act on the system can see it — a {@code VIEWER} watching
 * for unexpected activity is a legitimate reader, arguably the most important one.
 *
 * <p>No write endpoint exists, and that is the point: rows are written only by the code path that
 * performs the action being recorded. An audit trail with an HTTP writer is a log, not a trail.
 *
 * <p>Binding names are explicit for the same reason as {@code FailedMessageController}'s: this
 * module does not get the {@code -parameters} compiler flag the service modules receive from
 * Spring Boot's Gradle plugin.
 */
@RestController
@RequestMapping("/operator-actions")
public class OperatorActionController {

    private final OperatorActionStore store;

    public OperatorActionController(OperatorActionStore store) {
        this.store = store;
    }

    @GetMapping
    public List<OperatorAction> list(
            @RequestParam(name = "targetType", required = false) String targetType,
            @RequestParam(name = "targetId", required = false) String targetId,
            @RequestParam(name = "limit", defaultValue = "100") int limit) {
        if (targetType != null && targetId != null) {
            return store.findByTarget(targetType, targetId);
        }
        return store.findRecent(limit);
    }
}
