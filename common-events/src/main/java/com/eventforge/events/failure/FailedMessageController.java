package com.eventforge.events.failure;

import java.util.List;
import java.util.UUID;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * The operator-facing view of what failed here, and the action to recover it.
 *
 * <p>Lives in {@code common-events} rather than in any one service because failure data is
 * per-service by construction — each service owns its own database and no service may read
 * another's. Shipping the endpoints here means all four expose the same contract, and the console
 * talks to each service for its own failures instead of some aggregator reaching across a
 * database boundary this project does not allow crossing.
 *
 * <p>Every {@code @PathVariable} and {@code @RequestParam} below names its binding explicitly.
 * That is not style: this module is a plain library, so it does not get the {@code -parameters}
 * compiler flag that Spring Boot's Gradle plugin adds to the four service modules, and without a
 * name Spring cannot resolve the argument at all. Found by running — every endpoint here threw
 * {@code IllegalArgumentException: Name for argument of type [java.util.UUID] not specified} until
 * the names were added. Naming them keeps the controller correct wherever it is compiled, rather
 * than depending on a build setting in a different module.
 */
@RestController
@RequestMapping("/failed-messages")
public class FailedMessageController {

    private final FailedMessageStore store;
    private final FailedMessageReplayer replayer;

    public FailedMessageController(FailedMessageStore store, FailedMessageReplayer replayer) {
        this.store = store;
        this.replayer = replayer;
    }

    /** Readable by any authenticated user: seeing what is broken is not a privileged action. */
    @GetMapping
    public List<FailedMessageRow> list(
            @RequestParam(name = "openOnly", defaultValue = "true") boolean openOnly,
            @RequestParam(name = "limit", defaultValue = "100") int limit) {
        return openOnly ? store.findOpen(limit) : store.findAll(limit);
    }

    @GetMapping("/{id}")
    public ResponseEntity<FailedMessageRow> get(@PathVariable("id") UUID id) {
        return store.find(id).map(ResponseEntity::ok).orElseGet(() -> ResponseEntity.notFound().build());
    }

    /**
     * Republishing a message changes what the system does, so it is {@code OPERATOR}-only. A
     * {@code VIEWER} can see the failure and can not act on it — enforced here, not by the console
     * choosing which buttons to draw.
     */
    @PreAuthorize("hasRole('OPERATOR')")
    @PostMapping("/{id}/replay")
    public ResponseEntity<String> replay(@PathVariable("id") UUID id) {
        return switch (replayer.replay(id)) {
            case REPLAYED -> ResponseEntity.accepted().body("Replayed to the original topic");
            // Already replayed, abandoned, unknown, or being claimed by someone else right now.
            // A conflict rather than a 404: the caller's request was understood, it just no longer
            // applies to this row's state.
            case NOT_CLAIMABLE -> ResponseEntity.status(HttpStatus.CONFLICT)
                    .body("Not replayable: already replayed, abandoned, unknown, or claimed concurrently");
            case PUBLISH_FAILED -> ResponseEntity.status(HttpStatus.BAD_GATEWAY)
                    .body("The broker did not accept the replay; the message remains eligible for another attempt");
        };
    }
}
